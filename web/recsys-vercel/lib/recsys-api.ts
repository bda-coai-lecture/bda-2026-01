export type ApiErrorPayload = {
  ok: false;
  status: number;
  message: string;
  upstreamUrl: string;
  details?: unknown;
};

export type ApiSuccessPayload<T> = {
  ok: true;
  data: T;
  upstreamUrl: string;
};

type CacheOptions = {
  cacheKey?: string;
  cacheControl?: string;
  freshTtlMs?: number;
  staleTtlMs?: number;
};

type CacheEntry = {
  cachedAt: number;
  payload: unknown;
};

const DEFAULT_FRESH_TTL_MS = 5 * 60 * 1000;
const DEFAULT_STALE_TTL_MS = 24 * 60 * 60 * 1000;

const responseCache = globalThis as typeof globalThis & {
  __recsysProxyCache?: Map<string, CacheEntry>;
};

function cacheStore() {
  responseCache.__recsysProxyCache ??= new Map<string, CacheEntry>();
  return responseCache.__recsysProxyCache;
}

export function apiBaseUrl(request?: Request) {
  const baseUrl = (process.env.NEXT_PUBLIC_RECSYS_API_BASE_URL || "http://localhost:8001").replace(/\/$/, "");
  if (request && isLocalhostUrl(baseUrl) && !isLocalhostRequest(request)) {
    throw new Error(
      "NEXT_PUBLIC_RECSYS_API_BASE_URL is still localhost. Set it to a tunnel or public local API URL before deploying.",
    );
  }
  return baseUrl;
}

function isLocalhostUrl(value: string) {
  try {
    const url = new URL(value);
    return url.hostname === "localhost" || url.hostname === "127.0.0.1";
  } catch {
    return false;
  }
}

function isLocalhostRequest(request: Request) {
  const host = request.headers.get("host") || "";
  return host.startsWith("localhost:") || host.startsWith("127.0.0.1:");
}

export function configuredApiBaseUrl() {
  return (process.env.NEXT_PUBLIC_RECSYS_API_BASE_URL || "http://localhost:8001").replace(/\/$/, "");
}

export function jsonHeaders() {
  return {
    "content-type": "application/json",
  };
}

export async function proxyJson<T>(url: string, init?: RequestInit, options: CacheOptions = {}): Promise<Response> {
  const cacheKey = options.cacheKey || url;
  const freshTtlMs = options.freshTtlMs ?? DEFAULT_FRESH_TTL_MS;
  const staleTtlMs = options.staleTtlMs ?? DEFAULT_STALE_TTL_MS;
  const store = cacheStore();
  const cached = store.get(cacheKey);
  const now = Date.now();

  if (cached && now - cached.cachedAt <= freshTtlMs) {
    return cachedJson(cached, "HIT", { "cache-control": options.cacheControl || "no-store" });
  }

  try {
    const response = await fetch(url, {
      ...init,
      headers: {
        ...jsonHeaders(),
        ...(init?.headers || {}),
      },
      cache: "no-store",
    });
    const payload = await readPayload(response);

    if (!response.ok) {
      if (cached && now - cached.cachedAt <= staleTtlMs) {
        return cachedJson(cached, "STALE", {
          "cache-control": options.cacheControl || "no-store",
          "x-recsys-upstream-status": String(response.status),
        });
      }
      return Response.json(
        {
          ok: false,
          status: response.status,
          message: errorMessage(payload, response.statusText),
          upstreamUrl: url,
          details: payload,
        } satisfies ApiErrorPayload,
        { status: response.status },
      );
    }

    store.set(cacheKey, { cachedAt: now, payload });
    return Response.json(payload as T, {
      headers: {
        "cache-control": options.cacheControl || "no-store",
        "x-recsys-cache": cached ? "REFRESHED" : "MISS",
      },
    });
  } catch (error) {
    if (cached && now - cached.cachedAt <= staleTtlMs) {
      return cachedJson(cached, "STALE", {
        "cache-control": options.cacheControl || "no-store",
        "x-recsys-upstream-error": error instanceof Error ? error.message : "Failed to reach recommendation API.",
      });
    }
    return Response.json(
      {
        ok: false,
        status: 502,
        message: error instanceof Error ? error.message : "Failed to reach recommendation API.",
        upstreamUrl: url,
      } satisfies ApiErrorPayload,
      { status: 502 },
    );
  }
}

function cachedJson(entry: CacheEntry, cacheStatus: "HIT" | "STALE", headers: Record<string, string> = {}) {
  return Response.json(annotateCachedPayload(entry.payload, cacheStatus, entry.cachedAt), {
    headers: {
      "x-recsys-cache": cacheStatus,
      "x-recsys-cache-age-seconds": String(Math.floor((Date.now() - entry.cachedAt) / 1000)),
      ...headers,
    },
  });
}

function annotateCachedPayload(payload: unknown, cacheStatus: "HIT" | "STALE", cachedAt: number) {
  if (cacheStatus !== "STALE" || typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return payload;
  }

  const clone = { ...(payload as Record<string, unknown>) };
  const metadata = typeof clone.metadata === "object" && clone.metadata !== null && !Array.isArray(clone.metadata)
    ? { ...(clone.metadata as Record<string, unknown>) }
    : {};
  const warnings = Array.isArray(metadata.warnings) ? metadata.warnings.map(String) : [];
  warnings.push(`serving stale cached response from ${new Date(cachedAt).toISOString()}`);
  metadata.warnings = warnings;
  metadata.cached = true;
  metadata.cache_status = cacheStatus.toLowerCase();
  clone.metadata = metadata;
  return clone;
}

async function readPayload(response: Response) {
  const text = await response.text();
  if (!text) {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function errorMessage(payload: unknown, fallback: string) {
  if (typeof payload === "object" && payload !== null) {
    const detail = "detail" in payload ? payload.detail : undefined;
    if (typeof detail === "string") {
      return detail;
    }
    if (typeof detail === "object" && detail !== null && "error" in detail) {
      const error = detail.error;
      if (typeof error === "object" && error !== null && "message" in error) {
        return String(error.message);
      }
    }
  }
  return fallback || "Recommendation API request failed.";
}
