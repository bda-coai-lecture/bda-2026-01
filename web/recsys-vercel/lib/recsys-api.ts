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

export async function proxyJson<T>(url: string, init?: RequestInit): Promise<Response> {
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

    return Response.json(payload as T);
  } catch (error) {
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
