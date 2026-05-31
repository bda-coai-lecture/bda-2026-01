import { apiBaseUrl, proxyJson } from "@/lib/recsys-api";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const limit = searchParams.get("limit") || "10";
  let upstream: string;
  try {
    upstream = `${apiBaseUrl(request)}/api/trending?limit=${encodeURIComponent(limit)}`;
  } catch (error) {
    return Response.json({ message: error instanceof Error ? error.message : String(error) }, { status: 500 });
  }

  return proxyJson(upstream, undefined, {
    cacheKey: `trending:${limit}`,
    cacheControl: "public, s-maxage=300, stale-while-revalidate=86400",
    freshTtlMs: 5 * 60 * 1000,
    staleTtlMs: 7 * 24 * 60 * 60 * 1000,
  });
}
