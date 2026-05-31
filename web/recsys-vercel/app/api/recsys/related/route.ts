import { apiBaseUrl, proxyJson } from "@/lib/recsys-api";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const owner = searchParams.get("owner")?.trim();
  const repo = searchParams.get("repo")?.trim();
  const limit = searchParams.get("limit") || "10";

  if (!owner || !repo) {
    return Response.json(
      {
        ok: false,
        status: 400,
        message: "owner and repo are required.",
        upstreamUrl: "",
      },
      { status: 400 },
    );
  }

  let upstream: string;
  try {
    upstream = `${apiBaseUrl(request)}/api/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/related?limit=${encodeURIComponent(limit)}`;
  } catch (error) {
    return Response.json({ message: error instanceof Error ? error.message : String(error) }, { status: 500 });
  }
  return proxyJson(upstream, undefined, {
    cacheKey: `related:${owner.toLowerCase()}/${repo.toLowerCase()}:${limit}`,
    cacheControl: "public, s-maxage=300, stale-while-revalidate=86400",
    freshTtlMs: 5 * 60 * 1000,
    staleTtlMs: 7 * 24 * 60 * 60 * 1000,
  });
}
