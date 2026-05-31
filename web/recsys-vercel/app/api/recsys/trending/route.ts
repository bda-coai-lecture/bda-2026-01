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

  return proxyJson(upstream);
}
