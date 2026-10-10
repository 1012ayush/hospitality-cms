import { NextResponse } from "next/server";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const propertyId = searchParams.get("propertyId");
  
  const baseUrl = process.env.DASHBOARD_API_BASE_URL;
  const token = process.env.DASHBOARD_API_TOKEN;

  if (!baseUrl || !token) {
    return NextResponse.json({ error: "Missing Vercel environment variables" }, { status: 500 });
  }

  try {
    const response = await fetch(`${baseUrl}/v1/dev-dashboard/sync-logs?propertyId=${propertyId}`, {
      headers: {
        "Authorization": `Bearer ${token}` 
      },
      cache: "no-store"
    });

    if (!response.ok) {
      throw new Error(`Admin API rejected request with status: ${response.status}`);
    }

    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error("Vercel proxy error:", error);
    return NextResponse.json({ error: String(error) }, { status: 500 });
  }
}