import { sendTextgridSMS } from "@/lib/providers/textgrid.js";
import { requireDevRouteAccess } from "@/lib/security/dev-route-guard.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request) {
  const denied = requireDevRouteAccess(request);

  if (denied) {
    return denied;
  }

  try {
    const result = await sendTextgridSMS({
      from: "+16128060495",
      to: "+16127433952",
      body: "🔥 FORCE SEND TEST",
    });

    console.log("FORCE SEND RESULT:", result);

    return Response.json({
      success: true,
      result,
    });
  } catch (err) {
    console.error("FORCE SEND ERROR:", err);

    return Response.json({
      success: false,
      error: err?.message || "Unknown error",
    });
  }
}
