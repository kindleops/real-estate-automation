/**
 * /api/internal/run-locks/release
 *
 * Alias for /api/internal/runs/release-lock.
 * Both paths call the same handler so either URL works in production.
 *
 * POST /api/internal/run-locks/release
 * Header: x-internal-api-secret: <INTERNAL_API_SECRET>
 * Body:   { "scope": "feeder:view:SMS / TIER #1 / ALL" }
 */
export { GET, POST } from "@/app/api/internal/runs/release-lock/route.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
