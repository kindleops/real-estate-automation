import { NextResponse } from "next/server";

import { child } from "@/lib/logging/logger.js";
import { handleFeedCandidatesRequest } from "@/lib/domain/outbound/feed-candidates-request.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 300;

const logger = child({ module: "api.internal.outbound.feed_candidates" });

export async function GET(request) {
  return handleFeedCandidatesRequest(request, "GET", {
    route: "internal/outbound/feed-candidates",
    logger,
    jsonResponse: NextResponse.json,
  });
}

export async function POST(request) {
  return handleFeedCandidatesRequest(request, "POST", {
    route: "internal/outbound/feed-candidates",
    logger,
    jsonResponse: NextResponse.json,
  });
}
