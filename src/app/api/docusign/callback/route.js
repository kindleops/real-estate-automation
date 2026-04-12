import { NextResponse } from "next/server";

import {
  exchangeDocusignAuthorizationCode,
  resolveDocusignRedirectUri,
} from "@/lib/providers/docusign.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function clean(value) {
  return String(value ?? "").trim();
}

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = clean(value).toLowerCase();
  if (["true", "1", "yes"].includes(normalized)) return true;
  if (["false", "0", "no"].includes(normalized)) return false;
  return fallback;
}

function inferTargetFromOrigin(origin = "") {
  const normalized = clean(origin).toLowerCase();
  if (!normalized) return "auto";
  if (normalized.includes("localhost") || normalized.includes("127.0.0.1")) return "local";
  if (normalized.includes("vercel.app")) return "preview";
  return "prod";
}

export async function GET(request) {
  const { searchParams, origin } = new URL(request.url);
  const code = clean(searchParams.get("code"));
  const state = clean(searchParams.get("state"));
  const redirect_uri = clean(searchParams.get("redirect_uri"));
  const code_verifier = clean(searchParams.get("code_verifier"));
  const target = clean(searchParams.get("target")) || inferTargetFromOrigin(origin);
  const dry_run = asBoolean(searchParams.get("dry_run"), false);
  const provider_error = clean(searchParams.get("error"));
  const provider_error_description = clean(searchParams.get("error_description"));

  if (provider_error) {
    return NextResponse.json(
      {
        ok: false,
        route: "docusign/callback",
        reason: provider_error,
        error_description: provider_error_description || null,
        state: state || null,
      },
      { status: 400 }
    );
  }

  if (!code) {
    return NextResponse.json(
      {
        ok: false,
        route: "docusign/callback",
        reason: "missing_authorization_code",
        redirect_uri:
          redirect_uri ||
          resolveDocusignRedirectUri({
            target,
            origin,
          }),
        state: state || null,
      },
      { status: 400 }
    );
  }

  const result = await exchangeDocusignAuthorizationCode({
    code,
    redirect_uri: redirect_uri || null,
    target,
    code_verifier: code_verifier || null,
    dry_run,
  });

  return NextResponse.json(
    {
      ok: result?.ok !== false,
      route: "docusign/callback",
      reason: result?.reason || null,
      environment: result?.environment || null,
      redirect_uri: result?.redirect_uri || null,
      token_type: result?.token_type || null,
      expires_in: result?.expires_in || null,
      access_token_present: Boolean(clean(result?.access_token)),
      refresh_token_present: Boolean(clean(result?.refresh_token)),
      state: state || null,
    },
    { status: result?.ok === false ? 400 : 200 }
  );
}
