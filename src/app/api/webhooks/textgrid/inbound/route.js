import { NextResponse } from "next/server.js";

import { maybeHandleBuyerTextgridInbound } from "@/lib/domain/buyers/handle-buyer-response-webhook.js";
import { child } from "@/lib/logging/logger.js";
import { hasSupabaseConfig } from "@/lib/supabase/client.js";
import {
  logInboundMessageEvent as logSupabaseInboundMessageEvent,
  writeWebhookLog,
} from "@/lib/supabase/sms-engine.js";
import { handleTextgridInbound } from "@/lib/flows/handle-textgrid-inbound.js";
import {
  buildTextgridWebhookBypassResult,
  buildTextgridWebhookLogMeta,
  buildTextgridWebhookVerificationMeta,
  getTextgridWebhookSignatureMode,
  verifyTextgridWebhookRequest,
} from "@/lib/webhooks/textgrid-verify-webhook.js";
import { normalizeTextgridInboundPayload } from "@/lib/webhooks/textgrid-inbound-normalize.js";
import { captureRouteException, addSentryBreadcrumb } from "@/lib/monitoring/sentry.js";
import { captureSystemEvent } from "@/lib/analytics/posthog-server.js";
import { sendHotLeadAlert } from "@/lib/alerts/discord.js";
import { sendInboundSmsDiscordAlert } from "@/lib/discord/inbound-alerts.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const logger = child({
  module: "api.webhooks.textgrid.inbound",
});

const ROUTE_SECRET_ENV_NAMES = Object.freeze([
  "INTERNAL_API_SECRET",
  "CRON_SECRET",
  "SUPABASE_SERVICE_ROLE_KEY",
  "SUPABASE_ANON_KEY",
  "DISCORD_BOT_TOKEN",
  "TEXTGRID_AUTH_TOKEN",
  "TEXTGRID_WEBHOOK_SECRET",
  "PODIO_CLIENT_SECRET",
  "PODIO_PASSWORD",
  "OPENAI_API_KEY",
  "ANTHROPIC_API_KEY",
  "POSTHOG_KEY",
  "SENTRY_AUTH_TOKEN",
]);

const defaultDeps = {
  logger,
  maybeHandleBuyerTextgridInboundImpl: maybeHandleBuyerTextgridInbound,
  handleTextgridInboundImpl: handleTextgridInbound,
  verifyTextgridWebhookRequestImpl: verifyTextgridWebhookRequest,
  normalizeTextgridInboundPayloadImpl: normalizeTextgridInboundPayload,
  writeWebhookLogImpl: writeWebhookLog,
  logSupabaseInboundMessageEventImpl: logSupabaseInboundMessageEvent,
  sendInboundSmsDiscordAlertImpl: sendInboundSmsDiscordAlert,
};

let runtimeDeps = { ...defaultDeps };

function clean(value) {
  return String(value ?? "").trim();
}

function escapeRegExp(value = "") {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function redactRouteSecrets(value) {
  let text = String(value ?? "");
  for (const key of ROUTE_SECRET_ENV_NAMES) {
    text = text.replace(new RegExp(`\\b${escapeRegExp(key)}\\b`, "gi"), "[redacted_key]");
    const secret = clean(process.env[key]);
    if (secret.length >= 8) {
      text = text.replace(new RegExp(escapeRegExp(secret), "g"), "[redacted]");
    }
  }
  return text;
}

function safeErrorMessage(error, fallback = "Unknown error") {
  return redactRouteSecrets(error?.message || fallback);
}

function safeErrorStack(error) {
  return error?.stack ? redactRouteSecrets(error.stack) : null;
}

function asBool(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const normalized = clean(value).toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  return fallback;
}

function serializeForConsole(value) {
  try {
    return JSON.stringify(value, (_key, val) => {
      if (val instanceof Error) {
        return {
          name: val.name,
          message: val.message,
          stack: val.stack,
        };
      }
      return val;
    });
  } catch {
    return JSON.stringify({ serialization_error: true });
  }
}

function safeRouteLog(level, event, meta = {}) {
  try {
    const logFn = runtimeDeps?.logger?.[level];
    if (typeof logFn === "function") {
      logFn(event, meta);
    }
  } catch (log_error) {
    // Wrap the catch body so a console.error throw cannot escape safeRouteLog.
    try {
      console.error(
        serializeForConsole({
          event: `${event}.logger_failed`,
          log_error_message: log_error?.message || "unknown_logger_error",
          log_error_stack: log_error?.stack || null,
          original_event: event,
          original_level: level,
        })
      );
    } catch {}
  }
}

function isMainInboundHandlerDebugStage(stage = null) {
  return [
    "handler_entry",
    "after_extract",
    "after_normalize_from",
    "after_normalize_to",
    "after_inbound_received_log",
    "after_message_event_lookup",
    "after_brain_lookup",
    "after_phone_resolution",
    "after_message_event_create",
    "after_conversation_resolution",
    "after_prospect_resolution",
    "after_market_resolution",
    "after_podio_write",
    "handler_exit",
  ].includes(clean(stage));
}

async function parseRequestBody(request) {
  const contentType = clean(request.headers.get("content-type")).toLowerCase();

  if (contentType.includes("application/json")) {
    return await request.json().catch(() => ({}));
  }

  if (
    contentType.includes("application/x-www-form-urlencoded") ||
    contentType.includes("multipart/form-data")
  ) {
    const form = await request.formData().catch(() => null);
    return form ? Object.fromEntries(form.entries()) : {};
  }

  const text = await request.text().catch(() => "");
  return { raw_text: text };
}

export const __normalizeInboundPayloadForTest = normalizeTextgridInboundPayload;

export function __setTextgridInboundRouteTestDeps(overrides = {}) {
  runtimeDeps = { ...runtimeDeps, ...overrides };
}

export function __resetTextgridInboundRouteTestDeps() {
  runtimeDeps = { ...defaultDeps };
}

export async function GET() {
  return NextResponse.json({
    ok: true,
    route: "webhooks/textgrid/inbound",
    status: "listening",
  });
}

export async function POST(request) {
  let accepted_logged = false;
  let downstream_handler_invoked = false;
  let podio_persistence_attempted = false;
  let parsed_body_keys = [];
  let safe_message_id = null;
  let safe_from = null;
  let safe_to = null;
  let safe_status = null;
  let safe_signature_header_name = null;
  let safe_signature_verification_mode = null;
  let safe_signature_verified = false;
  let safe_signature_bypassed = false;
  let safe_signature_failure_reason = null;
  let safe_signature_unverified_observe_mode = false;
  let normalized_payload = null;
  let safe_webhook_verification = null;
  let inbound_alert_sent = false;
  let buyer_handler_failed = false;
  let buyer_handler_error_message = null;

  async function sendAcceptedInboundAlert({
    result = null,
    buyer_result = null,
    handler_name = null,
    failure = null,
    severity = null,
    final_response_status = null,
  } = {}) {
    if (inbound_alert_sent || !accepted_logged || !normalized_payload) return;
    inbound_alert_sent = true;

    try {
      await runtimeDeps.sendInboundSmsDiscordAlertImpl({
        payload: normalized_payload,
        result,
        buyer_result,
        provider_message_id: safe_message_id,
        from: safe_from,
        to: safe_to,
        message_body: normalized_payload?.message_body || normalized_payload?.message || null,
        handler_name,
        failure,
        severity,
        final_response_status,
        buyer_handler_failed,
        buyer_handler_error_message,
        webhook_verification: safe_webhook_verification,
      });
    } catch (alert_error) {
      safeRouteLog("warn", "textgrid_inbound.discord_alert_failed", {
        message_id: safe_message_id,
        message: safeErrorMessage(alert_error, "discord_alert_failed"),
      });
    }
  }

  try {
    const raw_body = await request.clone().text().catch(() => "");
    console.log("TEXTGRID INBOUND WEBHOOK HIT", serializeForConsole({ method: "POST", url: request.url }));
    const content_type = clean(request.headers.get("content-type"));
    console.log("INBOUND CONTENT TYPE", serializeForConsole({ content_type }));
    const body = await parseRequestBody(request);

    // form_params is the decoded key/value object when the body is form-encoded.
    // The Twilio signing algorithm needs these (sorted) to reproduce the digest.
    const is_form_encoded = content_type.toLowerCase().includes("application/x-www-form-urlencoded");
    const form_params = is_form_encoded && body && !body.raw_text ? body : null;
    parsed_body_keys = Object.keys(body || {});
    console.log("INBOUND PAYLOAD KEYS", serializeForConsole({ parsed_body_keys }));

    const payload = runtimeDeps.normalizeTextgridInboundPayloadImpl(body, request.headers);
    normalized_payload = payload;
    const request_url = new URL(request.url);
    const dry_run = asBool(
      body?.dry_run ??
      body?.dryRun ??
      payload?.dry_run ??
      request_url.searchParams.get("dry_run") ??
      request_url.searchParams.get("dryRun"),
      false
    );
    console.log("INBOUND BODY SOURCE", serializeForConsole({ body_source: payload?.body_source || null }));
    console.log("INBOUND MESSAGE BODY NORMALIZED", serializeForConsole({ message_body: payload?.message_body ?? null }));
    const signature_verification_mode = getTextgridWebhookSignatureMode();
    const verification =
      signature_verification_mode === "off"
        ? buildTextgridWebhookBypassResult({
            request_url: request.url,
            raw_body,
            form_params,
            content_type,
            signature: payload.header_signature,
            signature_header_name: payload.header_signature_name,
          })
        : runtimeDeps.verifyTextgridWebhookRequestImpl({
            request_url: request.url,
            raw_body,
            form_params,
            content_type,
            signature: payload.header_signature,
            signature_header_name: payload.header_signature_name,
          });
    const signature_meta = buildTextgridWebhookVerificationMeta({
      verification,
      mode: signature_verification_mode,
      signature_header_name: payload.header_signature_name,
    });
    const webhook_verification = {
      ...verification,
      ...signature_meta,
    };
    safe_webhook_verification = webhook_verification;
    safe_signature_verification_mode =
      webhook_verification?.signature_verification_mode || signature_verification_mode;
    safe_signature_verified = Boolean(webhook_verification?.signature_verified);
    safe_signature_bypassed = Boolean(webhook_verification?.signature_bypassed);
    safe_signature_failure_reason = webhook_verification?.signature_failure_reason || null;
    safe_signature_unverified_observe_mode = Boolean(
      webhook_verification?.signature_unverified_observe_mode
    );
    safe_signature_header_name =
      webhook_verification?.signature_header_name || payload?.header_signature_name || null;

    try {
      safe_message_id = payload?.message_id || null;
    } catch {}
    try {
      safe_from = payload?.from || null;
    } catch {}
    try {
      safe_to = payload?.to || null;
    } catch {}
    try {
      safe_status = clean(payload?.status) || null;
    } catch {}

    try {
      safeRouteLog(
        "info",
        "textgrid_inbound.normalized",
        buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
          extra: {
            event: payload.header_event || null,
            parsed_body_keys,
          },
        })
      );
    } catch (error) {
      console.error("SAFE_ROUTE_LOG_THROW_NORMALIZED", error.message, error.stack);
      return NextResponse.json(
        { ok: false, error: "textgrid_inbound_failed_safe_route_log" },
        { status: 500 }
      );
    }

    const inbound_debug_stage = request.headers.get("x-inbound-debug-stage");
    if (inbound_debug_stage === "after_normalized") {
      return NextResponse.json({ ok: true, stage: "after_normalized" });
    }

    if (inbound_debug_stage === "after_checkpoint_0") {
      return NextResponse.json({ ok: true, stage: "after_checkpoint_0" });
    }

    try {
      console.log(
        "INBOUND_CHECKPOINT_1",
        serializeForConsole({
          message_id: safe_message_id,
          from: safe_from,
          to: safe_to,
          parsed_body_keys,
          signature_verification_mode: safe_signature_verification_mode,
          next_statement: "build_checkpoint_base",
        })
      );
      try {
        runtimeDeps.logger.info("INBOUND_CHECKPOINT_1", {
          message_id: safe_message_id,
          from: safe_from,
          to: safe_to,
          parsed_body_keys,
          signature_verification_mode: safe_signature_verification_mode,
          next_statement: "build_checkpoint_base",
        });
      } catch (log_error) {
        console.error(
          "INBOUND_CHECKPOINT_1_LOGGER_FAILED",
          serializeForConsole({
            log_error_message: log_error?.message || "unknown_logger_error",
            log_error_stack: log_error?.stack || null,
          })
        );
      }

      const checkpoint_base = {
        message_id: safe_message_id,
        from: safe_from,
        to: safe_to,
        parsed_body_keys,
        signature_verification_mode: safe_signature_verification_mode,
      };

      if (inbound_debug_stage === "after_checkpoint_base") {
        return NextResponse.json({ ok: true, stage: "after_checkpoint_base" });
      }

      console.log(
        "INBOUND_CHECKPOINT_2",
        serializeForConsole({
          ...checkpoint_base,
          next_statement: "compute_signature_invalid",
        })
      );
      try {
        runtimeDeps.logger.info("INBOUND_CHECKPOINT_2", {
          ...checkpoint_base,
          next_statement: "compute_signature_invalid",
        });
      } catch (log_error) {
        console.error(
          "INBOUND_CHECKPOINT_2_LOGGER_FAILED",
          serializeForConsole({
            log_error_message: log_error?.message || "unknown_logger_error",
            log_error_stack: log_error?.stack || null,
          })
        );
      }

      const signature_invalid = Boolean(verification.required && !verification.ok);

      if (inbound_debug_stage === "after_signature_invalid") {
        return NextResponse.json({ ok: true, stage: "after_signature_invalid" });
      }

      console.log(
        "INBOUND_CHECKPOINT_3",
        serializeForConsole({
          ...checkpoint_base,
          signature_invalid,
          next_statement: "compute_signature_continuation",
        })
      );
      try {
        runtimeDeps.logger.info("INBOUND_CHECKPOINT_3", {
          ...checkpoint_base,
          signature_invalid,
          next_statement: "compute_signature_continuation",
        });
      } catch (log_error) {
        console.error(
          "INBOUND_CHECKPOINT_3_LOGGER_FAILED",
          serializeForConsole({
            log_error_message: log_error?.message || "unknown_logger_error",
            log_error_stack: log_error?.stack || null,
          })
        );
      }

      const will_continue_after_signature_check = !(
        verification.required &&
        !verification.ok &&
        safe_signature_verification_mode === "strict"
      );

      if (inbound_debug_stage === "after_signature_gate") {
        return NextResponse.json({ ok: true, stage: "after_signature_gate" });
      }

      console.log(
        "INBOUND_CHECKPOINT_4",
        serializeForConsole({
          ...checkpoint_base,
          signature_invalid,
          will_continue_after_signature_check,
          next_statement: "log_signature_branch_selected",
        })
      );
      try {
        runtimeDeps.logger.info("INBOUND_CHECKPOINT_4", {
          ...checkpoint_base,
          signature_invalid,
          will_continue_after_signature_check,
          next_statement: "log_signature_branch_selected",
        });
      } catch (log_error) {
        console.error(
          "INBOUND_CHECKPOINT_4_LOGGER_FAILED",
          serializeForConsole({
            log_error_message: log_error?.message || "unknown_logger_error",
            log_error_stack: log_error?.stack || null,
          })
        );
      }

      const branch_meta = {
        message_id: safe_message_id,
        from: safe_from,
        to: safe_to,
        status: safe_status,
        signature_verification_mode: safe_signature_verification_mode,
        signature_verified: safe_signature_verified,
        signature_bypassed: safe_signature_bypassed,
        signature_failure_reason: safe_signature_failure_reason,
        signature_header_name: safe_signature_header_name,
        signature_unverified_observe_mode: safe_signature_unverified_observe_mode,
        downstream_handler_invoked: false,
        podio_persistence_attempted: false,
        final_response_status: null,
        ...webhook_verification?.diagnostics,
        parsed_body_keys,
        signature_invalid,
        will_continue_after_signature_check,
      };

      console.log(
        "textgrid_inbound.signature_branch_selected",
        serializeForConsole(branch_meta)
      );
      try {
        runtimeDeps.logger.info("textgrid_inbound.signature_branch_selected", branch_meta);
      } catch (log_error) {
        console.error(
          "textgrid_inbound.signature_branch_selected.logger_failed",
          serializeForConsole({
            log_error_message: log_error?.message || "unknown_logger_error",
            log_error_stack: log_error?.stack || null,
          })
        );
      }

      if (inbound_debug_stage === "after_signature_branch_selected") {
        return NextResponse.json({ ok: true, stage: "after_signature_branch_selected" });
      }

      if (hasSupabaseConfig()) {
        try {
          await runtimeDeps.writeWebhookLogImpl({
            event_type: payload.header_event || "inbound",
            direction: "inbound",
            provider_message_sid: payload.message_id || null,
            payload,
            headers: Object.fromEntries(request.headers.entries()),
            received_at: new Date().toISOString(),
            source: "textgrid",
          });
        } catch (webhook_log_error) {
          safeRouteLog("error", "textgrid_inbound.webhook_log_failed", {
            message: safeErrorMessage(webhook_log_error, "webhook_log_write_failed"),
          });
        }
      }

      if (!payload.from) {
        const invalid_payload_meta = {
          ...branch_meta,
          response_error: "invalid_textgrid_inbound_payload",
        };
        console.error(
          "textgrid_inbound.invalid_payload",
          serializeForConsole(invalid_payload_meta)
        );
        try {
          runtimeDeps.logger.warn("textgrid_inbound.invalid_payload", invalid_payload_meta);
        } catch (log_error) {
          console.error(
            "textgrid_inbound.invalid_payload.logger_failed",
            serializeForConsole({
              log_error_message: log_error?.message || "unknown_logger_error",
              log_error_stack: log_error?.stack || null,
            })
          );
        }

        const invalid_payload_log_meta = {
          ...invalid_payload_meta,
          payload,
        };
        try {
          runtimeDeps.logger.warn("textgrid_inbound.invalid_payload.details", invalid_payload_log_meta);
        } catch {}

        const invalid_payload_response_meta = {
          ...branch_meta,
          final_response_status: 400,
          response_error: "invalid_textgrid_inbound_payload",
        };
        console.log(
          "textgrid_inbound.response_sent",
          serializeForConsole(invalid_payload_response_meta)
        );
        try {
          runtimeDeps.logger.info("textgrid_inbound.response_sent", invalid_payload_response_meta);
        } catch (log_error) {
          console.error(
            "textgrid_inbound.response_sent.logger_failed",
            serializeForConsole({
              log_error_message: log_error?.message || "unknown_logger_error",
              log_error_stack: log_error?.stack || null,
            })
          );
        }

        return NextResponse.json(
          {
            ok: false,
            error: "invalid_textgrid_inbound_payload",
          },
          { status: 400 }
        );
      }

      if (verification.required && !verification.ok) {
        const invalid_signature_meta = {
          ...branch_meta,
        };
        console.error(
          "textgrid_inbound.invalid_signature",
          serializeForConsole(invalid_signature_meta)
        );
        try {
          runtimeDeps.logger.warn("textgrid_inbound.invalid_signature", invalid_signature_meta);
        } catch (log_error) {
          console.error(
            "textgrid_inbound.invalid_signature.logger_failed",
            serializeForConsole({
              log_error_message: log_error?.message || "unknown_logger_error",
              log_error_stack: log_error?.stack || null,
            })
          );
        }

        if (safe_signature_verification_mode === "strict") {
          const strict_response_meta = {
            ...branch_meta,
            final_response_status: 401,
            response_error: "invalid_textgrid_signature",
          };
          console.log(
            "textgrid_inbound.response_sent",
            serializeForConsole(strict_response_meta)
          );
          try {
            runtimeDeps.logger.info("textgrid_inbound.response_sent", strict_response_meta);
          } catch (log_error) {
            console.error(
              "textgrid_inbound.response_sent.logger_failed",
              serializeForConsole({
                log_error_message: log_error?.message || "unknown_logger_error",
                log_error_stack: log_error?.stack || null,
              })
            );
          }

          return NextResponse.json(
            {
              ok: false,
              error: "invalid_textgrid_signature",
              verification: webhook_verification,
            },
            { status: 401 }
          );
        }
      }

      if (safe_signature_verification_mode === "off") {
        const disabled_meta = {
          ...branch_meta,
          signature_verification_disabled: true,
        };
        console.error(
          "textgrid_inbound.signature_verification_disabled",
          serializeForConsole(disabled_meta)
        );
        try {
          runtimeDeps.logger.warn("textgrid_inbound.signature_verification_disabled", disabled_meta);
        } catch (log_error) {
          console.error(
            "textgrid_inbound.signature_verification_disabled.logger_failed",
            serializeForConsole({
              log_error_message: log_error?.message || "unknown_logger_error",
              log_error_stack: log_error?.stack || null,
            })
          );
        }
      }

      payload.webhook_verification = webhook_verification;
      Object.assign(payload, signature_meta);

      const accepted_meta = {
        ...branch_meta,
      };
      console.log("textgrid_inbound.accepted", serializeForConsole(accepted_meta));
      try {
        runtimeDeps.logger.info("textgrid_inbound.accepted", accepted_meta);
      } catch (log_error) {
        console.error(
          "textgrid_inbound.accepted.logger_failed",
          serializeForConsole({
            log_error_message: log_error?.message || "unknown_logger_error",
            log_error_stack: log_error?.stack || null,
          })
        );
      }
      accepted_logged = true;

      addSentryBreadcrumb("textgrid_inbound", "inbound_message_accepted", {
        provider_message_id: safe_message_id,
        to: safe_to,
      });

      if (hasSupabaseConfig()) {
        try {
          await runtimeDeps.logSupabaseInboundMessageEventImpl(payload, {
            now: new Date().toISOString(),
          });
          console.log("INBOUND MESSAGE EVENT WRITTEN", serializeForConsole({
            provider_message_sid: payload.message_id || null,
            body_source: payload.body_source || null,
            message_body: payload.message_body ?? null,
          }));
        } catch (supabase_error) {
          safeRouteLog(
            "error",
            "textgrid_inbound.supabase_logging_failed",
            buildTextgridWebhookLogMeta({
              payload,
              webhook_verification,
              downstream_handler_invoked,
              podio_persistence_attempted,
              extra: {
                message: safeErrorMessage(supabase_error, "Unknown Supabase inbound logging error"),
              },
            })
          );
        }
      }

      if (inbound_debug_stage === "after_accepted") {
        await sendAcceptedInboundAlert({
          result: { ok: true, stage: "after_accepted" },
          handler_name: "textgrid_inbound_route",
          final_response_status: 200,
        });
        return NextResponse.json({ ok: true, stage: "after_accepted" });
      }
    } catch (error) {
      const error_meta = {
        message_id: safe_message_id,
        from: safe_from,
        to: safe_to,
        status: safe_status,
        signature_verification_mode: safe_signature_verification_mode,
        signature_verified: safe_signature_verified,
        signature_bypassed: safe_signature_bypassed,
        signature_failure_reason: safe_signature_failure_reason,
        signature_header_name: safe_signature_header_name,
        signature_unverified_observe_mode: safe_signature_unverified_observe_mode,
        downstream_handler_invoked,
        podio_persistence_attempted,
        final_response_status: 500,
        parsed_body_keys,
        error_message: safeErrorMessage(error),
        error_stack: safeErrorStack(error),
      };

      console.error("textgrid_inbound.failed_pre_accept", serializeForConsole(error_meta));
      try {
        runtimeDeps.logger.error("textgrid_inbound.failed_pre_accept", error_meta);
      } catch (log_error) {
        console.error(
          "textgrid_inbound.failed_pre_accept.logger_failed",
          serializeForConsole({
            log_error_message: log_error?.message || "unknown_logger_error",
            log_error_stack: log_error?.stack || null,
          })
        );
      }

      captureRouteException(error, {
        route: "webhooks/textgrid/inbound",
        subsystem: "textgrid_inbound",
        context: {
          provider_message_id: safe_message_id,
          to: safe_to,
          stage: "pre_accept",
        },
      });

      console.error("textgrid_inbound.failed", serializeForConsole(error_meta));
      try {
        runtimeDeps.logger.error("textgrid_inbound.failed", error_meta);
      } catch (log_error) {
        console.error(
          "textgrid_inbound.failed.logger_failed",
          serializeForConsole({
            log_error_message: log_error?.message || "unknown_logger_error",
            log_error_stack: log_error?.stack || null,
          })
        );
      }

      const response_meta = {
        ...error_meta,
        response_error: "textgrid_inbound_failed_pre_accept",
      };
      console.log("textgrid_inbound.response_sent", serializeForConsole(response_meta));
      try {
        runtimeDeps.logger.info("textgrid_inbound.response_sent", response_meta);
      } catch (log_error) {
        console.error(
          "textgrid_inbound.response_sent.logger_failed",
          serializeForConsole({
            log_error_message: log_error?.message || "unknown_logger_error",
            log_error_stack: log_error?.stack || null,
          })
        );
      }

      return NextResponse.json(
        {
          ok: false,
          error: "textgrid_inbound_failed_pre_accept",
        },
        { status: 500 }
      );
    }

    if (inbound_debug_stage === "before_handler") {
      await sendAcceptedInboundAlert({
        result: { ok: true, stage: "before_handler" },
        handler_name: "textgrid_inbound_route",
        final_response_status: 200,
      });
      return NextResponse.json({ ok: true, stage: "before_handler" });
    }

    const bypass_buyer_handler_for_debug = isMainInboundHandlerDebugStage(inbound_debug_stage);

    if (bypass_buyer_handler_for_debug) {
      const bypass_meta = buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        downstream_handler_invoked: false,
        podio_persistence_attempted: false,
        extra: {
          inbound_debug_stage,
          buyer_handler_bypassed_for_debug: true,
        },
      });
      console.log(
        "textgrid_inbound.buyer_handler_bypassed_for_debug",
        serializeForConsole(bypass_meta)
      );
      try {
        runtimeDeps.logger.info("textgrid_inbound.buyer_handler_bypassed_for_debug", bypass_meta);
      } catch (log_error) {
        console.error(
          "textgrid_inbound.buyer_handler_bypassed_for_debug.logger_failed",
          serializeForConsole({
            log_error_message: log_error?.message || "unknown_logger_error",
            log_error_stack: log_error?.stack || null,
          })
        );
      }
    }

    let buyer_result = { ok: true, matched: false, reason: "buyer_handler_not_invoked" };

    if (!bypass_buyer_handler_for_debug) {
      downstream_handler_invoked = true;
      safeRouteLog(
        "info",
        "textgrid_inbound.handler_started",
        buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
          downstream_handler_invoked: true,
          extra: {
            handler_name: "maybeHandleBuyerTextgridInbound",
          },
        })
      );

      try {
        buyer_result = await runtimeDeps.maybeHandleBuyerTextgridInboundImpl(payload);
      } catch (error) {
        buyer_handler_failed = true;
        buyer_handler_error_message = safeErrorMessage(error);
        const buyer_error_meta = buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
          downstream_handler_invoked: true,
          podio_persistence_attempted: false,
          final_response_status: 500,
          extra: {
            handler_name: "maybeHandleBuyerTextgridInbound",
            error_message: safeErrorMessage(error),
            error_stack: safeErrorStack(error),
            inbound_debug_stage,
            will_continue_to_main_handler: true,
          },
        });
        console.error(
          "textgrid_inbound.buyer_handler_failed",
          serializeForConsole(buyer_error_meta)
        );
        try {
          runtimeDeps.logger.error("textgrid_inbound.buyer_handler_failed", buyer_error_meta);
        } catch (log_error) {
          console.error(
            "textgrid_inbound.buyer_handler_failed.logger_failed",
            serializeForConsole({
              log_error_message: log_error?.message || "unknown_logger_error",
              log_error_stack: log_error?.stack || null,
            })
          );
        }
        captureRouteException(error, {
          route: "webhooks/textgrid/inbound",
          subsystem: "textgrid_inbound",
          context: {
            provider_message_id: safe_message_id,
            to: safe_to,
            stage: "buyer_handler",
          },
        });
        buyer_result = {
          ok: false,
          matched: false,
          reason: "buyer_handler_failed",
          error_message: safeErrorMessage(error),
        };
      }
    }

    if (inbound_debug_stage === "after_handler") {
      await sendAcceptedInboundAlert({
        result: { ok: true, stage: "after_handler", buyer_matched: Boolean(buyer_result?.matched) },
        buyer_result,
        handler_name: "maybeHandleBuyerTextgridInbound",
        final_response_status: 200,
      });
      return NextResponse.json({ ok: true, stage: "after_handler", buyer_matched: Boolean(buyer_result?.matched) });
    }

    if (!bypass_buyer_handler_for_debug) {
      safeRouteLog(
        "info",
        "textgrid_inbound.handler_completed",
        buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
          downstream_handler_invoked: true,
          podio_persistence_attempted: Boolean(buyer_result?.matched),
          extra: {
            handler_name: "maybeHandleBuyerTextgridInbound",
            buyer_disposition_matched: Boolean(buyer_result?.matched),
          },
        })
      );
    }

    if (!bypass_buyer_handler_for_debug && buyer_result?.matched) {
      podio_persistence_attempted = true;
      safeRouteLog("info", "textgrid_inbound.routed_to_buyer_disposition", {
        message_id: payload.message_id || null,
        from: payload.from || null,
        buyer_match_item_id: buyer_result?.result?.buyer_match_item_id || null,
        company_item_id: buyer_result?.result?.company_item_id || null,
        correlation_mode: buyer_result?.correlation_mode || null,
      });

      const response_status = buyer_result?.result?.ok === false ? 400 : 200;
      safeRouteLog(
        "info",
        "textgrid_inbound.response_sent",
        buildTextgridWebhookLogMeta({
          payload,
          webhook_verification,
          downstream_handler_invoked: true,
          podio_persistence_attempted: true,
          final_response_status: response_status,
          extra: {
            buyer_disposition: true,
          },
        })
      );

      await sendAcceptedInboundAlert({
        result: buyer_result?.result || null,
        buyer_result,
        handler_name: "maybeHandleBuyerTextgridInbound",
        final_response_status: response_status,
        severity: response_status >= 400 ? "warning" : "info",
      });

      return NextResponse.json(
        {
          ok: buyer_result?.result?.ok !== false,
          route: "webhooks/textgrid/inbound",
          verification: webhook_verification,
          buyer_disposition: true,
          result: buyer_result.result,
        },
        { status: buyer_result?.result?.ok === false ? 400 : 200 }
      );
    }

    downstream_handler_invoked = true;
    podio_persistence_attempted = true;
    safeRouteLog(
      "info",
      "textgrid_inbound.handler_started",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        downstream_handler_invoked: true,
        podio_persistence_attempted: true,
        extra: {
          handler_name: "handleTextgridInbound",
          buyer_handler_failed,
          buyer_handler_error_message,
        },
      })
    );

    const result = await runtimeDeps.handleTextgridInboundImpl(payload, {
      inbound_debug_stage,
      dry_run,
      auto_reply_enabled: asBool(process.env.INBOUND_AUTOPILOT_ENABLED, true),
      auto_post_discord_card: asBool(process.env.INBOUND_AUTOPILOT_POST_DISCORD_CARD, true),
      auto_reply_delay_seconds: Number.parseInt(process.env.INBOUND_AUTOPILOT_DELAY_SECONDS || "60", 10) || 60,
      inbound_user_initiated: true,
    });

    captureSystemEvent("inbound_sms_classified", {
      provider_message_id: payload?.message_id || null,
      ok: result?.ok !== false,
      buyer_matched: Boolean(buyer_result?.matched),
      buyer_handler_failed,
      retryable: Boolean(result?.retryable),
      reason: result?.reason || null,
    });

    if (
      result?.ok &&
      !result?.inbound_is_negative &&
      (result?.offer?.created || result?.offer_progress?.updated || result?.contract?.created)
    ) {
      sendHotLeadAlert({
        title: "Hot Seller Reply",
        description: "Inbound SMS triggered a deal-stage advancement",
        color: 0x27ae60,
        fields: [
          { name: "Offer Created", value: String(Boolean(result.offer?.created)), inline: true },
          { name: "Offer Progressed", value: String(Boolean(result.offer_progress?.updated)), inline: true },
          { name: "Contract Created", value: String(Boolean(result.contract?.created)), inline: true },
          { name: "Route Stage", value: String(result.route?.stage || "?"), inline: false },
        ],
        timestamp: new Date().toISOString(),
        footer: { text: "webhooks/textgrid/inbound" },
      });
    }

    safeRouteLog(
      "info",
      "textgrid_inbound.handler_completed",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        downstream_handler_invoked: true,
        podio_persistence_attempted: true,
        extra: {
          handler_name: "handleTextgridInbound",
          handler_ok: result?.ok !== false,
          buyer_handler_failed,
          buyer_handler_error_message,
        },
      })
    );

    const main_handler_response_status = result?.retryable ? 503 : 200;
    const response_headers = {};
    const retry_after_seconds = Number(result?.retry_after_seconds);
    if (Number.isFinite(retry_after_seconds) && retry_after_seconds > 0) {
      response_headers["Retry-After"] = String(Math.max(1, Math.ceil(retry_after_seconds)));
    }

    safeRouteLog(
      "info",
      "textgrid_inbound.response_sent",
      buildTextgridWebhookLogMeta({
        payload,
        webhook_verification,
        downstream_handler_invoked: true,
        podio_persistence_attempted: true,
        final_response_status: main_handler_response_status,
        extra: {
          buyer_handler_failed,
          buyer_handler_error_message,
          retryable: Boolean(result?.retryable),
          retry_after_seconds: response_headers["Retry-After"] || null,
          retry_after_at: result?.retry_after_at || null,
        },
      })
    );

    await sendAcceptedInboundAlert({
      result,
      buyer_result,
      handler_name: "handleTextgridInbound",
      final_response_status: main_handler_response_status,
      severity: result?.ok === false ? (result?.retryable ? "warning" : "error") : null,
    });

    if (result?.unknown_router && result?.context?.unknown_inbound) {
      return NextResponse.json(
        {
          ok: result?.ok !== false,
          route: "webhooks/textgrid/inbound",
          verification: webhook_verification,
          buyer_handler_failed,
          context: result.context,
          unknown_router: result.unknown_router,
          result,
        },
        {
          status: main_handler_response_status,
          headers: response_headers,
        }
      );
    }

    return NextResponse.json(
      {
        ok: result?.ok !== false,
        route: "webhooks/textgrid/inbound",
        verification: webhook_verification,
        buyer_handler_failed,
        result,
      },
      {
        status: main_handler_response_status,
        headers: response_headers,
      }
    );
  } catch (error) {
    const failure_meta = {
      message_id: safe_message_id,
      from: safe_from,
      to: safe_to,
      status: safe_status,
      signature_verification_mode: safe_signature_verification_mode,
      signature_verified: safe_signature_verified,
      signature_bypassed: safe_signature_bypassed,
      signature_failure_reason: safe_signature_failure_reason,
      signature_header_name: safe_signature_header_name,
      signature_unverified_observe_mode: safe_signature_unverified_observe_mode,
      downstream_handler_invoked,
      podio_persistence_attempted,
      final_response_status: 500,
      parsed_body_keys,
      accepted_logged,
      error_message: safeErrorMessage(error),
      error_stack: safeErrorStack(error),
    };

    console.error("textgrid_inbound.failed", serializeForConsole(failure_meta));
    try {
      runtimeDeps.logger.error("textgrid_inbound.failed", failure_meta);
    } catch (log_error) {
      console.error(
        "textgrid_inbound.failed.logger_failed",
        serializeForConsole({
          log_error_message: log_error?.message || "unknown_logger_error",
          log_error_stack: log_error?.stack || null,
        })
      );
    }

    captureRouteException(error, {
      route: "webhooks/textgrid/inbound",
      subsystem: "textgrid_inbound",
      context: {
        provider_message_id: safe_message_id,
        to: safe_to,
        downstream_handler_invoked,
        accepted_logged,
      },
    });

    if (!accepted_logged) {
      console.error("textgrid_inbound.failed_pre_accept", serializeForConsole(failure_meta));
      try {
        runtimeDeps.logger.error("textgrid_inbound.failed_pre_accept", failure_meta);
      } catch (log_error) {
        console.error(
          "textgrid_inbound.failed_pre_accept.logger_failed",
          serializeForConsole({
            log_error_message: log_error?.message || "unknown_logger_error",
            log_error_stack: log_error?.stack || null,
          })
        );
      }
    }

    const response_meta = {
      ...failure_meta,
      response_error: "textgrid_inbound_failed",
    };
    console.log("textgrid_inbound.response_sent", serializeForConsole(response_meta));
    try {
      runtimeDeps.logger.info("textgrid_inbound.response_sent", response_meta);
    } catch (log_error) {
      console.error(
        "textgrid_inbound.response_sent.logger_failed",
        serializeForConsole({
          log_error_message: log_error?.message || "unknown_logger_error",
          log_error_stack: log_error?.stack || null,
        })
      );
    }

    await sendAcceptedInboundAlert({
      handler_name: downstream_handler_invoked ? "handleTextgridInbound" : "textgrid_inbound_route",
      failure: {
        error_message: safeErrorMessage(error),
      },
      severity: "error",
      final_response_status: 500,
    });

    return NextResponse.json(
      {
        ok: false,
        error: "textgrid_inbound_failed",
      },
      { status: 500 }
    );
  }
}
