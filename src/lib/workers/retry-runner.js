// ─── retry-runner.js ─────────────────────────────────────────────────────
import { retrySendQueue } from "@/lib/domain/queue/retry-send-queue.js";
import { recordSystemAlert, resolveSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { withRunLock } from "@/lib/domain/runs/run-locks.js";
import {
  buildPodioBackpressureSkipResult,
  buildPodioCooldownSkipResult,
  isPodioRateLimitError,
} from "@/lib/providers/podio.js";
import { info, warn } from "@/lib/logging/logger.js";
import { getSystemFlag } from "@/lib/system-control.js";

const DEFAULT_RETRY_LIMIT = 50;

export async function runRetryRunner({
  limit = DEFAULT_RETRY_LIMIT,
  master_owner_id = null,
} = {}, deps = {}) {
  const retry_enabled = await getSystemFlag("retry_enabled");
  if (!retry_enabled) {
    warn("queue.retry_runner_disabled", {
      limit,
      master_owner_id: Number(master_owner_id || 0) || null,
      flag_key: "retry_enabled",
    });

    return {
      ok: true,
      skipped: true,
      reason: "system_control_disabled",
      flag_key: "retry_enabled",
      processed_count: 0,
      retried_count: 0,
      scheduled_count: 0,
      terminal_count: 0,
      skipped_count: 0,
      scanned_count: 0,
      results: [],
      retry_after_seconds: null,
      retry_after_at: null,
    };
  }

  const scoped_master_owner_id = Number(master_owner_id || 0) || null;
  const with_run_lock = deps.withRunLock || withRunLock;
  const record_system_alert = deps.recordSystemAlert || recordSystemAlert;
  const resolve_system_alert = deps.resolveSystemAlert || resolveSystemAlert;
  const retry_send_queue = deps.retrySendQueue || retrySendQueue;
  const build_cooldown_skip_result =
    deps.buildPodioCooldownSkipResult || buildPodioCooldownSkipResult;
  const build_backpressure_skip_result =
    deps.buildPodioBackpressureSkipResult || buildPodioBackpressureSkipResult;

  const cooldown_skip = await build_cooldown_skip_result({
    processed_count: 0,
    retried_count: 0,
    scheduled_count: 0,
    terminal_count: 0,
    skipped_count: 0,
    scanned_count: 0,
    results: [],
    master_owner_id: scoped_master_owner_id,
  });

  if (cooldown_skip?.podio_cooldown?.active) {
    warn("queue.retry_runner_skipped_podio_cooldown", {
      limit,
      master_owner_id: scoped_master_owner_id,
      retry_after_seconds: cooldown_skip.retry_after_seconds,
      retry_after_at: cooldown_skip.retry_after_at,
      podio_status: cooldown_skip.podio_cooldown?.status ?? null,
      podio_path: cooldown_skip.podio_cooldown?.path ?? null,
      podio_operation: cooldown_skip.podio_cooldown?.operation ?? null,
      rate_limit_remaining:
        cooldown_skip.podio_cooldown?.rate_limit_remaining ?? null,
      rate_limit_limit:
        cooldown_skip.podio_cooldown?.rate_limit_limit ?? null,
    });

    return cooldown_skip;
  }

  const backpressure_skip = await build_backpressure_skip_result(
    {
      processed_count: 0,
      retried_count: 0,
      scheduled_count: 0,
      terminal_count: 0,
      skipped_count: 0,
      scanned_count: 0,
      results: [],
      master_owner_id: scoped_master_owner_id,
    },
    {
      min_remaining: 100,
      max_age_ms: 10 * 60_000,
    }
  );

  if (backpressure_skip?.podio_backpressure?.active) {
    warn("queue.retry_runner_skipped_podio_backpressure", {
      limit,
      master_owner_id: scoped_master_owner_id,
      reason: backpressure_skip.reason,
      min_remaining:
        backpressure_skip.podio_backpressure?.min_remaining ?? null,
      rate_limit_remaining:
        backpressure_skip.podio_backpressure?.observation?.rate_limit_remaining ??
        null,
      rate_limit_limit:
        backpressure_skip.podio_backpressure?.observation?.rate_limit_limit ??
        null,
      podio_path:
        backpressure_skip.podio_backpressure?.observation?.path ?? null,
      podio_operation:
        backpressure_skip.podio_backpressure?.observation?.operation ?? null,
    });

    return backpressure_skip;
  }

  return with_run_lock({
    scope: scoped_master_owner_id
      ? `queue-retry:${scoped_master_owner_id}`
      : "queue-retry",
    lease_ms: 10 * 60_000,
    owner: "retry_runner",
    metadata: {
      limit,
      master_owner_id: scoped_master_owner_id,
    },
    onLocked: async (lock) => {
      await record_system_alert({
        subsystem: "retries",
        code: "runner_overlap",
        severity: "warning",
        retryable: true,
        summary: "Retry runner skipped because an active lease is already in progress.",
        dedupe_key: scoped_master_owner_id
          ? `retry:${scoped_master_owner_id}`
          : "retry",
        metadata: {
          limit,
          master_owner_id: scoped_master_owner_id,
          lock,
        },
      });

      return {
        ok: true,
        skipped: true,
        reason: "retry_runner_lock_active",
        processed_count: 0,
        retried_count: 0,
        scheduled_count: 0,
        terminal_count: 0,
        skipped_count: 0,
        results: [],
        lock,
        master_owner_id: scoped_master_owner_id,
      };
    },
    fn: async () => {
      info("queue.retry_runner_started", {
        limit,
        master_owner_id: scoped_master_owner_id,
      });

      try {
        const result = await retry_send_queue({
          limit,
          master_owner_id: scoped_master_owner_id,
        });

        if ((result?.skipped_count || 0) > 0) {
          await record_system_alert({
            subsystem: "retries",
            code: "runner_skipped_items",
            severity: "warning",
            retryable: true,
            summary: `Retry runner skipped ${result?.skipped_count || 0} item(s).`,
            dedupe_key: scoped_master_owner_id
              ? `retry:${scoped_master_owner_id}`
              : "retry",
            metadata: {
              ...result,
            },
          });
        } else {
          await resolve_system_alert({
            subsystem: "retries",
            code: "runner_skipped_items",
            dedupe_key: scoped_master_owner_id
              ? `retry:${scoped_master_owner_id}`
              : "retry",
            resolution_message: "Retry runner completed without skipped items.",
          });
        }

        info("queue.retry_runner_completed", {
          limit,
          processed_count: result?.processed_count || 0,
          retried_count: result?.retried_count || 0,
          scheduled_count: result?.scheduled_count || 0,
          terminal_count: result?.terminal_count || 0,
          skipped_count: result?.skipped_count || 0,
          master_owner_id: scoped_master_owner_id,
        });

        return {
          ok: true,
          ...result,
        };
      } catch (err) {
        if (isPodioRateLimitError(err)) {
          warn("queue.retry_runner_rate_limit_abort", {
            limit,
            master_owner_id: scoped_master_owner_id,
            message: err?.message || "Podio cooldown active",
          });

          return build_cooldown_skip_result({
            processed_count: 0,
            retried_count: 0,
            scheduled_count: 0,
            terminal_count: 0,
            skipped_count: 0,
            scanned_count: 0,
            results: [],
            master_owner_id: scoped_master_owner_id,
          });
        }

        warn("queue.retry_runner_failed", {
          limit,
          message: err?.message || "Unknown retry runner error",
          master_owner_id: scoped_master_owner_id,
        });

        await record_system_alert({
          subsystem: "retries",
          code: "runner_failed",
          severity: "high",
          retryable: true,
          summary: `Retry runner failed: ${err?.message || "Unknown retry runner error"}`,
          dedupe_key: scoped_master_owner_id
            ? `retry:${scoped_master_owner_id}`
            : "retry",
          metadata: {
            limit,
            master_owner_id: scoped_master_owner_id,
          },
        });

        return {
          ok: false,
          processed_count: 0,
          retried_count: 0,
          scheduled_count: 0,
          terminal_count: 0,
          skipped_count: 0,
          results: [],
          reason: err?.message || "retry_runner_failed",
          master_owner_id: scoped_master_owner_id,
        };
      }
    },
  });
}

export default runRetryRunner;
