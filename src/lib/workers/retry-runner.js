// ─── retry-runner.js ─────────────────────────────────────────────────────
import { retrySendQueue } from "@/lib/domain/queue/retry-send-queue.js";
import { recordSystemAlert, resolveSystemAlert } from "@/lib/domain/alerts/system-alerts.js";
import { withRunLock } from "@/lib/domain/runs/run-locks.js";
import { info, warn } from "@/lib/logging/logger.js";

const DEFAULT_RETRY_LIMIT = 50;

export async function runRetryRunner({
  limit = DEFAULT_RETRY_LIMIT,
  master_owner_id = null,
} = {}) {
  const scoped_master_owner_id = Number(master_owner_id || 0) || null;

  return withRunLock({
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
      await recordSystemAlert({
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
        const result = await retrySendQueue({
          limit,
          master_owner_id: scoped_master_owner_id,
        });

        if ((result?.skipped_count || 0) > 0) {
          await recordSystemAlert({
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
          await resolveSystemAlert({
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
        warn("queue.retry_runner_failed", {
          limit,
          message: err?.message || "Unknown retry runner error",
          master_owner_id: scoped_master_owner_id,
        });

        await recordSystemAlert({
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
