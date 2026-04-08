# Outbound Throughput Ops

## Goal

Keep outbound SMS throughput high enough to support a minimum of 2,000 sends per day without manual babysitting.

## Podio Budget Assumptions

- Podio, not TextGrid, is the main scaling bottleneck.
- The most expensive shared bucket has been `POST /item/app/30541680/filter/`.
- `queue_run` is the highest-priority consumer because it directly turns queued inventory into sends.
- `feed-master-owners`, `queue_retry`, `queue_reconcile`, and `autopilot` should back off first when Podio budget is constrained.

## Recommended Cron Priority

1. `queue_run`
2. `feed-master-owners`
3. `queue_retry`
4. `queue_reconcile`
5. `autopilot`

Current production-safe spacing is reflected in [vercel.json](/Users/ryankindle/real-estate-automation/vercel.json):

- `queue_run`: every minute
- `feed-master-owners`: every 8 minutes, offset from the minute boundary
- `queue_retry`: every 30 minutes
- `queue_reconcile`: hourly
- `autopilot`: hourly, later than reconcile

## Backpressure Behavior

- Hard cooldown:
  Triggered by Podio `420` / `429` with `retry_after_seconds`.
  Shared and persisted through the Podio cooldown state file.
- Soft backpressure:
  Triggered when recent observed Podio remaining budget is low.
  Non-essential jobs should skip before making more expensive filter calls.
- Drain-first rule:
  `queue_run` should keep using hard cooldown only.
  Non-essential jobs should pause first.

## Metrics That Matter

- `queue_run`:
  `claimed_count`, `attempted_count`, `sent_count`, `failed_count`, `blocked_count`, `skipped_count`, `duplicate_locked_count`, `batch_duration_ms`
- Queue inventory:
  `queued_due_now_count`, `queued_future_count`, `sending_count`, `failed_recent_count`
- Feeder health:
  `critical_low_threshold_breached`, `healthy_buffer_threshold_met`, `ideal_buffer_threshold_met`
- Template audit:
  selected template source, resolution source, fallback reason, attachment success
- Delivery health:
  sent vs delivered vs failed callbacks, invalid payload rate, hard-bounce suppression updates

## Operational Pause Order Under Pressure

When Podio remaining budget gets tight:

1. Pause `autopilot`
2. Pause `queue_reconcile`
3. Pause `queue_retry`
4. Slow `feed-master-owners`
5. Preserve `queue_run` unless hard cooldown is active

## Queue Build Philosophy

- Build fully rendered queue rows ahead of send time.
- Persist final message text before send whenever possible.
- Attach a real Podio template relation when the selected template is Podio-backed.
- Treat local templates as emergency fallback only and log them honestly as non-attachable.
