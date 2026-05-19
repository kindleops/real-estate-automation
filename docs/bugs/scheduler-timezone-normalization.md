# Bug: feeder scheduler `getTodayWindowUtc` does not resolve timezone labels to IANA zones

**File:** `src/lib/domain/outbound/supabase-candidate-feeder.js` — `getTodayWindowUtc` ~line 1447  
**Severity:** Medium — causes 1-hour scheduling error for Eastern contacts  
**Workaround active:** `schedule_start_local: "08:00"` (CDT offset) → must be removed after fix

## Problem

`getTodayWindowUtc` falls back silently to `America/Chicago` when the candidate's
`timezone` field is a friendly label like `"Eastern"`, `"Central"`, `"Mountain"`, or `"Pacific"`.

`Intl.DateTimeFormat` rejects non-IANA strings, so `getLocalDateTimeParts(date, "Eastern")`
returns null and the function falls back to `America/Chicago`.

**Impact:** Eastern contacts scheduled at 9am CDT (14:00 UTC) instead of 9am EDT (13:00 UTC).

## Root cause

```js
// supabase-candidate-feeder.js ~line 1447
function getTodayWindowUtc(timezone) {
  const requested_timezone = clean(timezone) || "America/Chicago";
  const effective_timezone = getLocalDateTimeParts(new Date(now_ms), requested_timezone)
    ? requested_timezone
    : "America/Chicago";  // ← "Eastern" silently lands here
  ...
}
```

## Already fixed correctly in sibling

`evaluateContactWindow` in `src/lib/supabase/sms-engine.js` has a `TIMEZONE_MAP`:

```js
const TIMEZONE_MAP = {
  eastern: "America/New_York",
  et: "America/New_York",
  est: "America/New_York",
  edt: "America/New_York",
  // ...
};
const tz_lower = timezone_raw.toLowerCase();
if (TIMEZONE_MAP[tz_lower]) resolved_timezone = TIMEZONE_MAP[tz_lower];
```

## Fix

Add the same resolution at the top of `getTodayWindowUtc`:

```js
const SCHEDULER_TIMEZONE_MAP = {
  eastern: "America/New_York",
  et: "America/New_York",
  est: "America/New_York",
  edt: "America/New_York",
  central: "America/Chicago",
  ct: "America/Chicago",
  cst: "America/Chicago",
  cdt: "America/Chicago",
  mountain: "America/Denver",
  mt: "America/Denver",
  mst: "America/Denver",
  mdt: "America/Denver",
  pacific: "America/Los_Angeles",
  pt: "America/Los_Angeles",
  pst: "America/Los_Angeles",
  pdt: "America/Los_Angeles",
};

function getTodayWindowUtc(timezone) {
  const raw = clean(timezone) || "America/Chicago";
  const requested_timezone = SCHEDULER_TIMEZONE_MAP[raw.toLowerCase()] || raw;
  const effective_timezone = getLocalDateTimeParts(new Date(now_ms), requested_timezone)
    ? requested_timezone
    : "America/Chicago";
  ...
}
```

## Workaround (active as of 2026-05-19)

Used `schedule_start_local: "08:00"` (CDT) to compensate:  
8am CDT = 13:00 UTC = 9am EDT ✓  
**Remove workaround once fix is deployed.**
