"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

import styles from "@/app/dashboard/ops/ops-dashboard.module.css";

const DEFAULT_FILTERS = {
  source_view_id: "",
  priority_tier: "",
  file: "",
  market: "",
  event_type: "",
  time_range: "24h",
};

const FAST_POLL_MS = 8_000;
const FEEDER_POLL_MS = 60_000;
const ALERT_SILENCE_MINUTES = 240;

const US_OUTLINE_PATH =
  "M79 257L96 232L116 219L133 196L152 181L177 163L209 147L249 140L285 144L319 140L350 148L387 149L423 160L454 178L484 194L511 211L539 235L563 252L585 275L579 292L558 297L542 314L515 324L490 339L461 342L432 331L406 335L378 349L346 346L315 338L283 340L251 349L220 347L192 339L162 336L136 319L116 301L99 289Z";

function clean(value) {
  return String(value ?? "").trim();
}

function buildQuery(filters = {}) {
  const params = new URLSearchParams();

  for (const [key, value] of Object.entries(filters)) {
    if (clean(value)) params.set(key, clean(value));
  }

  return params.toString();
}

async function fetchJson(path, filters = {}) {
  const query = buildQuery(filters);
  const response = await fetch(query ? `${path}?${query}` : path, {
    cache: "no-store",
  });

  const payload = await response.json().catch(() => ({}));

  if (!response.ok || payload?.ok === false) {
    throw new Error(payload?.message || payload?.error || `Request failed for ${path}`);
  }

  return payload?.data ?? payload?.result ?? payload;
}

async function postJson(path, body = {}) {
  const response = await fetch(path, {
    method: "POST",
    cache: "no-store",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  const payload = await response.json().catch(() => ({}));

  if (!response.ok || payload?.ok === false) {
    throw new Error(payload?.message || payload?.error || `Request failed for ${path}`);
  }

  return payload?.data ?? payload?.result ?? payload;
}

function pickDefaultView(views = []) {
  return views.find((view) => /sms/i.test(view?.name || "")) || views[0] || null;
}

function formatRelativeTime(value) {
  if (!value) return "No timestamp";

  const ts = new Date(value).getTime();
  if (Number.isNaN(ts)) return "Unknown time";

  const delta_ms = ts - Date.now();
  const delta_minutes = Math.round(delta_ms / (60 * 1000));
  const formatter = new Intl.RelativeTimeFormat("en", { numeric: "auto" });

  if (Math.abs(delta_minutes) < 60) {
    return formatter.format(delta_minutes, "minute");
  }

  const delta_hours = Math.round(delta_minutes / 60);
  if (Math.abs(delta_hours) < 48) {
    return formatter.format(delta_hours, "hour");
  }

  const delta_days = Math.round(delta_hours / 24);
  return formatter.format(delta_days, "day");
}

function formatTimestamp(value) {
  if (!value) return "Unknown";

  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(value));
}

function projectLower48Point(lat, lng) {
  const min_lng = -125;
  const max_lng = -66.5;
  const min_lat = 24;
  const max_lat = 49.5;

  const left = ((lng - min_lng) / (max_lng - min_lng)) * 100;
  const top = ((max_lat - lat) / (max_lat - min_lat)) * 100;

  return {
    left: Math.min(98, Math.max(2, left)),
    top: Math.min(92, Math.max(6, top)),
  };
}

function LoadingBlock({ label }) {
  return <div className={styles.loadingBlock}>{label}</div>;
}

function EmptyState({ title, detail }) {
  return (
    <div className={styles.emptyState}>
      <div className={styles.emptyTitle}>{title}</div>
      <div className={styles.emptyDetail}>{detail}</div>
    </div>
  );
}

function createPanelErrors() {
  return {
    filters: "",
    kpis: "",
    feed: "",
    map: "",
    queue: "",
    feeder: "",
  };
}

function countPanelErrors(errors = {}) {
  return Object.values(errors).filter(Boolean).length;
}

function buildBannerMessage(errors = {}, pollingPaused = false) {
  const errorCount = countPanelErrors(errors);
  if (!errorCount) {
    return pollingPaused ? "Live polling paused while this tab is hidden." : "";
  }

  if (errorCount === 1) {
    return Object.values(errors).find(Boolean) || "";
  }

  return `${errorCount} dashboard panels are degraded. Last good data is still shown where available.`;
}

function PanelAlert({ message, muted = false }) {
  if (!clean(message)) return null;

  return (
    <div className={muted ? styles.panelAlertMuted : styles.panelAlert}>
      {message}
    </div>
  );
}

export default function OpsDashboardClient() {
  const [filters, setFilters] = useState(DEFAULT_FILTERS);
  const [filterOptions, setFilterOptions] = useState(null);
  const [pollingPaused, setPollingPaused] = useState(false);
  const [refreshToken, setRefreshToken] = useState(0);
  const [alertControlState, setAlertControlState] = useState({});
  const [state, setState] = useState({
    loading: true,
    errors: createPanelErrors(),
    last_updated_at: null,
    kpis: null,
    feed: null,
    map: null,
    queue: null,
    feeder: null,
  });

  useEffect(() => {
    if (typeof document === "undefined") return undefined;

    const syncVisibility = () => {
      setPollingPaused(document.visibilityState === "hidden");
    };

    syncVisibility();
    document.addEventListener("visibilitychange", syncVisibility);

    return () => {
      document.removeEventListener("visibilitychange", syncVisibility);
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    fetchJson("/api/internal/dashboard/ops/filters")
      .then((data) => {
        if (cancelled) return;
        setFilterOptions(data);
        setState((current) => ({
          ...current,
          errors: {
            ...current.errors,
            filters: "",
          },
        }));
      })
      .catch((error) => {
        if (cancelled) return;
        setState((current) => ({
          ...current,
          errors: {
            ...current.errors,
            filters: error?.message || "Failed to load dashboard filters",
          },
        }));
      });

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!filterOptions?.views?.length) return;
    if (clean(filters.source_view_id)) return;

    const default_view = pickDefaultView(filterOptions.views);
    if (!default_view?.view_id) return;

    setFilters((current) => ({
      ...current,
      source_view_id: String(default_view.view_id),
    }));
  }, [filterOptions, filters.source_view_id]);

  useEffect(() => {
    let cancelled = false;

    async function loadFast(is_background = false) {
      if (!is_background) {
        setState((current) => ({
          ...current,
          loading: true,
        }));
      }

      const settled = await Promise.allSettled([
        fetchJson("/api/internal/dashboard/ops/kpis", filters),
        fetchJson("/api/internal/dashboard/ops/feed", filters),
        fetchJson("/api/internal/dashboard/ops/map", filters),
        fetchJson("/api/internal/dashboard/ops/queue", filters),
      ]);

      if (cancelled) return;

      setState((current) => ({
        ...current,
        loading: false,
        last_updated_at:
          settled.some((entry) => entry.status === "fulfilled")
            ? new Date().toISOString()
            : current.last_updated_at,
        errors: {
          ...current.errors,
          kpis:
            settled[0].status === "rejected"
              ? settled[0].reason?.message || "KPI refresh failed"
              : "",
          feed:
            settled[1].status === "rejected"
              ? settled[1].reason?.message || "Feed refresh failed"
              : "",
          map:
            settled[2].status === "rejected"
              ? settled[2].reason?.message || "Map refresh failed"
              : "",
          queue:
            settled[3].status === "rejected"
              ? settled[3].reason?.message || "Queue refresh failed"
              : "",
        },
        kpis:
          settled[0].status === "fulfilled" ? settled[0].value : current.kpis,
        feed:
          settled[1].status === "fulfilled" ? settled[1].value : current.feed,
        map:
          settled[2].status === "fulfilled" ? settled[2].value : current.map,
        queue:
          settled[3].status === "fulfilled" ? settled[3].value : current.queue,
      }));
    }

    loadFast(false);
    const fast_timer = pollingPaused
      ? null
      : window.setInterval(() => {
          loadFast(true);
        }, FAST_POLL_MS);

    return () => {
      cancelled = true;
      if (fast_timer) window.clearInterval(fast_timer);
    };
  }, [
    filters.priority_tier,
    filters.file,
    filters.market,
    filters.event_type,
    filters.time_range,
    pollingPaused,
    refreshToken,
  ]);

  useEffect(() => {
    let cancelled = false;

    async function loadFeeder() {
      try {
        const feeder = await fetchJson("/api/internal/dashboard/ops/feeder", {
          source_view_id: filters.source_view_id,
        });
        if (cancelled) return;

        setState((current) => ({
          ...current,
          feeder,
          errors: {
            ...current.errors,
            feeder: "",
          },
        }));
      } catch (error) {
        if (cancelled) return;

        setState((current) => ({
          ...current,
          errors: {
            ...current.errors,
            feeder: error?.message || "Feeder refresh failed",
          },
        }));
      }
    }

    loadFeeder();
    const feeder_timer = pollingPaused
      ? null
      : window.setInterval(() => {
          loadFeeder();
        }, FEEDER_POLL_MS);

    return () => {
      cancelled = true;
      if (feeder_timer) window.clearInterval(feeder_timer);
    };
  }, [filters.source_view_id, pollingPaused]);

  const kpis = state.kpis?.kpis || [];
  const flow = state.kpis?.flow || {};
  const health = state.kpis?.health || null;
  const attention = health?.attention || null;
  const healthNotes = [...(health?.notes || []), ...(health?.partials || [])];
  const events = state.feed?.events || [];
  const points = state.map?.points || [];
  const queueSummary = state.queue || null;
  const feeder = state.feeder || null;
  const buyerDispo = state.kpis?.buyer_dispo || null;
  const bannerMessage = buildBannerMessage(state.errors, pollingPaused);
  const panelDegradeCount = countPanelErrors(state.errors);
  const timeRanges =
    filterOptions?.time_ranges?.length
      ? filterOptions.time_ranges
      : [
          { id: "1h", label: "Last Hour" },
          { id: "6h", label: "Last 6 Hours" },
          { id: "24h", label: "Last 24 Hours" },
          { id: "7d", label: "Last 7 Days" },
        ];

  async function handleAlertControl(alert, action) {
    const alertKey = String(alert?.item_id || alert?.id || "");
    if (!alertKey) return;

    setAlertControlState((current) => ({
      ...current,
      [alertKey]: {
        loading: true,
        error: "",
      },
    }));

    try {
      await postJson("/api/internal/alerts/control", {
        action,
        alert_item_id: alert?.item_id || null,
        actor: "ops_dashboard",
        note:
          action === "acknowledge"
            ? "Acknowledged from ops dashboard"
            : action === "silence"
              ? `Silenced for ${ALERT_SILENCE_MINUTES} minutes from ops dashboard`
              : "Unsilenced from ops dashboard",
        silence_for_minutes:
          action === "silence" ? ALERT_SILENCE_MINUTES : undefined,
      });

      setAlertControlState((current) => ({
        ...current,
        [alertKey]: {
          loading: false,
          error: "",
        },
      }));
      setRefreshToken((current) => current + 1);
    } catch (error) {
      setAlertControlState((current) => ({
        ...current,
        [alertKey]: {
          loading: false,
          error: error?.message || "Alert control failed",
        },
      }));
    }
  }

  return (
    <main className={styles.page}>
      <div className={styles.backdropGrid} />

      <section className={styles.hero}>
        <div className={styles.heroMeta}>
          <div className={styles.eyebrow}>Operations Command</div>
          <h1 className={styles.title}>Autonomous Real Estate Engine</h1>
          <p className={styles.subtitle}>
            Polling-driven war room for feeder velocity, queue execution, inbound replies,
            and downstream acquisition flow.
          </p>
        </div>

        <div className={styles.heroStatus}>
          <div className={styles.liveBadge}>
            <span className={styles.liveDot} />
            {pollingPaused ? "Polling Paused" : "Live Polling"}
          </div>
          <div className={styles.lastUpdated}>
            {state.last_updated_at
              ? `Updated ${formatRelativeTime(state.last_updated_at)}`
              : "Waiting for first snapshot"}
          </div>
          <div className={styles.rangePill}>
            {state.kpis?.filters?.time_range_label || "Last 24 Hours"}
          </div>
        </div>
      </section>

      <section className={styles.statusStrip}>
        <div className={styles.statusChip}>
          {pollingPaused ? "Visibility paused" : "Hot polling online"}
        </div>
        <div className={styles.statusChip}>
          {panelDegradeCount ? `${panelDegradeCount} degraded panels` : "All panels healthy"}
        </div>
        <div className={styles.statusChip}>
          Buyer/Dispo feed {buyerDispo?.recent_events?.length ? "active" : "idle"}
        </div>
        <div className={styles.statusChip}>
          {attention?.needs_attention
            ? `${attention.open_alerts_count} alerts need attention`
            : "No active system alerts"}
        </div>
      </section>

      <section className={styles.filtersPanel}>
        <div className={styles.filtersHeader}>
          <div>
            <div className={styles.panelEyebrow}>Controls</div>
            <h2 className={styles.panelTitle}>Operational Filters</h2>
          </div>
          <button
            className={styles.resetButton}
            type="button"
            onClick={() => setFilters(DEFAULT_FILTERS)}
          >
            Reset
          </button>
        </div>

        <PanelAlert message={state.errors.filters} muted={Boolean(filterOptions)} />

        <div className={styles.filtersGrid}>
          <label className={styles.control}>
            <span>Priority Tier</span>
            <select
              value={filters.priority_tier}
              onChange={(event) =>
                setFilters((current) => ({
                  ...current,
                  priority_tier: event.target.value,
                }))
              }
            >
              <option value="">All Tiers</option>
              {(filterOptions?.priority_tiers || []).map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>

          <label className={styles.control}>
            <span>File</span>
            <select
              value={filters.file}
              onChange={(event) =>
                setFilters((current) => ({
                  ...current,
                  file: event.target.value,
                }))
              }
            >
              <option value="">All Files</option>
              {(filterOptions?.files || []).map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>

          <label className={styles.control}>
            <span>Market</span>
            <input
              list="dashboard-market-suggestions"
              value={filters.market}
              onChange={(event) =>
                setFilters((current) => ({
                  ...current,
                  market: event.target.value,
                }))
              }
              placeholder="Chicago, IL"
            />
            <datalist id="dashboard-market-suggestions">
              {(filterOptions?.market_suggestions || []).map((option) => (
                <option key={option} value={option} />
              ))}
            </datalist>
          </label>

          <label className={styles.control}>
            <span>Event Type</span>
            <select
              value={filters.event_type}
              onChange={(event) =>
                setFilters((current) => ({
                  ...current,
                  event_type: event.target.value,
                }))
              }
            >
              <option value="">All Events</option>
              {(filterOptions?.event_types || []).map((option) => (
                <option key={option.id} value={option.id}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label className={styles.control}>
            <span>Time Range</span>
            <select
              value={filters.time_range}
              onChange={(event) =>
                setFilters((current) => ({
                  ...current,
                  time_range: event.target.value,
                }))
              }
            >
              {timeRanges.map((option) => (
                <option key={option.id} value={option.id}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
        </div>
      </section>

      {bannerMessage ? <div className={styles.errorBanner}>{bannerMessage}</div> : null}

      <section className={styles.kpiGrid}>
        <PanelAlert message={state.errors.kpis} muted={Boolean(kpis.length)} />
        {kpis.length
          ? kpis.map((card) => (
              <article key={card.id} className={styles.kpiCard}>
                <div className={styles.kpiLabel}>{card.label}</div>
                <div className={styles.kpiValue}>{card.display_value}</div>
                <div className={styles.kpiScope}>
                  {card.scope === "global" ? "Global" : "Recent sample"}
                </div>
              </article>
            ))
          : Array.from({ length: 5 }).map((_, index) => (
              <LoadingBlock key={`kpi-${index}`} label="Loading KPI" />
            ))}
      </section>

      <section className={styles.primaryGrid}>
        <article className={`${styles.panel} ${styles.mapPanel}`}>
          <div className={styles.panelHeader}>
            <div>
              <div className={styles.panelEyebrow}>Nationwide Activity</div>
              <h2 className={styles.panelTitle}>Pulse Map</h2>
            </div>
            <div className={styles.panelMetric}>
              {state.map?.marker_count ?? 0} plotted signals
            </div>
          </div>

          <PanelAlert message={state.errors.map} muted={Boolean(points.length)} />

          <div className={styles.mapFrame}>
            <svg
              viewBox="0 0 660 390"
              className={styles.mapOutline}
              aria-hidden="true"
            >
              <defs>
                <linearGradient id="opsMapStroke" x1="0%" y1="0%" x2="100%" y2="100%">
                  <stop offset="0%" stopColor="rgba(125, 211, 252, 0.72)" />
                  <stop offset="100%" stopColor="rgba(250, 204, 21, 0.18)" />
                </linearGradient>
              </defs>
              <path d={US_OUTLINE_PATH} />
            </svg>

            <div className={styles.mapSweep} />

            {points.length ? (
              points.map((point) => {
                const projected = projectLower48Point(point.lat, point.lng);
                const size = Math.min(28, 10 + point.count * 2.5);

                return (
                  <div
                    key={point.id}
                    className={styles.mapMarker}
                    title={`${point.label} • ${point.market_name || "Unmapped"} • ${point.count}`}
                    style={{
                      left: `${projected.left}%`,
                      top: `${projected.top}%`,
                      width: `${size}px`,
                      height: `${size}px`,
                      "--marker-color": point.meta?.color || "#38bdf8",
                      "--marker-glow": point.meta?.glow || "rgba(56, 189, 248, 0.24)",
                    }}
                  >
                    <span className={styles.mapMarkerCore} />
                    <span className={styles.mapMarkerPulse} />
                    <span className={styles.mapMarkerCount}>{point.count}</span>
                  </div>
                );
              })
            ) : (
              <EmptyState
                title="No plottable geo activity"
                detail="This slice has no recent property coordinates or market centroid fallbacks."
              />
            )}
          </div>

          <div className={styles.legend}>
            {(filterOptions?.event_types || []).slice(0, 8).map((option) => {
              const event = points.find((entry) => entry.event_type === option.id);
              const color = event?.meta?.color || "#64748b";

              return (
                <div key={option.id} className={styles.legendItem}>
                  <span
                    className={styles.legendSwatch}
                    style={{ background: color }}
                  />
                  {option.label}
                </div>
              );
            })}
          </div>
        </article>

        <article className={`${styles.panel} ${styles.feedPanel}`}>
          <div className={styles.panelHeader}>
            <div>
              <div className={styles.panelEyebrow}>Timeline</div>
              <h2 className={styles.panelTitle}>Operational Feed</h2>
            </div>
            <div className={styles.panelMetric}>
              {state.feed?.total_matching_events ?? 0} matching events
            </div>
          </div>

          <PanelAlert message={state.errors.feed} muted={Boolean(events.length)} />

          <div className={styles.feedScroller}>
            {events.length ? (
              events.map((event) => (
                <article key={event.id} className={styles.feedItem}>
                  <div
                    className={styles.feedDot}
                    style={{ background: event.meta?.color || "#38bdf8" }}
                  />
                  <div className={styles.feedBody}>
                    <div className={styles.feedTopline}>
                      <span
                        className={styles.eventTypeTag}
                        style={{
                          color: event.meta?.color || "#38bdf8",
                          borderColor: event.meta?.color || "#38bdf8",
                        }}
                      >
                        {event.meta?.short_label || event.event_type}
                      </span>
                      <span className={styles.feedTime}>
                        {formatRelativeTime(event.timestamp)}
                      </span>
                    </div>
                    <div className={styles.feedTitle}>{event.title}</div>
                    <div className={styles.feedMeta}>
                      {[event.owner_name, event.property_address, event.market_name]
                        .filter(Boolean)
                        .slice(0, 3)
                        .join(" • ") || "No linked owner/property context"}
                    </div>
                    {event.detail ? (
                      <div className={styles.feedDetail}>{event.detail}</div>
                    ) : null}
                  </div>
                </article>
              ))
            ) : state.loading ? (
              <LoadingBlock label="Loading feed" />
            ) : (
              <EmptyState
                title="No recent events"
                detail="Widen the time range or clear filters to inspect broader activity."
              />
            )}
          </div>
        </article>
      </section>

      <section className={styles.secondaryGrid}>
        <article className={styles.panel}>
          <div className={styles.panelHeader}>
            <div>
              <div className={styles.panelEyebrow}>Feeder</div>
              <h2 className={styles.panelTitle}>Master Owner Visibility</h2>
            </div>
            <div className={styles.panelMetric}>
              {feeder?.source_view?.name || "No view selected"}
            </div>
          </div>

          <PanelAlert message={state.errors.feeder} muted={Boolean(feeder)} />

          {feeder ? (
            <>
              <label className={styles.control}>
                <span>Feeder View</span>
                <select
                  value={filters.source_view_id}
                  onChange={(event) =>
                    setFilters((current) => ({
                      ...current,
                      source_view_id: event.target.value,
                    }))
                  }
                >
                  <option value="">Auto-select</option>
                  {(filterOptions?.views || []).map((view) => (
                    <option key={view.view_id || view.name} value={view.view_id || ""}>
                      {view.name || view.view_id}
                    </option>
                  ))}
                </select>
              </label>

              <div className={styles.statsRow}>
                <div className={styles.statTile}>
                  <span>Raw Pulled</span>
                  <strong>{feeder.raw_items_pulled ?? 0}</strong>
                </div>
                <div className={styles.statTile}>
                  <span>Eligible</span>
                  <strong>{feeder.eligible_owner_count ?? 0}</strong>
                </div>
                <div className={styles.statTile}>
                  <span>Queued</span>
                  <strong>{feeder.queued_count ?? 0}</strong>
                </div>
              </div>

              <div className={styles.stackTitle}>Skip Reasons</div>
              <div className={styles.reasonList}>
                {(feeder.skip_reason_counts || []).slice(0, 6).map((entry) => {
                  const width = Math.max(
                    8,
                    (entry.count /
                      Math.max(
                        1,
                        ...(feeder.skip_reason_counts || []).map((item) => item.count || 0)
                      )) *
                      100
                  );

                  return (
                    <div key={entry.reason} className={styles.reasonItem}>
                      <div className={styles.reasonMeta}>
                        <span>{entry.reason}</span>
                        <strong>{entry.count}</strong>
                      </div>
                      <div className={styles.reasonBarTrack}>
                        <div
                          className={styles.reasonBarFill}
                          style={{ width: `${width}%` }}
                        />
                      </div>
                    </div>
                  );
                })}
              </div>

              {feeder.deferred_resolution ? (
                <div className={styles.noticeBox}>
                  This panel is feeder-view scoped only. Template resolution and brain
                  suppression stay deferred to queue/send time.
                </div>
              ) : null}

              {feeder.ok === false ? (
                <div className={styles.noticeBox}>
                  Feeder snapshot unavailable: {feeder.reason || "unknown_feeder_state"}
                </div>
              ) : null}
            </>
          ) : (
            <LoadingBlock label="Loading feeder" />
          )}
        </article>

        <article className={styles.panel}>
          <div className={styles.panelHeader}>
            <div>
              <div className={styles.panelEyebrow}>Queue</div>
              <h2 className={styles.panelTitle}>Execution Health</h2>
            </div>
            <div className={styles.panelMetric}>
              {queueSummary?.queue_sample_size ?? 0} sampled rows
            </div>
          </div>

          <PanelAlert message={state.errors.queue} muted={Boolean(queueSummary)} />

          {queueSummary ? (
            <>
              <div className={styles.reasonList}>
                {(queueSummary.status_breakdown || []).map((entry) => (
                  <div key={entry.status} className={styles.reasonItem}>
                    <div className={styles.reasonMeta}>
                      <span>{entry.status}</span>
                      <strong>{entry.count}</strong>
                    </div>
                    <div className={styles.reasonBarTrack}>
                      <div
                        className={styles.reasonBarFill}
                        style={{
                          width: `${Math.min(100, Math.max(8, entry.count))}%`,
                          background:
                            entry.meta?.color || "linear-gradient(90deg, #38bdf8, #22d3ee)",
                        }}
                      />
                    </div>
                  </div>
                ))}
              </div>

              <div className={styles.stackTitle}>Recent Failures</div>
              <div className={styles.compactList}>
                {(queueSummary.recent_failures || []).length ? (
                  queueSummary.recent_failures.map((failure) => (
                    <div key={failure.id} className={styles.compactItem}>
                      <div>
                        <strong>{failure.title}</strong>
                        <div className={styles.compactMeta}>
                          {failure.market_name || "Unknown market"}
                        </div>
                      </div>
                      <div className={styles.compactTime}>
                        {formatTimestamp(failure.timestamp)}
                      </div>
                    </div>
                  ))
                ) : (
                  <EmptyState
                    title="No recent failures"
                    detail="The current sample does not contain queue or carrier failures."
                  />
                )}
              </div>
            </>
          ) : (
            <LoadingBlock label="Loading queue summary" />
          )}
        </article>
      </section>

      <section className={styles.tertiaryGrid}>
        <article className={styles.panel}>
          <div className={styles.panelHeader}>
            <div>
              <div className={styles.panelEyebrow}>Deal Flow</div>
              <h2 className={styles.panelTitle}>Offers to Revenue</h2>
            </div>
          </div>

          <div className={styles.noticeBox}>
            Recent sampled status mix only. These are not full app totals.
          </div>

          <div className={styles.flowGrid}>
            {Object.entries(flow).map(([key, section]) => (
              <div key={key} className={styles.flowCard}>
                <div className={styles.flowHeader}>
                  <span>{section.label}</span>
                  <strong>{section.total_recent}</strong>
                </div>
                <div className={styles.flowStatuses}>
                  {(section.statuses || []).map((status) => (
                    <div key={`${key}-${status.label}`} className={styles.flowStatusRow}>
                      <span>{status.label}</span>
                      <strong>{status.count}</strong>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </article>

        <article className={styles.panel}>
          <div className={styles.panelHeader}>
            <div>
              <div className={styles.panelEyebrow}>Disposition</div>
              <h2 className={styles.panelTitle}>Buyer Response Matrix</h2>
            </div>
            <div className={styles.panelMetric}>
              {buyerDispo?.threads_total ?? 0} active buyer threads
            </div>
          </div>

          <PanelAlert
            message={panelDegradeCount && !buyerDispo ? "Buyer/dispo snapshot waiting on KPI refresh." : ""}
            muted={Boolean(buyerDispo)}
          />

          {buyerDispo ? (
            <>
              <div className={styles.buyerMatrix}>
                <div className={styles.flowCard}>
                  <div className={styles.flowHeader}>
                    <span>Match Status</span>
                    <strong>{buyerDispo.total_recent}</strong>
                  </div>
                  <div className={styles.flowStatuses}>
                    {(buyerDispo.statuses || []).map((status) => (
                      <div key={`buyer-status-${status.label}`} className={styles.flowStatusRow}>
                        <span>{status.label}</span>
                        <strong>{status.count}</strong>
                      </div>
                    ))}
                  </div>
                </div>

                <div className={styles.flowCard}>
                  <div className={styles.flowHeader}>
                    <span>Response Status</span>
                    <strong>{(buyerDispo.response_statuses || []).length}</strong>
                  </div>
                  <div className={styles.flowStatuses}>
                    {(buyerDispo.response_statuses || []).map((status) => (
                      <div key={`buyer-response-${status.label}`} className={styles.flowStatusRow}>
                        <span>{status.label}</span>
                        <strong>{status.count}</strong>
                      </div>
                    ))}
                  </div>
                </div>

                <div className={styles.flowCard}>
                  <div className={styles.flowHeader}>
                    <span>Thread State</span>
                    <strong>{buyerDispo.threads_total || 0}</strong>
                  </div>
                  <div className={styles.flowStatuses}>
                    {(buyerDispo.thread_states || []).map((status) => (
                      <div key={`buyer-thread-${status.label}`} className={styles.flowStatusRow}>
                        <span>{status.label}</span>
                        <strong>{status.count}</strong>
                      </div>
                    ))}
                  </div>
                </div>
              </div>

              <div className={styles.stackTitle}>Active Buyer Threads</div>
              <div className={styles.compactList}>
                {(buyerDispo.recent_threads || []).length ? (
                  buyerDispo.recent_threads.map((thread) => (
                    <div key={thread.id} className={styles.compactItem}>
                      <div>
                        <strong>{thread.company_name}</strong>
                        <div className={styles.compactMeta}>
                          {[
                            thread.current_state,
                            thread.last_channel ? `${thread.last_channel.toUpperCase()} thread` : "",
                            thread.primary_email || thread.primary_phone || "",
                          ]
                            .filter(Boolean)
                            .join(" • ") || "Buyer thread"}
                        </div>
                      </div>
                      <div className={styles.compactTime}>
                        {formatTimestamp(thread.last_contact_at)}
                      </div>
                    </div>
                  ))
                ) : (
                  <EmptyState
                    title="No buyer threads yet"
                    detail="Partner threads appear here once a buyer package goes out or a response comes back in."
                  />
                )}
              </div>

              <div className={styles.stackTitle}>Recent Buyer Signals</div>
              <div className={styles.compactList}>
                {(buyerDispo.recent_events || []).length ? (
                  buyerDispo.recent_events.map((event) => (
                    <div key={event.id} className={styles.compactItem}>
                      <div>
                        <strong>{event.title}</strong>
                        <div className={styles.compactMeta}>
                          {[event.detail, event.market_name, event.property_address]
                            .filter(Boolean)
                            .slice(0, 3)
                            .join(" • ") || "Buyer/dispo event"}
                        </div>
                      </div>
                      <div className={styles.compactTime}>
                        {formatTimestamp(event.timestamp)}
                      </div>
                    </div>
                  ))
                ) : (
                  <EmptyState
                    title="No buyer responses yet"
                    detail="Recent buyer blast and response events will surface here as the dispo loop runs."
                  />
                )}
              </div>
            </>
          ) : (
            <LoadingBlock label="Loading buyer disposition" />
          )}
        </article>

        <article className={styles.panel}>
          <div className={styles.panelHeader}>
            <div>
              <div className={styles.panelEyebrow}>System Health</div>
              <h2 className={styles.panelTitle}>Runtime Notes + Attention</h2>
            </div>
          </div>

          <PanelAlert message={!health ? state.errors.kpis : ""} muted={Boolean(health)} />

          {health ? (
            <>
              {attention ? (
                <>
                  <div className={styles.attentionGrid}>
                    <div className={styles.attentionCard}>
                      <span>Open Alerts</span>
                      <strong>{attention.open_alerts_count}</strong>
                    </div>
                    <div className={styles.attentionCard}>
                      <span>Critical / High</span>
                      <strong>{attention.critical_count + attention.high_count}</strong>
                    </div>
                    <div className={styles.attentionCard}>
                      <span>Retryable</span>
                      <strong>{attention.retryable_count}</strong>
                    </div>
                    <div className={styles.attentionCard}>
                      <span>Manual Review</span>
                      <strong>{attention.non_retryable_count}</strong>
                    </div>
                    <div className={styles.attentionCard}>
                      <span>Acknowledged</span>
                      <strong>{attention.acknowledged_count}</strong>
                    </div>
                    <div className={styles.attentionCard}>
                      <span>Silenced</span>
                      <strong>{attention.silenced_count}</strong>
                    </div>
                  </div>

                  <div className={styles.stackTitle}>Needs Attention</div>
                  <div className={styles.compactList}>
                    {(attention.recent_alerts || []).length ? (
                      attention.recent_alerts.map((alert) => {
                        const alertKey = String(alert?.item_id || alert?.id || "");
                        const control = alertControlState[alertKey] || {};
                        const operatorState = clean(alert?.operator_state) || "open";

                        return (
                          <div key={alert.id} className={styles.alertItem}>
                            <div className={styles.alertItemTop}>
                              <div className={styles.alertBadgeRow}>
                                <span className={styles.alertSeverity}>{alert.severity}</span>
                                <span className={styles.alertState}>
                                  {operatorState}
                                </span>
                              </div>
                              <span className={styles.compactTime}>
                                {formatTimestamp(alert.last_seen_at)}
                              </span>
                            </div>
                            <strong>{alert.summary}</strong>
                            <div className={styles.compactMeta}>
                              {[alert.subsystem, alert.code, alert.retryable ? "retryable" : "manual review"]
                                .filter(Boolean)
                                .join(" • ")}
                            </div>
                            {alert.affected_ids?.length ? (
                              <div className={styles.alertAffected}>
                                IDs: {alert.affected_ids.slice(0, 4).join(", ")}
                              </div>
                            ) : null}
                            {alert.silenced_until ? (
                              <div className={styles.alertAffected}>
                                Silenced until: {formatTimestamp(alert.silenced_until)}
                              </div>
                            ) : null}
                            <div className={styles.alertActions}>
                              {operatorState !== "acknowledged" ? (
                                <button
                                  className={styles.alertActionButton}
                                  type="button"
                                  onClick={() => handleAlertControl(alert, "acknowledge")}
                                  disabled={Boolean(control.loading)}
                                >
                                  {control.loading ? "Working..." : "Acknowledge"}
                                </button>
                              ) : null}
                              {operatorState !== "silenced" ? (
                                <button
                                  className={styles.alertActionButtonSecondary}
                                  type="button"
                                  onClick={() => handleAlertControl(alert, "silence")}
                                  disabled={Boolean(control.loading)}
                                >
                                  {control.loading ? "Working..." : `Silence ${ALERT_SILENCE_MINUTES / 60}h`}
                                </button>
                              ) : (
                                <button
                                  className={styles.alertActionButtonSecondary}
                                  type="button"
                                  onClick={() => handleAlertControl(alert, "unsilence")}
                                  disabled={Boolean(control.loading)}
                                >
                                  {control.loading ? "Working..." : "Unsilence"}
                                </button>
                              )}
                            </div>
                            {clean(control.error) ? (
                              <div className={styles.alertActionError}>{control.error}</div>
                            ) : null}
                          </div>
                        );
                      })
                    ) : (
                      <EmptyState
                        title="No active alerts"
                        detail="Recent runner, provider, and webhook failures will appear here when they need operator attention."
                      />
                    )}
                  </div>
                </>
              ) : null}

              <div className={styles.healthList}>
                {healthNotes.map((note) => (
                  <div key={note} className={styles.healthItem}>
                    {note}
                  </div>
                ))}
              </div>

              <div className={styles.stackTitle}>Quick Links</div>
              <div className={styles.quickLinks}>
                <Link href="/api/internal/dashboard/ops/queue" className={styles.quickLink}>
                  Queue Snapshot
                </Link>
                <Link href="/api/internal/dashboard/ops/feeder" className={styles.quickLink}>
                  Feeder Snapshot
                </Link>
                <Link
                  href="/api/internal/dashboard/ops/feed?event_type=queue_failure"
                  className={styles.quickLink}
                >
                  Error Feed
                </Link>
                <Link
                  href="/api/internal/dashboard/ops/kpis"
                  className={styles.quickLink}
                >
                  KPI JSON
                </Link>
              </div>

              <div className={styles.healthTimestamps}>
                <div>Snapshot: {formatTimestamp(health.snapshot_generated_at)}</div>
                <div>
                  Latest activity:{" "}
                  {health.latest_activity_at
                    ? formatTimestamp(health.latest_activity_at)
                    : "Not available"}
                </div>
              </div>
            </>
          ) : (
            <LoadingBlock label="Loading health" />
          )}
        </article>
      </section>
    </main>
  );
}
