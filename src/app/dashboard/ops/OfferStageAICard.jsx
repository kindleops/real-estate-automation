"use client";

import styles from "@/app/dashboard/ops/ops-dashboard.module.css";

export default function OfferStageAICard({ data, error }) {
  const yesText = "Yes";
  const noText = "No";
  const noneText = "none";
  const notSafeText = "Not Safe";
  const safeText = "Safe to Reveal";
  const natext = "N/A";
  const internalText = "INTERNAL";
  const notForSellersText = "Not for sellers";

  // Diagnostics
  if (data) {
    console.log("[OfferStageAICard]", {
      hasData: true,
      threadId: data.thread_key || "unknown",
      triggerReason: data.trigger_reason || "none",
      safeToReveal: data.safe_to_reveal_offer || false,
      sendMode: data.send_mode || "unknown",
      hasDraftMessage: Boolean(data.draft_message),
      confidenceScore: data.offer_confidence_score || 0,
      assetType: data.asset_type || "unknown",
    });
  }

  if (error) {
    return (
      <article className={`${styles.panel} ${styles.offerStageAI || ""}`}>
        <div className={styles.panelHeader}>
          <div>
            <div className={styles.panelEyebrow}>Offer Stage AI</div>
            <h2 className={styles.panelTitle}>Dry-run Underwriting Result</h2>
          </div>
        </div>
        <div className={styles.alertItem}>
          <strong>Error loading Offer Stage AI</strong>
          <div>{error}</div>
        </div>
      </article>
    );
  }

  if (!data) {
    return (
      <article className={`${styles.panel} ${styles.offerStageAI || ""}`}>
        <div className={styles.panelHeader}>
          <div>
            <div className={styles.panelEyebrow}>Offer Stage AI</div>
            <h2 className={styles.panelTitle}>Dry-run Underwriting Result</h2>
          </div>
        </div>
        <div className={styles.emptyState}>
          <div className={styles.emptyTitle}>No offer-stage AI result yet.</div>
          <div className={styles.emptyDetail}>Appears when seller asks price or reaches offer stage.</div>
        </div>
      </article>
    );
  }

  const confidencePct = Math.round((data.offer_confidence_score || 0) * 100);
  const confidenceColor = data.safe_to_reveal_offer ? "#4ade80" : "#fbbf24";

  return (
    <article className={`${styles.panel} ${styles.offerStageAI || ""}`}>
      <div className={styles.panelHeader}>
        <div>
          <div className={styles.panelEyebrow}>Offer Stage AI</div>
          <h2 className={styles.panelTitle}>Dry-run Underwriting Result</h2>
        </div>
        {data.send_mode ? (
          <div className={styles.panelMetric}>{data.send_mode}</div>
        ) : null}
      </div>

      <div className={styles.offerStageAI}>
        <div className={styles.stackTitle}>Trigger Status</div>
        <div className={styles.compactList}>
          <div className={styles.compactItem}>
            <div>
              <strong>Triggered</strong>
              <div className={styles.compactMeta}>
                {data.triggered ? yesText : noText} • {data.trigger_reason || noneText}
              </div>
            </div>
          </div>

          {data.asset_type ? (
            <div className={styles.compactItem}>
              <div>
                <strong>Asset Type</strong>
                <div className={styles.compactMeta}>{data.asset_type}</div>
              </div>
            </div>
          ) : null}

          <div className={styles.compactItem}>
            <div>
              <strong>Confidence</strong>
              <div className={styles.compactMeta}>
                {confidencePct}% •{" "}
                <span style={{ color: confidenceColor }}>
                  {data.safe_to_reveal_offer ? safeText : notSafeText}
                </span>
              </div>
            </div>
          </div>
        </div>

        <div className={styles.stackTitle}>Offer Numbers (Internal)</div>
        <div className={styles.compactList}>
          {data.recommended_opening_offer ? (
            <div className={styles.compactItem}>
              <div>
                <strong>Opening Offer</strong>
                <div className={styles.compactMeta}>
                  {data.recommended_opening_offer.toLocaleString() || natext}
                </div>
              </div>
            </div>
          ) : null}

          {data.target_contract ? (
            <div className={styles.compactItem}>
              <div>
                <strong>Target Contract</strong>
                <div className={styles.compactMeta}>
                  {data.target_contract.toLocaleString() || natext}
                </div>
              </div>
            </div>
          ) : null}

          {data.walkaway_internal ? (
            <div className={styles.compactItem}>
              <div>
                <strong style={{ color: "#f87171" }}>Walkaway ({internalText})</strong>
                <div className={styles.compactMeta}>
                  {data.walkaway_internal.toLocaleString() || natext} • {notForSellersText}
                </div>
              </div>
            </div>
          ) : null}
        </div>

        {data.missing_required_info?.length > 0 ? (
          <>
            <div className={styles.stackTitle}>Missing Info</div>
            <div className={styles.reasonList}>
              {data.missing_required_info.map((item) => (
                <div key={item} className={styles.reasonItem}>
                  <span>{item}</span>
                </div>
              ))}
            </div>
          </>
        ) : null}

        {data.blocked_reason ? (
          <>
            <div className={styles.stackTitle}>Blocked Reason</div>
            <div className={styles.alertItem}>
              <strong>{data.blocked_reason}</strong>
            </div>
          </>
        ) : null}

        {data.draft_message ? (
          <>
            <div className={styles.stackTitle}>Draft Message</div>
            <div
              style={{
                padding: "14px",
                borderRadius: "12px",
                background: "rgba(2, 6, 12, 0.78)",
                border: "1px solid rgba(34, 211, 238, 0.18)",
                fontFamily: "monospace",
                fontSize: "0.84rem",
                lineHeight: "1.6",
                color: "rgba(226, 232, 240, 0.88)",
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
                marginBottom: "8px",
              }}
            >
              {data.draft_message}
            </div>
            <button
              type="button"
              style={{
                padding: "8px 16px",
                borderRadius: "999px",
                background: "rgba(15, 23, 42, 0.7)",
                border: "1px solid rgba(34, 211, 238, 0.2)",
                color: "#e2f8fb",
                fontSize: "0.84rem",
                cursor: "pointer",
                marginTop: "8px",
              }}
              onClick={() => navigator.clipboard?.writeText(data.draft_message || "")}
            >
              Copy Draft
            </button>
          </>
        ) : null}

        <div className={styles.stackTitle}>Routing</div>
        <div className={styles.compactList}>
          <div className={styles.compactItem}>
            <div>
              <strong>Would Queue</strong>
              <div className={styles.compactMeta}>{data.would_queue ? yesText : noText}</div>
            </div>
          </div>
          <div className={styles.compactItem}>
            <div>
              <strong>Would Auto-Send</strong>
              <div className={styles.compactMeta}>{data.would_auto_send ? yesText : noText}</div>
            </div>
          </div>
          {data.action ? (
            <div className={styles.compactItem}>
              <div>
                <strong>Action</strong>
                <div className={styles.compactMeta}>{data.action}</div>
              </div>
            </div>
          ) : null}
        </div>
      </div>
    </article>
  );
}
