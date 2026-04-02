import { cookies } from "next/headers";

import OpsDashboardClient from "@/app/dashboard/ops/OpsDashboardClient.js";
import styles from "@/app/dashboard/ops/ops-dashboard.module.css";
import {
  OPS_DASHBOARD_SESSION_COOKIE,
  getOpsDashboardPageGate,
  hasValidOpsDashboardSessionToken,
} from "@/lib/security/dashboard-auth.js";

export const metadata = {
  title: "Ops Dashboard | Real Estate Automation",
  description: "Operational command center for queue, feeder, and transaction activity.",
};

function OpsDashboardAccessGate({ searchParams = {} }) {
  const auth_status = String(searchParams?.auth || "").trim().toLowerCase();

  return (
    <main className={styles.page}>
      <div className={styles.backdropGrid} />
      <section className={`${styles.panel} ${styles.loginPanel}`}>
        <div className={styles.panelEyebrow}>Internal Access</div>
        <h1 className={styles.title}>Ops Dashboard</h1>
        <p className={styles.subtitle}>
          This dashboard is protected by `OPS_DASHBOARD_SECRET`. Authenticate to
          access the internal queue, feeder, and KPI routes.
        </p>
        <form
          className={styles.loginForm}
          action="/api/internal/dashboard/ops/auth"
          method="post"
        >
          <input type="hidden" name="redirect_to" value="/dashboard/ops" />
          <label className={styles.control}>
            <span>Dashboard Secret</span>
            <input
              name="secret"
              type="password"
              autoComplete="current-password"
              placeholder="OPS_DASHBOARD_SECRET"
              required
            />
          </label>
          <button className={styles.resetButton} type="submit">
            Unlock Dashboard
          </button>
        </form>
        {auth_status === "invalid" ? (
          <div className={styles.errorBanner}>
            Dashboard authentication failed. Check `OPS_DASHBOARD_SECRET`.
          </div>
        ) : null}
      </section>
    </main>
  );
}

function OpsDashboardMisconfigured() {
  return (
    <main className={styles.page}>
      <div className={styles.backdropGrid} />
      <section className={`${styles.panel} ${styles.loginPanel}`}>
        <div className={styles.panelEyebrow}>Configuration Required</div>
        <h1 className={styles.title}>Ops Dashboard Locked</h1>
        <p className={styles.subtitle}>
          `OPS_DASHBOARD_SECRET` is required in production before the internal
          ops routes can be exposed.
        </p>
      </section>
    </main>
  );
}

export default function OpsDashboardPage({ searchParams = {} }) {
  const gate = getOpsDashboardPageGate();

  if (!gate.ok) {
    return <OpsDashboardMisconfigured />;
  }

  if (gate.required) {
    const cookie_store = cookies();
    const session_token =
      cookie_store.get(OPS_DASHBOARD_SESSION_COOKIE)?.value || "";

    if (!hasValidOpsDashboardSessionToken(session_token)) {
      return <OpsDashboardAccessGate searchParams={searchParams} />;
    }
  }

  return <OpsDashboardClient />;
}
