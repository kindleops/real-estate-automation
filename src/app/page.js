const routes = [
  "/dashboard/ops",
  "/api/internal/queue/dry-run",
  "/api/internal/outbound/queue-message",
  "/api/internal/outbound/send-now",
  "/api/internal/autopilot/run",
  "/api/internal/queue/run",
  "/api/internal/queue/retry",
  "/api/webhooks/textgrid/inbound",
  "/api/webhooks/textgrid/delivery",
  "/api/webhooks/docusign",
  "/api/webhooks/title",
  "/api/webhooks/closings",
  "/api/webhooks/podio/hooks",
];

export default function HomePage() {
  return (
    <main
      style={{
        fontFamily: "ui-sans-serif, system-ui, sans-serif",
        margin: "0 auto",
        maxWidth: "760px",
        padding: "48px 24px",
        lineHeight: 1.5,
      }}
    >
      <h1 style={{ marginBottom: "12px" }}>Real Estate Automation</h1>
      <p style={{ marginBottom: "24px" }}>
        Next.js runtime scaffold for the existing internal routes and webhook handlers.
      </p>
      <p style={{ marginBottom: "12px" }}>
        Operations dashboard: <code>/dashboard/ops</code>
      </p>
      <p style={{ marginBottom: "12px" }}>
        Start the server with <code>npm install && npm run dev</code>, then hit one of the
        API endpoints below.
      </p>
      <ul>
        {routes.map((route) => (
          <li key={route}>
            <code>{route}</code>
          </li>
        ))}
      </ul>
    </main>
  );
}
