import * as Sentry from "@sentry/nextjs";

import "@/app/globals.css";

export function generateMetadata() {
  return {
    title: "Real Estate Automation",
    description: "Local runtime for internal API routes and webhooks.",
    other: Sentry.getTraceData(),
  };
}

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
