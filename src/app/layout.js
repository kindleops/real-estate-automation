import "@/app/globals.css";

export const metadata = {
  title: "Real Estate Automation",
  description: "Local runtime for internal API routes and webhooks.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
