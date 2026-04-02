import { sendEmail } from "@/lib/providers/email.js";

export async function createDraft({
  to,
  subject,
  body = "",
  html = "",
  cc = [],
  bcc = [],
} = {}) {
  const text = String(body || "");
  const html_body = html || (text ? `<pre>${text}</pre>` : "");

  const result = await sendEmail({
    to,
    subject,
    text,
    html: html_body,
    cc,
    bcc,
    dry_run: true,
  });

  return {
    ok: Boolean(result?.ok),
    drafted: true,
    provider_message_id: null,
    raw: result?.raw || null,
  };
}

export default {
  createDraft,
};
