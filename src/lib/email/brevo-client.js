function clean(value) {
  return String(value ?? "").trim();
}

function sanitizeBrevoError({ status = null, code = null } = {}) {
  if (status === 401 || status === 403) {
    return {
      code: "brevo_unauthorized",
      message: "Email provider authorization failed.",
      retryable: false,
      status,
    };
  }

  if (status === 400 || status === 422) {
    return {
      code: code || "brevo_invalid_request",
      message: "Email provider rejected the request.",
      retryable: false,
      status,
    };
  }

  if (status === 429) {
    return {
      code: "brevo_rate_limited",
      message: "Email provider rate limit reached.",
      retryable: true,
      status,
    };
  }

  if (status && status >= 500) {
    return {
      code: "brevo_provider_unavailable",
      message: "Email provider is temporarily unavailable.",
      retryable: true,
      status,
    };
  }

  return {
    code: code || "brevo_send_failed",
    message: "Email provider send failed.",
    retryable: false,
    status,
  };
}

function toArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function sanitizeTextPayload(value, max = 5000) {
  return clean(value).slice(0, max);
}

export async function sendBrevoTransactionalEmail(
  {
    to,
    subject,
    htmlContent,
    textContent,
    sender,
    replyTo,
    tags,
    params,
  } = {},
  deps = {}
) {
  const api_key = clean(process.env.BREVO_API_KEY);
  if (!api_key) {
    const err = new Error("brevo_not_configured");
    err.code = "brevo_not_configured";
    err.retryable = false;
    throw err;
  }

  const fetch_impl = deps.fetch_impl || fetch;

  const to_email = clean(to);
  const subject_line = clean(subject);
  const html = sanitizeTextPayload(htmlContent, 200_000);
  const text = sanitizeTextPayload(textContent, 80_000);

  if (!to_email || !subject_line || !html) {
    const err = new Error("brevo_invalid_email_payload");
    err.code = "brevo_invalid_email_payload";
    err.retryable = false;
    throw err;
  }

  const payload = {
    sender: {
      name: clean(sender?.name) || clean(process.env.EMAIL_DEFAULT_SENDER_NAME) || "Acquisitions Team",
      email: clean(sender?.email) || clean(process.env.EMAIL_DEFAULT_SENDER_EMAIL),
    },
    to: [{ email: to_email }],
    subject: subject_line,
    htmlContent: html,
    ...(text ? { textContent: text } : {}),
    ...(clean(replyTo?.email) ? { replyTo: { email: clean(replyTo.email) } } : {}),
    ...(toArray(tags).length ? { tags: toArray(tags).map((tag) => clean(tag).slice(0, 64)) } : {}),
    ...(params && typeof params === "object" ? { params } : {}),
  };

  if (!payload.sender.email) {
    const err = new Error("brevo_sender_missing");
    err.code = "brevo_sender_missing";
    err.retryable = false;
    throw err;
  }

  let response;
  try {
    response = await fetch_impl("https://api.brevo.com/v3/smtp/email", {
      method: "POST",
      headers: {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": api_key,
      },
      body: JSON.stringify(payload),
    });
  } catch {
    const err = new Error("Email provider request failed.");
    err.code = "brevo_network_error";
    err.retryable = true;
    throw err;
  }

  let data = null;
  try {
    data = await response.json();
  } catch {
    data = null;
  }

  if (!response.ok) {
    const sanitized = sanitizeBrevoError({
      status: response.status,
      code: clean(data?.code),
    });
    const err = new Error(sanitized.message);
    err.code = sanitized.code;
    err.status = sanitized.status;
    err.retryable = sanitized.retryable;
    throw err;
  }

  return {
    ok: true,
    provider: "brevo",
    message_id: clean(data?.messageId || data?.message_id) || null,
  };
}

export default sendBrevoTransactionalEmail;
