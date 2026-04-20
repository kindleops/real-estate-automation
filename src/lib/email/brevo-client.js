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

function normalizeBrandKey(value) {
  return clean(value).toLowerCase();
}

function resolveBrandEnvVar(brand_key) {
  const normalized = normalizeBrandKey(brand_key);

  if (["prominent_cash_offer", "prominent", "pco"].includes(normalized)) {
    return "BREVO_PROMINENT_API_KEY";
  }

  if (["reivesti", "rivesti"].includes(normalized)) {
    return "BREVO_REIVESTI_API_KEY";
  }

  return null;
}

export function resolveBrevoApiKeyForBrand(
  brand_key,
  { allow_legacy_fallback = false } = {}
) {
  const env_var = resolveBrandEnvVar(brand_key);
  if (!env_var) return null;

  const brand_specific_key = clean(process.env[env_var]);
  if (brand_specific_key) return brand_specific_key;

  if (allow_legacy_fallback) {
    return clean(process.env.BREVO_API_KEY) || null;
  }

  return null;
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
    brand_key,
    provider_account_key,
  } = {},
  deps = {}
) {
  const requested_brand_key = clean(brand_key || provider_account_key);
  const default_brand_key = clean(process.env.EMAIL_DEFAULT_BRAND_KEY) || "prominent_cash_offer";
  const effective_brand_key = requested_brand_key || default_brand_key;

  const api_key = resolveBrevoApiKeyForBrand(effective_brand_key, {
    // Optional legacy fallback is only allowed for implicit/default brand calls.
    allow_legacy_fallback: !requested_brand_key,
  });

  if (!api_key) {
    const err = new Error("Email provider is not configured for this brand.");
    err.code = "missing_brevo_api_key_for_brand";
    err.brand_key = effective_brand_key || null;
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
