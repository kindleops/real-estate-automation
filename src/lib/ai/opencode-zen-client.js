import ENV from "@/lib/config/env.js";

const ENDPOINT = "https://opencode.ai/zen/v1/chat/completions";
const MODEL = "big-pickle";
const DEFAULT_TIMEOUT_MS = 30000;
const DEFAULT_RETRIES = 2;
const RETRY_BACKOFF_MS = 1000;

export function log(prefix, message, data = null) {
  const ts = new Date().toISOString();
  if (data) {
    console.log(`[${ts}] [${prefix}] ${message}`, data);
  } else {
    console.log(`[${ts}] [${prefix}] ${message}`);
  }
}

export function maskPhone(text) {
  if (!text) return text;
  return text.replace(/\b(\+?1[\s.-]?)?\(?(\d{3})\)?[\s.-]?(\d{3})[\s.-]?(\d{4})\b/g,
    (match, prefix, area, exch, num) => {
      return `${prefix || ""}(${area}) ***-${num}`;
    });
}

export function maskName(text) {
  if (!text) return text;
  return text.replace(/\b([A-Z][a-z]+)\s+([A-Z][a-z]+)\b/g, "$1 [REDACTED]");
}

export function redactContext(text) {
  if (!text) return text;
  let result = text;
  result = maskPhone(result);
  result = maskName(result);
  result = result.replace(/OPENCODE_ZEN_API_KEY[=:]\s*\S+/gi, "OPENCODE_ZEN_API_KEY=[REDACTED]");
  result = result.replace(/OPENAI_KEY[=:]\s*\S+/gi, "OPENAI_KEY=[REDACTED]");
  result = result.replace(/TEXTGRID_AUTH_TOKEN[=:]\s*\S+/gi, "TEXTGRID_AUTH_TOKEN=[REDACTED]");
  result = result.replace(/PODIO_CLIENT_SECRET[=:]\s*\S+/gi, "PODIO_CLIENT_SECRET=[REDACTED]");
  return result;
}

export function parseJSON(text) {
  if (!text) return null;
  const cleaned = text.trim();
  const jsonMatch = cleaned.match(/```json\s*([\s\S]*?)\s*```|({[\s\S]*})|(\[[\s\S]*\])/);
  if (jsonMatch) {
    try {
      return JSON.parse(jsonMatch[1] || jsonMatch[2] || jsonMatch[3]);
    } catch {
      // fall through
    }
  }
  try {
    return JSON.parse(cleaned);
  } catch {
    return null;
  }
}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export async function callBigPickle(messages, options = {}) {
  const apiKey = options.apiKey || process.env.OPENCODE_ZEN_API_KEY;
  if (!apiKey) {
    log("BigPickle", "OPENCODE_ZEN_API_KEY not set, skipping");
    return null;
  }

  const timeout = options.timeout ?? DEFAULT_TIMEOUT_MS;
  const retries = options.retries ?? DEFAULT_RETRIES;
  const temperature = options.temperature ?? 0;
  const max_tokens = options.max_tokens ?? 1024;

  const payload = {
    model: MODEL,
    messages,
    temperature,
    max_tokens,
  };

  let lastError = null;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      log("BigPickle", `Calling big-pickle (attempt ${attempt + 1}/${retries + 1})`);

      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeout);

      const response = await fetch(ENDPOINT, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${apiKey}`,
        },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        const errorText = await response.text().catch(() => "unknown error");
        throw new Error(`OpenCode Zen API error ${response.status}: ${errorText}`);
      }

      const data = await response.json();
      const content = data.choices?.[0]?.message?.content ?? null;

      log("BigPickle", "Received response", { contentLength: content?.length });

      if (options.expectJson) {
        const parsed = parseJSON(content);
        if (!parsed) {
          log("BigPickle", "JSON parse failed, invalid response", { content: content?.slice(0, 200) });
          return null;
        }
        return parsed;
      }

      return content;
    } catch (err) {
      lastError = err;
      log("BigPickle", `Attempt ${attempt + 1} failed`, { error: err.message });

      if (attempt < retries) {
        const backoff = RETRY_BACKOFF_MS * Math.pow(2, attempt);
        log("BigPickle", `Retrying in ${backoff}ms`);
        await sleep(backoff);
      }
    }
  }

  log("BigPickle", "All retries exhausted", { error: lastError?.message });
  return null;
}

export function buildSafeContext({ sellerName, phone, message, propertyDetails, additionalContext }) {
  const parts = [];

  if (propertyDetails) {
    const redacted = redactContext(propertyDetails);
    parts.push(`Property: ${redacted}`);
  }

  if (message) {
    const redacted = redactContext(message);
    parts.push(`Message: ${redacted}`);
  }

  if (additionalContext) {
    const redacted = redactContext(additionalContext);
    parts.push(redacted);
  }

  return parts.join("\n");
}

export function isStale(dateStr, staleDays = 30) {
  if (!dateStr) return false;
  const date = new Date(dateStr);
  if (isNaN(date.getTime())) return false;
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffDays = diffMs / (1000 * 60 * 60 * 24);
  return diffDays >= staleDays;
}
