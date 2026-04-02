import OpenAI from "openai";

import ENV from "@/lib/config/env.js";

const client = new OpenAI({
  apiKey: ENV.OPENAI_KEY,
});

function normalizeMessages(input) {
  if (typeof input === "string") {
    return [{ role: "user", content: input }];
  }

  if (Array.isArray(input)) {
    return input;
  }

  if (input && Array.isArray(input.messages)) {
    return input.messages;
  }

  throw new Error("ai: invalid input. Expected a prompt string or an object with messages.");
}

export async function ai(input, overrides = {}) {
  const isStringInput = typeof input === "string";
  const config = isStringInput ? {} : (input || {});

  const model = overrides.model || config.model || "gpt-4o-mini";
  const temperature =
    overrides.temperature ?? config.temperature ?? (isStringInput ? 0.4 : 0);
  const max_tokens = overrides.max_tokens ?? config.max_tokens;
  const messages = normalizeMessages(input);

  const request = {
    model,
    messages,
    temperature,
  };

  if (typeof max_tokens === "number") {
    request.max_tokens = max_tokens;
  }

  const res = await client.chat.completions.create(request);

  return res.choices?.[0]?.message?.content ?? "";
}

export default ai;