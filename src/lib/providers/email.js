import crypto from "node:crypto";
import net from "node:net";
import tls from "node:tls";

import ENV from "@/lib/config/env.js";
import { child } from "@/lib/logging/logger.js";

const logger = child({
  module: "providers.email",
});

const SMTP_TIMEOUT_MS = 30_000;

function clean(value) {
  return String(value ?? "").trim();
}

function stripNewlines(value) {
  return clean(value).replace(/[\r\n]+/g, " ");
}

function wrapBase64(value = "") {
  return Buffer.from(String(value ?? ""), "utf8")
    .toString("base64")
    .replace(/(.{76})/g, "$1\r\n");
}

function formatAddress(address) {
  if (!address) return null;

  if (typeof address === "string") {
    const email = clean(address);
    return email ? { email, name: "" } : null;
  }

  if (typeof address === "object") {
    const email = clean(address.email || address.address || address.value);
    const name = stripNewlines(address.name || address.label || "");
    return email ? { email, name } : null;
  }

  return null;
}

function normalizeRecipients(value) {
  const input = Array.isArray(value) ? value : [value];
  return input.map(formatAddress).filter(Boolean);
}

function toHeaderRecipients(recipients = []) {
  return recipients
    .map((recipient) =>
      recipient.name
        ? `"${recipient.name.replace(/"/g, '\\"')}" <${recipient.email}>`
        : recipient.email
    )
    .join(", ");
}

function buildFromAddress() {
  const email = clean(ENV.SMTP_FROM || ENV.SMTP_USERNAME);
  const name = stripNewlines(ENV.SMTP_FROM_NAME || "");

  if (!email) {
    return null;
  }

  return { email, name };
}

export function getSmtpConfigSummary() {
  const host = clean(ENV.SMTP_HOST);
  const port = Number(ENV.SMTP_PORT || 587);
  const username = clean(ENV.SMTP_USERNAME);
  const password = clean(ENV.SMTP_PASSWORD);
  const from = buildFromAddress();
  const missing = [];

  if (!host) missing.push("SMTP_HOST");
  if (!username) missing.push("SMTP_USERNAME");
  if (!password) missing.push("SMTP_PASSWORD");
  if (!from?.email) missing.push("SMTP_FROM_OR_SMTP_USERNAME");

  return {
    configured: missing.length === 0,
    missing,
    host_present: Boolean(host),
    port,
    username_present: Boolean(username),
    password_present: Boolean(password),
    from_present: Boolean(from?.email),
  };
}

function buildMimeMessage({
  from,
  to = [],
  cc = [],
  subject = "",
  text = "",
  html = "",
  message_id,
}) {
  const boundary = `alt-${crypto.randomUUID()}`;
  const has_text = text !== "";
  const has_html = html !== "";
  const body_text = has_text ? String(text) : String(html || "");
  const body_html = has_html ? String(html) : "";

  const headers = [
    `Date: ${new Date().toUTCString()}`,
    `Message-ID: <${message_id}>`,
    `From: ${toHeaderRecipients([from])}`,
    `To: ${toHeaderRecipients(to)}`,
    ...(cc.length ? [`Cc: ${toHeaderRecipients(cc)}`] : []),
    `Subject: ${stripNewlines(subject) || "(no subject)"}`,
    "MIME-Version: 1.0",
  ];

  if (has_text && has_html) {
    headers.push(`Content-Type: multipart/alternative; boundary="${boundary}"`);

    return [
      ...headers,
      "",
      `--${boundary}`,
      'Content-Type: text/plain; charset="UTF-8"',
      "Content-Transfer-Encoding: base64",
      "",
      wrapBase64(body_text),
      `--${boundary}`,
      'Content-Type: text/html; charset="UTF-8"',
      "Content-Transfer-Encoding: base64",
      "",
      wrapBase64(body_html),
      `--${boundary}--`,
      "",
    ].join("\r\n");
  }

  headers.push(
    `Content-Type: ${has_html ? 'text/html' : 'text/plain'}; charset="UTF-8"`,
    "Content-Transfer-Encoding: base64"
  );

  return [
    ...headers,
    "",
    wrapBase64(has_html ? body_html : body_text),
    "",
  ].join("\r\n");
}

function createLineQueue(socket) {
  let buffer = "";
  let closed_error = null;
  const queued_lines = [];
  const waiters = [];

  const flush = () => {
    while (true) {
      const newline_index = buffer.indexOf("\n");
      if (newline_index === -1) break;

      let line = buffer.slice(0, newline_index);
      buffer = buffer.slice(newline_index + 1);

      if (line.endsWith("\r")) line = line.slice(0, -1);

      if (waiters.length) {
        waiters.shift().resolve(line);
      } else {
        queued_lines.push(line);
      }
    }
  };

  const fail = (error) => {
    if (closed_error) return;
    closed_error = error instanceof Error ? error : new Error(String(error));

    while (waiters.length) {
      waiters.shift().reject(closed_error);
    }
  };

  const on_data = (chunk) => {
    buffer += chunk.toString("utf8");
    flush();
  };

  const on_error = (error) => fail(error);
  const on_close = () => fail(new Error("SMTP connection closed"));
  const on_timeout = () => fail(new Error("SMTP timeout"));

  socket.on("data", on_data);
  socket.on("error", on_error);
  socket.on("close", on_close);
  socket.on("timeout", on_timeout);

  return {
    async nextLine() {
      if (queued_lines.length) return queued_lines.shift();
      if (closed_error) throw closed_error;

      return new Promise((resolve, reject) => {
        waiters.push({ resolve, reject });
      });
    },
    detach() {
      socket.off("data", on_data);
      socket.off("error", on_error);
      socket.off("close", on_close);
      socket.off("timeout", on_timeout);
    },
  };
}

async function readResponse(queue) {
  const lines = [];
  let code = null;

  while (true) {
    const line = await queue.nextLine();
    lines.push(line);

    const match = line.match(/^(\d{3})([ -])(.*)$/);
    if (!match) continue;

    code ||= Number(match[1]);

    if (match[2] === " " && Number(match[1]) === code) {
      return {
        code,
        lines,
        message: lines.map((value) => value.replace(/^\d{3}[ -]?/, "")).join("\n"),
      };
    }
  }
}

function ensureExpected(response, expected_codes, command) {
  if (expected_codes.includes(response.code)) return;

  throw new Error(
    `SMTP ${command} failed with ${response.code}: ${response.message || "unknown_response"}`
  );
}

function writeLine(socket, line) {
  return new Promise((resolve, reject) => {
    socket.write(`${line}\r\n`, (error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

async function sendCommand(socket, queue, command, expected_codes = [250]) {
  await writeLine(socket, command);
  const response = await readResponse(queue);
  ensureExpected(response, expected_codes, command.split(" ")[0]);
  return response;
}

function parseCapabilities(response) {
  return response.lines
    .slice(1)
    .map((line) => line.replace(/^\d{3}[ -]?/, "").trim().toUpperCase())
    .filter(Boolean);
}

async function connectSocket({ host, port }) {
  const secure = port === 465;

  const socket = secure
    ? tls.connect({
        host,
        port,
        servername: host,
      })
    : net.createConnection({
        host,
        port,
      });

  socket.setTimeout(SMTP_TIMEOUT_MS);

  await new Promise((resolve, reject) => {
    const on_error = (error) => {
      socket.off("connect", on_connect);
      socket.off("secureConnect", on_secure_connect);
      reject(error);
    };

    const on_connect = () => {
      socket.off("error", on_error);
      resolve();
    };

    const on_secure_connect = () => {
      socket.off("error", on_error);
      resolve();
    };

    socket.once("error", on_error);

    if (secure) {
      socket.once("secureConnect", on_secure_connect);
    } else {
      socket.once("connect", on_connect);
    }
  });

  return {
    socket,
    secure,
  };
}

async function upgradeToTls({ socket, queue, host }) {
  await sendCommand(socket, queue, "STARTTLS", [220]);
  queue.detach();

  const secure_socket = tls.connect({
    socket,
    servername: host,
  });

  secure_socket.setTimeout(SMTP_TIMEOUT_MS);

  await new Promise((resolve, reject) => {
    secure_socket.once("secureConnect", resolve);
    secure_socket.once("error", reject);
  });

  return {
    socket: secure_socket,
    queue: createLineQueue(secure_socket),
  };
}

async function authenticate(socket, queue, capabilities, username, password) {
  const auth_line = capabilities.find((line) => line.startsWith("AUTH "));
  const auth_caps = auth_line ? auth_line.split(/\s+/).slice(1) : [];

  if (auth_caps.includes("PLAIN")) {
    const payload = Buffer.from(`\u0000${username}\u0000${password}`, "utf8").toString("base64");
    await sendCommand(socket, queue, `AUTH PLAIN ${payload}`, [235]);
    return;
  }

  if (auth_caps.includes("LOGIN") || !auth_caps.length) {
    await sendCommand(socket, queue, "AUTH LOGIN", [334]);
    await sendCommand(
      socket,
      queue,
      Buffer.from(username, "utf8").toString("base64"),
      [334]
    );
    await sendCommand(
      socket,
      queue,
      Buffer.from(password, "utf8").toString("base64"),
      [235]
    );
    return;
  }

  throw new Error(`SMTP auth mechanism not supported: ${auth_line || "none"}`);
}

async function sendViaSmtp({
  from,
  to = [],
  cc = [],
  bcc = [],
  subject = "",
  text = "",
  html = "",
}) {
  const host = clean(ENV.SMTP_HOST);
  const port = Number(ENV.SMTP_PORT || 587);
  const username = clean(ENV.SMTP_USERNAME);
  const password = clean(ENV.SMTP_PASSWORD);

  if (!host) throw new Error("smtp_missing_host");
  if (!from?.email) throw new Error("smtp_missing_from");
  if (!username || !password) throw new Error("smtp_missing_auth");

  const all_recipients = [...to, ...cc, ...bcc];
  if (!all_recipients.length) throw new Error("smtp_missing_recipients");

  let connection = await connectSocket({ host, port });
  let queue = createLineQueue(connection.socket);

  try {
    const greeting = await readResponse(queue);
    ensureExpected(greeting, [220], "CONNECT");

    const ehlo_host = clean(process.env.SMTP_EHLO_HOST) || "localhost";
    const ehlo_response = await sendCommand(connection.socket, queue, `EHLO ${ehlo_host}`);
    let capabilities = parseCapabilities(ehlo_response);

    if (!connection.secure && capabilities.some((line) => line === "STARTTLS")) {
      const upgraded = await upgradeToTls({
        socket: connection.socket,
        queue,
        host,
      });

      connection = {
        ...connection,
        socket: upgraded.socket,
        secure: true,
      };
      queue = upgraded.queue;

      const secure_ehlo = await sendCommand(connection.socket, queue, `EHLO ${ehlo_host}`);
      capabilities = parseCapabilities(secure_ehlo);
    }

    await authenticate(connection.socket, queue, capabilities, username, password);
    await sendCommand(connection.socket, queue, `MAIL FROM:<${from.email}>`);

    const seen_recipients = new Set();
    for (const recipient of all_recipients) {
      const email = clean(recipient.email).toLowerCase();
      if (!email || seen_recipients.has(email)) continue;
      seen_recipients.add(email);
      await sendCommand(connection.socket, queue, `RCPT TO:<${recipient.email}>`, [250, 251]);
    }

    await sendCommand(connection.socket, queue, "DATA", [354]);

    const message_id = `${crypto.randomUUID()}@${host}`;
    const message = buildMimeMessage({
      from,
      to,
      cc,
      subject,
      text,
      html,
      message_id,
    })
      .replace(/\r?\n\./g, "\r\n..");

    await new Promise((resolve, reject) => {
      connection.socket.write(`${message}\r\n.\r\n`, (error) => {
        if (error) reject(error);
        else resolve();
      });
    });

    const accepted = await readResponse(queue);
    ensureExpected(accepted, [250], "DATA");

    await sendCommand(connection.socket, queue, "QUIT", [221]);

    return {
      ok: true,
      provider_message_id: message_id,
      raw: {
        response: accepted.message,
        host,
        port,
      },
    };
  } finally {
    queue.detach();
    connection.socket.destroy();
  }
}

export async function verifySmtpConnection() {
  const host = clean(ENV.SMTP_HOST);
  const port = Number(ENV.SMTP_PORT || 587);
  const username = clean(ENV.SMTP_USERNAME);
  const password = clean(ENV.SMTP_PASSWORD);

  if (!host) {
    return {
      ok: false,
      reason: "smtp_missing_host",
    };
  }

  if (!username || !password) {
    return {
      ok: false,
      reason: "smtp_missing_auth",
    };
  }

  let connection = null;
  let queue = null;

  try {
    connection = await connectSocket({ host, port });
    queue = createLineQueue(connection.socket);

    const greeting = await readResponse(queue);
    ensureExpected(greeting, [220], "CONNECT");

    const ehlo_host = clean(process.env.SMTP_EHLO_HOST) || "localhost";
    const ehlo_response = await sendCommand(connection.socket, queue, `EHLO ${ehlo_host}`);
    let capabilities = parseCapabilities(ehlo_response);

    if (!connection.secure && capabilities.some((line) => line === "STARTTLS")) {
      const upgraded = await upgradeToTls({
        socket: connection.socket,
        queue,
        host,
      });

      connection = {
        ...connection,
        socket: upgraded.socket,
        secure: true,
      };
      queue = upgraded.queue;

      const secure_ehlo = await sendCommand(connection.socket, queue, `EHLO ${ehlo_host}`);
      capabilities = parseCapabilities(secure_ehlo);
    }

    await authenticate(connection.socket, queue, capabilities, username, password);
    await sendCommand(connection.socket, queue, "QUIT", [221]);

    return {
      ok: true,
      reason: "smtp_connection_verified",
      host,
      port,
      secure: connection.secure,
    };
  } catch (error) {
    return {
      ok: false,
      reason: clean(error?.message) || "smtp_connection_failed",
      host,
      port,
    };
  } finally {
    queue?.detach?.();
    connection?.socket?.destroy?.();
  }
}

export async function sendEmail({
  to,
  subject,
  html = "",
  text = "",
  cc = [],
  bcc = [],
  attachments = [],
  dry_run = false,
}) {
  const normalized_to = normalizeRecipients(to);
  const normalized_cc = normalizeRecipients(cc);
  const normalized_bcc = normalizeRecipients(bcc);
  const from = buildFromAddress();
  const payload = {
    from: from?.email || null,
    to: normalized_to.map((recipient) => recipient.email),
    subject,
    html,
    text,
    cc: normalized_cc.map((recipient) => recipient.email),
    bcc: normalized_bcc.map((recipient) => recipient.email),
    attachments_count: Array.isArray(attachments) ? attachments.length : 0,
    dry_run,
  };

  logger.info("email.send_requested", payload);

  if (dry_run) {
    return {
      ok: true,
      dry_run: true,
      provider_message_id: null,
      raw: payload,
    };
  }

  if (Array.isArray(attachments) && attachments.length > 0) {
    const error_message = "email_attachments_not_supported";
    logger.warn("email.send_failed", {
      ...payload,
      error_message,
    });

    return {
      ok: false,
      dry_run: false,
      provider_message_id: null,
      raw: payload,
      error_message,
    };
  }

  try {
    const result = await sendViaSmtp({
      from,
      to: normalized_to,
      cc: normalized_cc,
      bcc: normalized_bcc,
      subject,
      text,
      html,
    });

    logger.info("email.send_completed", {
      ...payload,
      provider_message_id: result.provider_message_id,
    });

    return {
      ok: true,
      dry_run: false,
      provider_message_id: result.provider_message_id,
      raw: {
        ...payload,
        smtp: result.raw,
      },
    };
  } catch (error) {
    logger.warn("email.send_failed", {
      ...payload,
      error_message: error?.message || "smtp_send_failed",
    });

    return {
      ok: false,
      dry_run: false,
      provider_message_id: null,
      raw: payload,
      error_message: error?.message || "smtp_send_failed",
    };
  }
}

export default {
  sendEmail,
};
