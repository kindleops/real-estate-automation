const LOG_LEVEL = process.env.LOG_LEVEL || "info";
const NODE_ENV = process.env.NODE_ENV || "development";

const LEVELS = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

function shouldLog(level) {
  const current = LEVELS[String(LOG_LEVEL).toLowerCase()] ?? LEVELS.info;
  const incoming = LEVELS[String(level).toLowerCase()] ?? LEVELS.info;
  return incoming >= current;
}

function safeSerialize(value) {
  try {
    return JSON.parse(
      JSON.stringify(value, (_key, val) => {
        if (val instanceof Error) {
          return {
            name: val.name,
            message: val.message,
            stack: val.stack,
          };
        }
        return val;
      })
    );
  } catch {
    return { serialization_error: true };
  }
}

function buildLogEntry(level, event, meta = {}) {
  return {
    timestamp: new Date().toISOString(),
    level: String(level).toUpperCase(),
    event: String(event || "unknown.event"),
    env: NODE_ENV,
    meta: safeSerialize(meta),
  };
}

function print(entry) {
  if (NODE_ENV === "development") {
    const { timestamp, level, event, meta } = entry;
    const output = [`[${timestamp}] ${level} ${event}`, meta];

    if (level === "ERROR") {
      console.error(...output);
      return;
    }

    console.log(...output);
    return;
  }

  const line = JSON.stringify(entry);

  if (entry.level === "ERROR") {
    console.error(line);
    return;
  }

  console.log(line);
}

export function log(level, event, meta = {}) {
  if (!shouldLog(level)) return;
  const entry = buildLogEntry(level, event, meta);
  print(entry);
}

export function debug(event, meta = {}) {
  log("debug", event, meta);
}

export function info(event, meta = {}) {
  log("info", event, meta);
}

export function warn(event, meta = {}) {
  log("warn", event, meta);
}

export function error(event, meta = {}) {
  log("error", event, meta);
}

export function child(base_meta = {}) {
  return {
    debug(event, meta = {}) {
      log("debug", event, { ...base_meta, ...meta });
    },
    info(event, meta = {}) {
      log("info", event, { ...base_meta, ...meta });
    },
    warn(event, meta = {}) {
      log("warn", event, { ...base_meta, ...meta });
    },
    error(event, meta = {}) {
      log("error", event, { ...base_meta, ...meta });
    },
  };
}

export function withRequestContext(context = {}) {
  return child(context);
}

export default {
  log,
  debug,
  info,
  warn,
  error,
  child,
  withRequestContext,
};