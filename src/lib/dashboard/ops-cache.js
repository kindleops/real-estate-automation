const globalCache = globalThis.__ops_dashboard_cache__ || new Map();

if (!globalThis.__ops_dashboard_cache__) {
  globalThis.__ops_dashboard_cache__ = globalCache;
}

function now() {
  return Date.now();
}

export async function readThroughCache(key, ttl_ms, loader) {
  const cache_key = String(key || "");
  const cached = globalCache.get(cache_key);

  if (cached && cached.expires_at > now() && "value" in cached) {
    return cached.value;
  }

  if (cached?.promise) {
    return cached.promise;
  }

  const promise = Promise.resolve()
    .then(loader)
    .then((value) => {
      globalCache.set(cache_key, {
        value,
        expires_at: now() + Math.max(0, Number(ttl_ms) || 0),
      });
      return value;
    })
    .catch((error) => {
      globalCache.delete(cache_key);
      throw error;
    });

  globalCache.set(cache_key, {
    promise,
    expires_at: now() + Math.max(0, Number(ttl_ms) || 0),
  });

  return promise;
}

export function clearDashboardCache(prefix = "") {
  const normalized = String(prefix || "");

  for (const key of globalCache.keys()) {
    if (!normalized || String(key).startsWith(normalized)) {
      globalCache.delete(key);
    }
  }
}

export default {
  readThroughCache,
  clearDashboardCache,
};
