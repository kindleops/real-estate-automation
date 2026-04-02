export function buildUrl(base, path = "", params = {}) {
  const url = new URL(path, base);

  Object.entries(params).forEach(([key, value]) => {
    if (value == null || value === "") return;
    url.searchParams.set(key, String(value));
  });

  return url.toString();
}

export function appendQueryParams(urlString, params = {}) {
  const url = new URL(urlString);

  Object.entries(params).forEach(([key, value]) => {
    if (value == null || value === "") return;
    url.searchParams.set(key, String(value));
  });

  return url.toString();
}

export default {
  buildUrl,
  appendQueryParams,
};