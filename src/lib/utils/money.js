export function toMoneyNumber(value, fallback = 0) {
  if (typeof value === "number" && Number.isFinite(value)) return value;

  const cleaned = String(value ?? "").replace(/[^0-9.-]/g, "");
  const parsed = Number(cleaned);

  return Number.isFinite(parsed) ? parsed : fallback;
}

export function formatUsd(value) {
  const amount = toMoneyNumber(value, 0);

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(amount);
}

export default {
  toMoneyNumber,
  formatUsd,
};