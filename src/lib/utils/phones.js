import { clean } from "@/lib/utils/strings.js";

export function digitsOnly(value) {
  return clean(value).replace(/\D+/g, "");
}

export function normalizePhone(value) {
  const digits = digitsOnly(value);

  if (!digits) return "";
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith("1")) return `+${digits}`;
  if (digits.startsWith("+")) return digits;

  return digits.length >= 11 ? `+${digits}` : "";
}

export function isValidUsPhone(value) {
  return Boolean(normalizePhone(value));
}

export function formatUsPhone(value) {
  const digits = digitsOnly(value);

  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }

  if (digits.length === 11 && digits.startsWith("1")) {
    return `+1 (${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }

  return clean(value);
}

export default {
  digitsOnly,
  normalizePhone,
  isValidUsPhone,
  formatUsPhone,
};