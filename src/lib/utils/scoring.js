export function sum(values = []) {
  return values.reduce((total, value) => total + Number(value || 0), 0);
}

export function average(values = [], fallback = 0) {
  if (!Array.isArray(values) || values.length === 0) return fallback;
  return sum(values) / values.length;
}

export function weightedScore(pairs = []) {
  let totalWeight = 0;
  let totalScore = 0;

  for (const pair of pairs) {
    const score = Number(pair?.score || 0);
    const weight = Number(pair?.weight || 0);

    totalScore += score * weight;
    totalWeight += weight;
  }

  if (!totalWeight) return 0;
  return totalScore / totalWeight;
}

export default {
  sum,
  average,
  weightedScore,
};