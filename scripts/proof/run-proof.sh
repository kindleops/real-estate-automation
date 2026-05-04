#!/bin/bash
set -e

echo "[Proof] Setting up environment..."
export $(grep -v '^#' .env.local | xargs)

echo "[Proof] Running proof script..."
node --import ./tests/register-aliases.mjs scripts/proof/offer-stage-ai-dashboard-proof.mjs

echo ""
echo "[Proof] Done!"
