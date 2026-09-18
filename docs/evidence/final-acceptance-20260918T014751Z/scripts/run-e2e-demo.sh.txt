#!/bin/bash
# R7 (PLT-4649): the Firecracker E2E demo (28 steps: timeout, logs across a restart, secrets)
. "$(dirname "$0")/common.sh"
W=$RUNS/e2e-demo; sudo rm -rf "$W"; mkdir -p "$W"
mkcfg "$W/gateway.toml" 18481 "$W/data"
hostinfo "$W/host.txt" before
vmload_sampler "$W/vm-load.tsv"; S=$!
sudo -n env PATH="$PATH" HOME="$HOME" TSLS_PROVIDER=firecracker TSLS_SKIP_BUILD=1 \
  TSLS_GATEWAY_CONFIG="$W/gateway.toml" TSLS_API_URL=http://127.0.0.1:18481 \
  bash scripts/e2e/demo.sh > "$W/stdout.txt" 2>&1
rc=$?
kill $S 2>/dev/null
echo "e2e demo exit=$rc" | tee -a "$W/host.txt"
hostinfo "$W/host.txt" after
fix_owner "$REPO/docs/evidence" "$W" "$REPO/.kvm/run"
leftovers >> "$W/host.txt" 2>&1
tail -20 "$W/stdout.txt"
