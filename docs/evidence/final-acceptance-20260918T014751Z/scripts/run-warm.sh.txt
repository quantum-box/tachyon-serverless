#!/bin/bash
# R3b (PLT-4633 / PLT-4649): warm (idle quiesce / resume) vs cold on a real microVM
. "$(dirname "$0")/common.sh"
W=$RUNS/warm; sudo rm -rf "$W"; mkdir -p "$W"
mkcfg "$W/gateway.toml" 18484 "$W/data"
hostinfo "$W/host.txt" before
vmload_sampler "$W/vm-load.tsv"; S=$!
sudo -n env PATH="$PATH" HOME="$HOME" TSLS_SKIP_BUILD=1 \
  TSLS_GATEWAY_CONFIG="$W/gateway.toml" TSLS_API_URL=http://127.0.0.1:18484 \
  bash scripts/kvm/measure-warm.sh > "$W/stdout.txt" 2>&1
rc=$?
kill $S 2>/dev/null
echo "warm exit=$rc" | tee -a "$W/host.txt"
hostinfo "$W/host.txt" after
fix_owner "$REPO/docs/evidence" "$W" "$REPO/.kvm/run"
leftovers >> "$W/host.txt" 2>&1
tail -25 "$W/stdout.txt"
