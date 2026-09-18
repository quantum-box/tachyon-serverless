#!/bin/bash
# R2 (PLT-4622 / PLT-4649): egress none / restricted / public-web, jailer, resource and disk limits
. "$(dirname "$0")/common.sh"
W=$RUNS/isolation; sudo rm -rf "$W"; mkdir -p "$W"
mkcfg "$W/gateway.toml" 18482 "$W/data"
hostinfo "$W/host.txt" before
vmload_sampler "$W/vm-load.tsv"; S=$!
sudo -n env PATH="$PATH" HOME="$HOME" TSLS_SKIP_BUILD=1 \
  TSLS_GATEWAY_CONFIG="$W/gateway.toml" TSLS_API_URL=http://127.0.0.1:18482 \
  bash scripts/kvm/measure-isolation.sh > "$W/stdout.txt" 2>&1
rc=$?
kill $S 2>/dev/null
echo "isolation exit=$rc" | tee -a "$W/host.txt"
hostinfo "$W/host.txt" after
fix_owner "$REPO/docs/evidence" "$W" "$REPO/.kvm/run"
leftovers >> "$W/host.txt" 2>&1
tail -30 "$W/stdout.txt"
