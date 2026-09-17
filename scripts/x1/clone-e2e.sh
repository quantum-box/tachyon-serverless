#!/usr/bin/env bash
# scripts/x1/clone-e2e.sh - X1 (PLT-4653, experimental): snapshot manifest, compatibility checks
# and the Firecracker clone path, end to end through the gateway on real KVM.
#
# Flow (every check is one row of summary.tsv; exit 0 only when all pass):
#   build     gateway + cli with `experimental-restore` and the guest bridge with it, into
#             target/x1-restore (the default target stays a normal build), and a rootfs image
#             with that bridge at .kvm/x1/rootfs-restore.ext4
#   config    config/gateway.firecracker.toml (jailer, cgroup required) with profile dev, its own
#             listen / data_dir / rootfs, and [snapshots] enabled + allow_unverified + fresh keys
#   run       as root (jailer and cgroups); the build runs as the calling user first
#     provider          snapshot_create / snapshot_clone are reported and not `unsupported`
#     cold-N            restore-aware without a restore policy: the cold baseline
#     require-no-snap   restore = require before any snapshot -> Host.RestoreRequiredUnavailable
#     snapshot          POST /v1/functions/{id}/snapshots: source held at the checkpoint, sealed
#     clones-concurrent two invocations at once -> two clones: start_kind restored, distinct
#                       environment / instance ids, each scratch marker is its own, both VMMs map
#                       the one memory file, restored guest clock within 5 s of the host
#     restore-N         sequential restores for the latency table
#     revision-stale    a new revision (restore = require) refuses the old snapshot
#     corrupt-require   one flipped byte in the memory file -> refused, snapshot quarantined
#     corrupt-prefer    same on a prefer revision -> succeeds cold with restore_fallback recorded
#     cleanup           gateway stopped; no firecracker, jail, cgroup, env dir, snapshot or data left
#
# Usage:   scripts/x1/clone-e2e.sh [out_dir]      (default docs/evidence/x1-clone-<UTC>)
# Env:     TSLS_SKIP_BUILD=1, CLONE_PORT (18093), COLD_RUNS (3), RESTORE_RUNS (3), KEEP_WORK=1
# Needs:   Linux + /dev/kvm, passwordless sudo, .kvm/bin/{firecracker,jailer}, .kvm/vmlinux
#          (scripts/kvm/bootstrap.sh), cargo with <arch>-unknown-linux-musl, curl, jq.
# Exit:    0 all checks passed, 1 a check failed, 2 prerequisites missing.
#
# The functions run through `record` and a trap, which shellcheck cannot follow (SC2317).
# shellcheck disable=SC2317
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

ARCH="$(uname -m)"
MUSL_TARGET="${ARCH}-unknown-linux-musl"
X1_TARGET="$REPO_ROOT/target/x1-restore"
GATEWAY_BIN="$X1_TARGET/release/tachyon-serverless-gateway"
TSLS_BIN="$X1_TARGET/release/tsls"
GUEST_DIR="$X1_TARGET/$MUSL_TARGET/release"
ROOTFS="$REPO_ROOT/.kvm/x1/rootfs-restore.ext4"
BASE_CONFIG="${TSLS_GATEWAY_CONFIG:-$REPO_ROOT/config/gateway.firecracker.toml}"
PORT="${CLONE_PORT:-18093}"
API="http://127.0.0.1:$PORT"
TOKEN="${TSLS_TOKEN:-dev-token-tenant-a}"
COLD_RUNS="${COLD_RUNS:-3}"
RESTORE_RUNS="${RESTORE_RUNS:-3}"
STAMP="${CLONE_STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="${1:-$REPO_ROOT/docs/evidence/x1-clone-$STAMP}"
WORK="$REPO_ROOT/.kvm/x1/clone-$STAMP"

log() { printf '[x1-clone] %s\n' "$*" >&2; }
ms_now() { echo $(($(date +%s%N) / 1000000)); }

# --- phase 1: build as the calling user, then re-run as root ------------------------------------
if [ "${1:-}" != "--as-root" ] && [ "$(id -u)" != 0 ]; then
  for tool in cargo curl jq sudo; do
    command -v "$tool" >/dev/null 2>&1 || { log "missing $tool"; exit 2; }
  done
  if [ ! -r /dev/kvm ] || [ ! -w /dev/kvm ]; then
    log "/dev/kvm is not readable and writable"
    exit 2
  fi
  for f in .kvm/bin/firecracker .kvm/bin/jailer .kvm/vmlinux; do
    [ -f "$f" ] || { log "missing $f (scripts/kvm/bootstrap.sh)"; exit 2; }
  done
  if [ "${TSLS_SKIP_BUILD:-0}" != 1 ]; then
    log "building gateway + cli (experimental-restore) and the guest (bridge with experimental-restore)"
    # Release: sealing and verifying 256 MiB of guest memory is ~50x slower in a debug build.
    CARGO_TARGET_DIR="$X1_TARGET" cargo build --release -p tachyon-serverless-gateway \
      --features tachyon-serverless-gateway/experimental-restore -p tachyon-serverless-cli >&2
    CARGO_TARGET_DIR="$X1_TARGET" cargo build --release --target "$MUSL_TARGET" \
      -p tachyon-serverless-runtime-bridge --features tachyon-serverless-runtime-bridge/experimental-restore \
      -p example-restore-aware >&2
    mkdir -p "$(dirname "$ROOTFS")"
    BRIDGE_BIN="$GUEST_DIR/tachyon-serverless-runtime-bridge" ROOTFS="$ROOTFS" scripts/kvm/build-rootfs.sh >&2
  fi
  for b in "$GATEWAY_BIN" "$TSLS_BIN" "$GUEST_DIR/example-restore-aware" "$ROOTFS"; do
    [ -f "$b" ] || { log "missing $b"; exit 2; }
  done
  mkdir -p "$OUT"
  rc=0
  sudo -n env PATH="$PATH" HOME="$HOME" CLONE_STAMP="$STAMP" CLONE_PORT="$PORT" CLONE_COMMIT="${CLONE_COMMIT:-}" \
    COLD_RUNS="$COLD_RUNS" RESTORE_RUNS="$RESTORE_RUNS" KEEP_WORK="${KEEP_WORK:-0}" \
    TSLS_GATEWAY_CONFIG="$BASE_CONFIG" bash "$0" --as-root "$OUT" || rc=$?
  sudo -n chown -R "$(id -u):$(id -g)" "$OUT" 2>/dev/null || true
  exit "$rc"
fi
[ "${1:-}" = "--as-root" ] && shift
OUT="${1:-$OUT}"

# --- phase 2: as root ---------------------------------------------------------------------------
mkdir -p "$OUT" "$WORK/data" "$OUT/invocations"
RESULTS="$OUT/summary.tsv"
printf 'check\tresult\tmetric_ms\tdetail\n' >"$RESULTS"
FAILS=0
record() { # check result metric detail
  printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" >>"$RESULTS"
  log "$1: $2 ${3:+$3 ms }$4"
  if [ "$2" = FAIL ]; then FAILS=$((FAILS + 1)); fi
  return 0
}

FC_RUN_DIR="$(sed -n 's/^workdir *= *"\([^"]*\)".*/\1/p' "$BASE_CONFIG" | head -n1)"
case "$FC_RUN_DIR" in /*) ;; *) FC_RUN_DIR="$REPO_ROOT/${FC_RUN_DIR#./}" ;; esac
CHROOT_BASE="$(sed -n 's/^chroot_base *= *"\([^"]*\)".*/\1/p' "$BASE_CONFIG" | head -n1)"
CHROOT_BASE="${CHROOT_BASE:-/srv/jailer}"
SNAP_ROOT="$FC_RUN_DIR/_snapshots"
GATEWAY_PID=""

cleanup() {
  local rc=$?
  set +e
  if [ -n "$GATEWAY_PID" ] && kill -0 "$GATEWAY_PID" 2>/dev/null; then
    kill -TERM "$GATEWAY_PID"
    for _ in $(seq 1 60); do kill -0 "$GATEWAY_PID" 2>/dev/null || break; sleep 0.25; done
    kill -KILL "$GATEWAY_PID" 2>/dev/null
  fi
  pkill -KILL -x firecracker 2>/dev/null
  rm -rf "$SNAP_ROOT"
  if [ "${KEEP_WORK:-0}" != 1 ]; then rm -rf "$WORK"; fi
  exit "$rc"
}
trap cleanup EXIT

# --- config + gateway ---------------------------------------------------------------------------
head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' >"$WORK/snap.key"
head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' >"$WORK/snap.sign"
chmod 600 "$WORK/snap.key" "$WORK/snap.sign"
CONFIG="$OUT/gateway.toml"
sed -e "s|^listen *=.*|listen = \"127.0.0.1:$PORT\"|" \
  -e 's|^profile *=.*|profile = "dev"                 # X1 measurement run (scripts/x1/clone-e2e.sh)|' \
  -e "s|^data_dir *=.*|data_dir = \"$WORK/data\"|" \
  -e "s|^rootfs *=.*|rootfs = \"$ROOTFS\"|" \
  "$BASE_CONFIG" >"$CONFIG"
cat >>"$CONFIG" <<EOF

# Added by scripts/x1/clone-e2e.sh (PLT-4653, experimental). allow_unverified accepts the
# provider's Unverified snapshot capabilities for this measurement run only.
[snapshots]
enabled = true
allow_unverified = true
ttl_seconds = 3600
key_file = "$WORK/snap.key"
signing_key_file = "$WORK/snap.sign"
EOF
LOG_FORMAT=json "$GATEWAY_BIN" --config "$CONFIG" >"$OUT/gateway.log" 2>&1 &
GATEWAY_PID=$!
for _ in $(seq 1 240); do
  curl -fsS -m 2 -o /dev/null "$API/readyz" 2>/dev/null && break
  kill -0 "$GATEWAY_PID" 2>/dev/null || { log "gateway exited"; tail -n 30 "$OUT/gateway.log" >&2; exit 1; }
  sleep 0.5
done
curl -fsS -m 2 -o /dev/null "$API/readyz" || { log "gateway not ready"; tail -n 30 "$OUT/gateway.log" >&2; exit 1; }
export TSLS_API_URL="$API" TSLS_TOKEN="$TOKEN"

api() { # method path [json] -> body on stdout; HTTP code in $WORK/.code
  local body="${3:-}"
  if [ -n "$body" ]; then
    curl -sS -o "$WORK/.body" -w '%{http_code}' -X "$1" "$API$2" -H "authorization: Bearer $TOKEN" \
      -H 'content-type: application/json' -d "$body" >"$WORK/.code"
  else
    curl -sS -o "$WORK/.body" -w '%{http_code}' -X "$1" "$API$2" -H "authorization: Bearer $TOKEN" >"$WORK/.code"
  fi
  cat "$WORK/.body"
}

{
  echo "firecracker $(.kvm/bin/firecracker --version 2>/dev/null | head -n1)"
  echo "firecracker_sha256 $(sha256sum .kvm/bin/firecracker | awk '{print $1}')"
  echo "kernel_sha256 $(sha256sum .kvm/vmlinux | awk '{print $1}')"
  echo "rootfs_restore_sha256 $(sha256sum "$ROOTFS" | awk '{print $1}')"
  echo "bridge_sha256 $(sha256sum "$GUEST_DIR/tachyon-serverless-runtime-bridge" | awk '{print $1}')"
  echo "restore_aware_sha256 $(sha256sum "$GUEST_DIR/example-restore-aware" | awk '{print $1}')"
  echo "host_uname $(uname -srm)"
  echo "host_cpu $(grep -m1 -i -E 'model name|CPU part' /proc/cpuinfo | sed 's/.*: //')"
  echo "commit ${CLONE_COMMIT:-$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)}"
} >"$OUT/versions.txt"

api GET /v1/provider >"$OUT/provider.json"
create_status="$(jq -r '.capabilities.snapshot_create.status' "$OUT/provider.json")"
clone_status="$(jq -r '.capabilities.snapshot_clone.status' "$OUT/provider.json")"
if [ "$create_status" != unsupported ] && [ "$clone_status" != unsupported ]; then
  record provider PASS "" "snapshot_create=$create_status snapshot_clone=$clone_status (allow_unverified measurement run)"
else
  record provider FAIL "" "snapshot_create=$create_status snapshot_clone=$clone_status: $(jq -c '.capabilities.snapshot_clone' "$OUT/provider.json")"
fi

deploy() { # function policy -> function id (prints); revision json saved
  local fn="$1" policy="$2" extra=()
  if [ "$policy" != none ]; then extra=(--restore-policy "$policy" --synthetic-init-sample); fi
  "$TSLS_BIN" functions get "$fn" --json >/dev/null 2>&1 ||
    "$TSLS_BIN" functions create --name "$fn" --description "X1 clone e2e" --json >/dev/null
  "$TSLS_BIN" functions deploy --function "$fn" --binary "$GUEST_DIR/example-restore-aware" \
    --arch "$ARCH" --memory-mib 256 --cpu-millis 1000 --ephemeral-storage-mib 64 \
    --timeout-seconds 30 --init-timeout-seconds 30 --description "restore=$policy" \
    "${extra[@]}" --json >"$OUT/revision-$fn-$(ms_now).json"
  "$TSLS_BIN" functions get "$fn" --json | jq -r .id
}

# invoke function_id label -> writes invocations/<label>.json (the invocation with attempts)
# and prints "<http code> <client ms> <invocation id>"
invoke() {
  local fid="$1" label="$2" t0 t1 code id host_ms
  host_ms="$(ms_now)"
  t0="$host_ms"
  code="$(curl -sS -D "$WORK/$label.headers" -o "$WORK/$label.out" -w '%{http_code}' -X POST \
    "$API/v1/functions/$fid/invoke" -H "authorization: Bearer $TOKEN" \
    -H 'content-type: application/json' -d '{"n":97}')"
  t1="$(ms_now)"
  id="$(tr -d '\r' <"$WORK/$label.headers" | sed -n 's/^x-tachyon-invocation-id: *//Ip' | head -n1)"
  if [ -z "$id" ]; then id="$(jq -r '.error.invocation_id // empty' "$WORK/$label.out" 2>/dev/null)"; fi
  if [ -n "$id" ]; then
    curl -sS "$API/v1/invocations/$id" -H "authorization: Bearer $TOKEN" |
      jq --slurpfile out "$WORK/$label.out" --argjson host_ms "$host_ms" --argjson client_ms "$((t1 - t0))" \
        '. + {client_output: $out[0], host_invoke_ms: $host_ms, client_ms: $client_ms}' >"$OUT/invocations/$label.json"
  fi
  echo "$code $((t1 - t0)) ${id:-none}"
}

attempt() { # field-path label -> that field of the last attempt (raw)
  jq -r ".attempts[-1].$1 // empty" "$OUT/invocations/$2.json" 2>/dev/null
}

# --- cold baseline ------------------------------------------------------------------------------
COLD_FN="$(deploy x1-cold none)"
for n in $(seq 1 "$COLD_RUNS"); do
  read -r code client _ <<<"$(invoke "$COLD_FN" "cold-$n")"
  kind="$(attempt start_kind "cold-$n")"
  ready="$(jq -r '(.attempts[-1].timings.environment_boot_ms // 0) + (.attempts[-1].timings.runtime_init_ms // 0)' "$OUT/invocations/cold-$n.json")"
  if [ "$code" = 200 ] && [ "$kind" = cold ]; then
    record "cold-$n" PASS "$ready" "boot+init to Ready (client ${client} ms), start_kind=cold"
  else
    record "cold-$n" FAIL "" "HTTP $code start_kind=$kind"
  fi
done

# --- require before any snapshot ----------------------------------------------------------------
FN="$(deploy x1-restore require)"
read -r code _ _ <<<"$(invoke "$FN" require-no-snapshot)"
etype="$(jq -r '.error.error_type // .error.code // empty' "$WORK/require-no-snapshot.out")"
if [ "$code" != 200 ] && grep -q 'RestoreRequiredUnavailable' "$WORK/require-no-snapshot.out" &&
  grep -q no_snapshot "$WORK/require-no-snapshot.out"; then
  record require-no-snapshot PASS "" "HTTP $code $etype (no_snapshot); nothing booted cold"
else
  record require-no-snapshot FAIL "" "HTTP $code $(head -c 300 "$WORK/require-no-snapshot.out")"
fi

# --- snapshot -----------------------------------------------------------------------------------
t0="$(ms_now)"
api POST "/v1/functions/$FN/snapshots" '{}' >"$OUT/snapshot-1.json"
SNAP="$(jq -r '.id // empty' "$OUT/snapshot-1.json")"
if [ "$(cat "$WORK/.code")" = 201 ] && [ -n "$SNAP" ]; then
  record snapshot PASS "$(($(ms_now) - t0))" "$SNAP: $(jq -c '.timings' "$OUT/snapshot-1.json") manifest $(jq -r .manifest_digest "$OUT/snapshot-1.json")"
else
  record snapshot FAIL "" "HTTP $(cat "$WORK/.code"): $(head -c 400 "$OUT/snapshot-1.json")"
fi
{
  echo "# plaintext snapshot files on the host (root-only directory; memory/vmstate 0640 root:<jail gid>)"
  ls -la "$SNAP_ROOT/$SNAP" 2>&1
  sha256sum "$SNAP_ROOT/$SNAP"/* 2>&1
  echo "# sealed store (AES-256-GCM chunks) and signed manifest"
  ls -la "$WORK/data/snapshots/$SNAP" 2>&1
  jq '{digest, key_id, manifest: (.manifest | fromjson | del(.artifacts))}' "$WORK/data/snapshots/$SNAP/manifest.json" 2>&1
  echo "# firecracker processes after the snapshot (the source must be gone)"
  pgrep -a firecracker || echo none
} >"$OUT/snapshot-files.txt"
if pgrep -x firecracker >/dev/null; then
  record snapshot-source-gone FAIL "" "a VMM is still running after the snapshot"
else
  record snapshot-source-gone PASS "" "source VMM terminated (never resumed)"
fi

# --- two concurrent clones ----------------------------------------------------------------------
: >"$OUT/clones-maps.txt"
rm -f "$WORK/.sampler-stop"
(
  while [ ! -f "$WORK/.sampler-stop" ]; do
    for p in $(pgrep -x firecracker || true); do
      if grep -q snapshot.mem "/proc/$p/maps" 2>/dev/null; then
        echo "$(date -u +%H:%M:%S.%N | cut -c1-12) pid $p $(grep snapshot.mem "/proc/$p/maps" | head -n1)"
        awk '/^(Rss|Pss|Shared_Clean|Private_Dirty):/ {print "  " $0}' "/proc/$p/smaps_rollup" 2>/dev/null
      fi
    done
    sleep 0.1
  done
) >"$OUT/clones-maps.txt" 2>&1 &
SAMPLER=$!
invoke "$FN" clone-a >"$WORK/clone-a.res" &
PA=$!
invoke "$FN" clone-b >"$WORK/clone-b.res" &
PB=$!
wait "$PA" || true
wait "$PB" || true
touch "$WORK/.sampler-stop"
wait "$SAMPLER" 2>/dev/null || true
ok=1
details=""
INSTANCES=()
ENVS=()
for c in a b; do
  read -r code client _ <"$WORK/clone-$c.res"
  f="$OUT/invocations/clone-$c.json"
  kind="$(jq -r '.attempts[-1].start_kind' "$f" 2>/dev/null)"
  inst="$(jq -r '.client_output.instance_id // empty' "$f" 2>/dev/null)"
  marker="$(jq -r '.client_output.scratch_marker // empty' "$f" 2>/dev/null)"
  before="$(jq -r '.client_output.scratch_marker_before // empty' "$f" 2>/dev/null)"
  # The guest wall clock read in after_restore vs the host's dispatch time of the attempt
  # (second precision): the snapshot's clock would be minutes behind without the restore frame.
  skew="$(jq -r '((.client_output.started_at_ms // 0) - ((.attempts[-1].dispatched_at | sub("\\.[0-9]+"; "") | fromdateiso8601) * 1000)) | fabs | floor' "$f" 2>/dev/null)"
  env="$(jq -r '.attempts[-1].environment_id' "$f" 2>/dev/null)"
  ready="$(jq -r '.attempts[-1].boot_evidence.details.restore_ready_ms // empty' "$f" 2>/dev/null)"
  INSTANCES+=("$inst")
  ENVS+=("$env")
  details="$details clone-$c: HTTP $code kind=$kind env=$env instance=$inst ready=${ready}ms client=${client}ms marker=$marker before='$before' clock_skew=${skew}ms;"
  if [ "$code" != 200 ] || [ "$kind" != restored ] || [ -z "$inst" ] || [ "$marker" != "$inst" ] ||
    [ -n "$before" ] || [ "${skew%.*}" -gt 5000 ]; then
    ok=0
  fi
  jq -c '{clone: "'"$c"'", start_kind: .attempts[-1].start_kind, timings: .attempts[-1].timings,
    restore: (.attempts[-1].boot_evidence.details | with_entries(select(.key | test("restore|snapshot|doorbell|reconnect|scratch|load|resume|boot_ms")))),
    output: .client_output}' "$f" >>"$OUT/clones.jsonl" 2>/dev/null || true
done
if [ "$ok" = 1 ] && [ "${INSTANCES[0]}" != "${INSTANCES[1]}" ] && [ "${ENVS[0]}" != "${ENVS[1]}" ]; then
  record clones-concurrent PASS "" "$details distinct instance and environment ids"
else
  record clones-concurrent FAIL "" "$details"
fi
MAPPERS="$(grep ' pid ' "$OUT/clones-maps.txt" | awk '{print $3}' | sort -u | wc -l | tr -d ' ')"
if [ "$MAPPERS" -ge 2 ]; then
  record clones-shared-memory PASS "" "$MAPPERS VMMs mapped the one snapshot memory file (see clones-maps.txt for the mapping flags and smaps)"
elif [ "$MAPPERS" -ge 1 ]; then
  record clones-shared-memory INFO "" "$MAPPERS VMM seen mapping the snapshot memory file (the other clone was not sampled)"
else
  record clones-shared-memory INFO "" "no VMM mapping observed within the sampling window"
fi

# --- sequential restores ------------------------------------------------------------------------
for n in $(seq 1 "$RESTORE_RUNS"); do
  read -r code client _ <<<"$(invoke "$FN" "restore-$n")"
  kind="$(attempt start_kind "restore-$n")"
  ready="$(attempt boot_evidence.details.restore_ready_ms "restore-$n")"
  if [ "$code" = 200 ] && [ "$kind" = restored ]; then
    record "restore-$n" PASS "$ready" "clone start to Ready (client ${client} ms; load $(attempt boot_evidence.details.restore_load_ms "restore-$n") ms, reconnect $(attempt boot_evidence.details.restore_reconnect_ms "restore-$n") ms, verify $(attempt boot_evidence.details.restore_verify_ms "restore-$n") ms)"
  else
    record "restore-$n" FAIL "" "HTTP $code start_kind=$kind"
  fi
done

# --- a new revision does not use the old snapshot -----------------------------------------------
deploy x1-restore require >/dev/null
read -r code _ _ <<<"$(invoke "$FN" revision-stale)"
if [ "$code" != 200 ] && grep -q revision_mismatch "$WORK/revision-stale.out"; then
  record revision-stale PASS "" "HTTP $code: $(jq -r '.error.message' "$WORK/revision-stale.out" | head -c 200)"
else
  record revision-stale FAIL "" "HTTP $code $(head -c 300 "$WORK/revision-stale.out")"
fi

flip_memory_byte() { # snapshot id: flip the lowest bit of byte 4096 of the plaintext memory file
  perl -e 'open(my $f, "+<", $ARGV[0]) or die "$ARGV[0]: $!"; binmode $f; seek($f, 4096, 0);
    read($f, my $b, 1) == 1 or die "short file"; seek($f, 4096, 0); print $f chr(ord($b) ^ 1); close $f' \
    "$SNAP_ROOT/$1/memory"
}

# --- corrupted artifact, require ----------------------------------------------------------------
api POST "/v1/functions/$FN/snapshots" '{}' >"$OUT/snapshot-2.json"
SNAP2="$(jq -r '.id // empty' "$OUT/snapshot-2.json")"
flip_memory_byte "$SNAP2"
read -r code _ _ <<<"$(invoke "$FN" corrupt-require)"
api GET "/v1/functions/$FN/snapshots" >"$OUT/snapshots-after-corruption.json"
state="$(jq -r --arg s "$SNAP2" '.items[] | select(.id == $s) | .state' "$OUT/snapshots-after-corruption.json")"
if [ -n "$SNAP2" ] && [ "$code" != 200 ] && grep -q artifact_corrupted "$WORK/corrupt-require.out" && [ "$state" = quarantined ]; then
  record corrupt-require PASS "" "HTTP $code Host.RestoreRequiredUnavailable artifact_corrupted; $SNAP2 quarantined"
else
  record corrupt-require FAIL "" "snapshot=$SNAP2 HTTP $code state=$state $(head -c 300 "$WORK/corrupt-require.out")"
fi

# --- corrupted artifact, prefer -----------------------------------------------------------------
PFN="$(deploy x1-prefer prefer)"
api POST "/v1/functions/$PFN/snapshots" '{}' >"$OUT/snapshot-3.json"
SNAP3="$(jq -r '.id // empty' "$OUT/snapshot-3.json")"
read -r code _ _ <<<"$(invoke "$PFN" prefer-restored)"
kind="$(attempt start_kind prefer-restored)"
if [ "$code" = 200 ] && [ "$kind" = restored ]; then
  record prefer-restored PASS "$(attempt boot_evidence.details.restore_ready_ms prefer-restored)" "prefer with an intact snapshot restores"
else
  record prefer-restored FAIL "" "HTTP $code start_kind=$kind"
fi
flip_memory_byte "$SNAP3"
read -r code _ _ <<<"$(invoke "$PFN" corrupt-prefer)"
kind="$(attempt start_kind corrupt-prefer)"
fallback="$(attempt boot_evidence.details.restore_fallback corrupt-prefer)"
if [ "$code" = 200 ] && [ "$kind" = cold ] && [ "$fallback" = artifact_corrupted ]; then
  record corrupt-prefer PASS "" "HTTP 200 start_kind=cold restore_fallback=artifact_corrupted (never counted as restored)"
else
  record corrupt-prefer FAIL "" "HTTP $code start_kind=$kind fallback=$fallback"
fi

# --- cold vs restored ---------------------------------------------------------------------------
median() { sort -n | awk '{a[NR]=$1} END {if (NR==0) print ""; else if (NR%2) print a[(NR+1)/2]; else print int((a[NR/2]+a[NR/2+1])/2)}'; }
cold_med="$(awk -F'\t' '$1 ~ /^cold-/ && $2 == "PASS" {print $3}' "$RESULTS" | median)"
restore_med="$(awk -F'\t' '$1 ~ /^restore-/ && $2 == "PASS" {print $3}' "$RESULTS" | median)"
record latency INFO "" "median cold boot+init ${cold_med:-?} ms vs restored clone-start-to-Ready ${restore_med:-?} ms (nested virtualization, reference only)"

# --- cleanup and leftovers ----------------------------------------------------------------------
kill -TERM "$GATEWAY_PID" 2>/dev/null || true
for _ in $(seq 1 80); do kill -0 "$GATEWAY_PID" 2>/dev/null || break; sleep 0.25; done
GATEWAY_PID=""
rm -rf "$SNAP_ROOT" "$WORK/data/snapshots"
{
  echo "# leftovers after the run (gateway stopped, snapshots removed)"
  echo "firecracker: $(pgrep -a firecracker || echo none)"
  echo "jails: $(find "$CHROOT_BASE/firecracker" -mindepth 1 -maxdepth 1 2>/dev/null | tr '\n' ' ' || true)"
  echo "cgroups: $(find /sys/fs/cgroup/tachyon -mindepth 1 -maxdepth 1 -type d 2>/dev/null | tr '\n' ' ' || true)"
  echo "env dirs: $(find "$FC_RUN_DIR" -mindepth 1 -maxdepth 1 -name 'env_*' 2>/dev/null | tr '\n' ' ' || true)"
  echo "snapshot dirs: $(find "$SNAP_ROOT" -mindepth 1 -maxdepth 1 2>/dev/null | tr '\n' ' ' || true)"
} >"$OUT/leftovers.txt"
if ! pgrep -x firecracker >/dev/null && [ -z "$(find "$CHROOT_BASE/firecracker" -mindepth 1 -maxdepth 1 2>/dev/null)" ] &&
  [ -z "$(find /sys/fs/cgroup/tachyon -mindepth 1 -maxdepth 1 -type d 2>/dev/null)" ] &&
  [ -z "$(find "$FC_RUN_DIR" -mindepth 1 -maxdepth 1 -name 'env_*' 2>/dev/null)" ]; then
  record cleanup PASS "" "no firecracker, jail, cgroup, env dir or snapshot left"
else
  record cleanup FAIL "" "see leftovers.txt"
fi

jq -R -s 'split("\n") | map(select(length > 0) | split("\t")) | .[1:] |
  map({check: .[0], result: .[1], metric_ms: (.[2] | tonumber? // null), detail: .[3]})' \
  "$RESULTS" >"$OUT/summary.json"
log "results: $RESULTS (failures: $FAILS)"
[ "$FAILS" = 0 ]
