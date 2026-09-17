#!/usr/bin/env bash
# scripts/lab/lab.sh - one entry command for a throwaway Tachyon Serverless lab (PLT-4648).
#
# Usage:
#   scripts/lab/lab.sh [--lab-dir DIR] [--provider process|firecracker] <command> [options]
#
# Commands:
#   preflight            host checks (OS/arch, KVM, disk/memory, tools, pins, ports, privileges)
#   bootstrap [--console] [--skip-build]
#                        pinned downloads with sha256 (nats-server; Firecracker, jailer, guest kernel
#                        on Linux/KVM), cargo build, musl guest + rootfs (firecracker). Idempotent.
#   up [--console]       generate config + throwaway secrets (0600), start nats-server and the gateway
#                        (migrations run at start), then health-check every component
#   demo p1|p2|p3|all    P1 sync invoke/logs/rollback, P2 burst/zero/alias/metrics, P3 async/DLQ/
#                        redrive/cron/webhook/usage/budget stop. Exit 0 only when every check passed
#   status               process + health table of every component (exit 1 when a check fails)
#   logs [gateway|nats|commands|last]
#   down                 stop the gateway and nats-server (data kept)
#   teardown [--dry-run] [--keep-cache] [--purge]
#                        remove only what this lab owns, then an orphan check that prints `clean`
#                        or the leftovers (exit 1)
#
# Options / environment:
#   --lab-dir DIR   (LAB_DIR)       default <repo>/.lab. Everything the lab creates is inside it,
#                                   except cgroups (/sys/fs/cgroup/tsls-<lab id>) on Linux/KVM.
#   --provider P    (LAB_PROVIDER)  process (default; NOT a microVM, no isolation) | firecracker.
#                                   Recorded in the manifest at the first bootstrap/up.
#   LAB_FC_PRIVILEGED=1|0           firecracker only: 1 (default) runs the gateway as root through
#                                   `sudo -n` with jailer + cgroup required (profile production);
#                                   0 runs it unprivileged (profile dev, no jailer, cgroup best-effort).
#
# Every command writes a timestamped transcript to <lab>/logs/commands/<UTC>-<command>.log.
# Docs: docs/runbook.md. Pins: deploy/lab/versions.lock. No Docker. No cloud resources.
set -euo pipefail

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
# shellcheck source=scripts/lab/lib.sh
. "$(dirname "$SCRIPT_PATH")/lib.sh"

usage() { sed -n '2,31p' "$SCRIPT_PATH" | sed 's/^# \{0,1\}//'; }

# ---------------------------------------------------------------------------
# argument parsing
# ---------------------------------------------------------------------------

ORIG_ARGS=("$@")
OPT_PROVIDER="${LAB_PROVIDER:-}"
LAB_DIR="${LAB_DIR:-$REPO_ROOT/.lab}"
while [ $# -gt 0 ]; do
  case "$1" in
    --lab-dir) LAB_DIR="$2"; shift 2 ;;
    --lab-dir=*) LAB_DIR="${1#--lab-dir=}"; shift ;;
    --provider) OPT_PROVIDER="$2"; shift 2 ;;
    --provider=*) OPT_PROVIDER="${1#--provider=}"; shift ;;
    -h | --help | help) usage; exit 0 ;;
    -*) echo "unknown option: $1 (see --help)" >&2; exit 2 ;;
    *) break ;;
  esac
done
[ $# -gt 0 ] || { usage; exit 2; }
COMMAND="$1"
shift
case "$COMMAND" in
  preflight | bootstrap | up | demo | status | logs | down | teardown) ;;
  *) echo "unknown command: $COMMAND (see --help)" >&2; exit 2 ;;
esac

case "$LAB_DIR" in /*) ;; *) LAB_DIR="$PWD/$LAB_DIR" ;; esac
LAB_DIR="${LAB_DIR%/}"
case "$LAB_DIR" in
  "" | / | "$HOME" | "$REPO_ROOT" | /tmp | /private/tmp | /var | /usr | /etc)
    echo "refusing lab directory '$LAB_DIR' (use a dedicated directory such as <repo>/.lab)" >&2
    exit 2 ;;
esac
export LAB_DIR
lab_paths

# ---------------------------------------------------------------------------
# transcript: re-run this command with its output stamped into logs/commands/
# ---------------------------------------------------------------------------

if [ "${LAB_TRANSCRIPT:-}" = "" ] && [ "$COMMAND" != logs ]; then
  if [ -d "$LAB_DIR" ] && [ ! -f "$MARKER" ] && [ -n "$(foreign_content "$LAB_DIR")" ]; then
    echo "refusing: $LAB_DIR exists, is not empty and is not a lab (no .tsls-lab marker)" >&2
    exit 2
  fi
  purge=false
  for a in "$@"; do [ "$a" = --purge ] && purge=true; done
  if [ "$COMMAND" = teardown ] && [ "$purge" = true ]; then
    # The lab directory itself is removed: keep the transcript next to the system temp files.
    transcript="${TMPDIR:-/tmp}/tsls-lab-teardown-$(lab_stamp)-$$.log"
  else
    mkdir -p "$CMD_LOG_DIR"
    transcript="$CMD_LOG_DIR/$(lab_stamp)-$COMMAND-$$.log"
  fi
  {
    echo "# tachyon-serverless lab transcript"
    echo "# command:  scripts/lab/lab.sh ${ORIG_ARGS[*]}"
    echo "# started:  $(lab_now)"
    echo "# lab_dir:  $LAB_DIR"
    echo "# lab_id:   $(cat "$MARKER" 2>/dev/null || echo '(new)')"
    echo "# host:     $(uname -srm) user=$(id -un)"
    echo "# git:      $(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)$(git -C "$REPO_ROOT" diff --quiet 2>/dev/null || echo ' (dirty)')"
  } >>"$transcript"
  set +e
  LAB_TRANSCRIPT="$transcript" "$SCRIPT_PATH" "${ORIG_ARGS[@]}" 2>&1 | {
    if have perl; then
      perl -MPOSIX=strftime -e '$|=1; open(my $f, ">>", $ARGV[0]) or die; select((select($f), $|=1)[0]);
        while (<STDIN>) { print; print $f strftime("%Y-%m-%dT%H:%M:%SZ ", gmtime), $_; }' "$transcript"
    else
      while IFS= read -r line; do
        printf '%s\n' "$line"
        printf '%s %s\n' "$(lab_now)" "$line" >>"$transcript"
      done
    fi
  }
  rc=${PIPESTATUS[0]}
  echo "# finished: $(lab_now) exit=$rc" >>"$transcript"
  echo "[lab] transcript: $transcript (exit $rc)"
  exit "$rc"
fi

# ---------------------------------------------------------------------------
# preflight
# ---------------------------------------------------------------------------

PF_FAILED=0
PF_WARNED=0
row() { # STATUS NAME DETAIL
  printf '%-5s %-26s %s\n' "$1" "$2" "$3"
  case "$1" in FAIL) PF_FAILED=1 ;; WARN) PF_WARNED=1 ;; esac
}

tool_row() { # NAME REQUIRED(1|0) VERSION_CMD... -> prints version
  local name="$1" required="$2" v
  shift 2
  if have "$name"; then
    v="$("$@" 2>&1 | head -n 1 || true)"
    row ok "tool:$name" "$(command -v "$name") ($v)"
  elif [ "$required" = 1 ]; then
    row FAIL "tool:$name" "not found in PATH"
  else
    row skip "tool:$name" "not found (optional)"
  fi
}

cmd_preflight() {
  lab_init
  lab_provider
  lab_binaries
  local os arch v free need mem
  os="$(host_os)"
  arch="$(host_arch)"
  echo "lab preflight: provider=$PROVIDER lab_dir=$LAB_DIR lab_id=$LAB_ID"
  echo
  # --- OS / arch -----------------------------------------------------------
  case "$os" in
    Linux) row ok os "Linux $(uname -r)" ;;
    Darwin)
      if [ "$PROVIDER" = firecracker ]; then
        row FAIL os "macOS cannot run Firecracker; use --provider process or the Lima VM (docs/kvm.md §5)"
      else
        row WARN os "macOS: process provider only (dev mode, NOT a microVM, no isolation)"
      fi ;;
    *) row FAIL os "$os (Linux or macOS required)" ;;
  esac
  case "$arch" in
    aarch64 | x86_64) row ok arch "$arch" ;;
    *) row FAIL arch "$arch (aarch64 or x86_64 required)" ;;
  esac
  if [ "$os" = Linux ] && [ "$PROVIDER" = firecracker ] && [ "$arch" = x86_64 ]; then
    row WARN arch_verified "x86_64 has never run this prototype (only aarch64 nested virt); expect gaps"
  fi
  # --- KVM -----------------------------------------------------------------
  if [ "$PROVIDER" = firecracker ]; then
    if [ -e /dev/kvm ]; then
      if [ -r /dev/kvm ] && [ -w /dev/kvm ]; then
        row ok kvm "/dev/kvm is rw for $(id -un)"
      else
        row FAIL kvm "/dev/kvm not rw for $(id -un): sudo usermod -aG kvm $(id -un) and re-login (Lima: sudo chmod 666 /dev/kvm after each start)"
      fi
    else
      row FAIL kvm "/dev/kvm missing: enable KVM (bare metal) or nested virtualization (VM)"
    fi
    if [ -r /sys/module/kvm_intel/parameters/nested ] || [ -r /sys/module/kvm_amd/parameters/nested ]; then
      row ok nested_virt "host exposes the nested parameter (only matters when this Linux runs VMs inside VMs)"
    elif systemd-detect-virt >/dev/null 2>&1 && [ "$(systemd-detect-virt 2>/dev/null)" != none ]; then
      row WARN nested_virt "running inside $(systemd-detect-virt 2>/dev/null): nested virtualization; boot times are reference values only"
    else
      row ok nested_virt "no hypervisor detected (bare metal or undetectable)"
    fi
    v="$(uname -r)"
    if version_ge "${v%%-*}" "$MIN_LINUX_KERNEL"; then row ok kernel "$v"; else row FAIL kernel "$v < $MIN_LINUX_KERNEL"; fi
    if [ -f /sys/fs/cgroup/cgroup.controllers ]; then
      row ok cgroup_v2 "$(tr '\n' ' ' </sys/fs/cgroup/cgroup.controllers)"
    else
      row FAIL cgroup_v2 "/sys/fs/cgroup is not cgroup v2 (jailer/cgroup limits need it)"
    fi
  fi
  # --- resources -------------------------------------------------------------
  need="$MIN_FREE_DISK_MIB_PROCESS"
  [ "$PROVIDER" = firecracker ] && need="$MIN_FREE_DISK_MIB_FIRECRACKER"
  free="$(free_mib "$LAB_DIR")"
  if [ "$free" -ge "$need" ]; then row ok disk_lab "${free} MiB free for $LAB_DIR (need $need)"; else row FAIL disk_lab "${free} MiB free for $LAB_DIR < $need MiB"; fi
  free="$(free_mib "$REPO_ROOT/target")"
  if [ "$free" -ge 8192 ]; then row ok disk_build "${free} MiB free for target/ (cargo build needs ~8 GiB)"; else row WARN disk_build "${free} MiB free for target/ (cargo build needs ~8 GiB)"; fi
  mem="$(total_memory_mib)"
  if [ "$mem" -ge "$MIN_MEMORY_MIB" ]; then row ok memory "${mem} MiB"; else row WARN memory "${mem} MiB < $MIN_MEMORY_MIB MiB"; fi
  # --- tools -----------------------------------------------------------------
  tool_row bash 1 bash --version
  tool_row curl 1 curl --version
  tool_row jq 1 jq --version
  if have jq && ! version_ge "$(jq --version | sed 's/^jq-//')" "$MIN_JQ"; then row FAIL jq_version "jq >= $MIN_JQ required"; fi
  tool_row python3 1 python3 --version
  if have python3 && ! version_ge "$(python3 --version 2>&1 | awk '{print $2}')" "$MIN_PYTHON3"; then row FAIL python3_version "python3 >= $MIN_PYTHON3 required"; fi
  tool_row openssl 1 openssl version
  tool_row tar 1 tar --version
  tool_row git 1 git --version
  tool_row cargo 1 cargo --version
  tool_row perl 0 perl -e 'print "perl $]"'
  tool_row mise 0 mise --version
  if have shasum || have sha256sum; then row ok tool:sha256 "$(command -v sha256sum || command -v shasum)"; else row FAIL tool:sha256 "sha256sum or shasum required"; fi
  # --- pins ------------------------------------------------------------------
  v="$(sed -n 's/^channel *= *"\(.*\)"/\1/p' "$REPO_ROOT/rust-toolchain.toml")"
  if [ "$v" = "$RUST_TOOLCHAIN" ]; then row ok pin:rust "rust-toolchain.toml $v = versions.lock"; else row FAIL pin:rust "rust-toolchain.toml $v != versions.lock $RUST_TOOLCHAIN"; fi
  if have rustc; then
    v="$(cd "$REPO_ROOT" && rustc --version 2>/dev/null | awk '{print $2}')"
    if [ "$v" = "$RUST_TOOLCHAIN" ]; then row ok rustc "$v (from the repository toolchain file)"; else row FAIL rustc "rustc in $REPO_ROOT is '$v', want $RUST_TOOLCHAIN (rustup reads rust-toolchain.toml; or: mise install)"; fi
  else
    row FAIL rustc "rustc not found (install rustup, or: mise install)"
  fi
  local nats_env
  nats_env="$(grep -E '^NATS_SERVER_(VERSION|SHA256_)' "$REPO_ROOT/deploy/nats/versions.env" | sort)"
  if [ "$nats_env" = "$(grep -E '^NATS_SERVER_(VERSION|SHA256_)' "$LOCK_FILE" | sort)" ]; then
    row ok pin:nats "versions.lock = deploy/nats/versions.env ($NATS_SERVER_VERSION)"
  else
    row FAIL pin:nats "versions.lock and deploy/nats/versions.env disagree"
  fi
  if [ "$PROVIDER" = firecracker ]; then
    local fsha ksha
    fsha="$(eval "printf '%s' \"\${FIRECRACKER_TGZ_SHA256_$arch:-}\"")"
    ksha="$(eval "printf '%s' \"\${GUEST_KERNEL_SHA256_$arch:-}\"")"
    if [ -n "$fsha" ] && [ -n "$ksha" ]; then row ok pin:firecracker "$FIRECRACKER_VERSION + kernel pinned for $arch"; else row FAIL pin:firecracker "no pin for $arch in versions.lock"; fi
    tool_row rustup 1 rustup --version
    tool_row cc 1 cc --version
    tool_row mkfs.ext4 1 sh -c 'mkfs.ext4 -V 2>&1'
    if have mkfs.ext4; then
      v="$(mkfs.ext4 -V 2>&1 | head -n 1 | awk '{print $2}')"
      version_ge "$v" "$MIN_E2FSPROGS" || row FAIL e2fsprogs "$v < $MIN_E2FSPROGS (mkfs.ext4 -d)"
    fi
    tool_row ip 0 ip -V
    tool_row nft 0 nft --version
    v="$FC_RUN_DIR/env_00000000000000000000000000/v.sock_5000"
    if [ "${#v}" -le 107 ]; then row ok socket_path "${#v} bytes (max 107)"; else row FAIL socket_path "${#v} bytes > 107: use a shorter --lab-dir"; fi
  fi
  # --- privileges ------------------------------------------------------------
  if [ "$PROVIDER" = firecracker ] && [ "${LAB_FC_PRIVILEGED:-1}" = 1 ]; then
    if [ "$(id -u)" = 0 ]; then
      row ok privileges "running as root"
    elif have sudo && sudo -n true 2>/dev/null; then
      row ok privileges "passwordless sudo: gateway runs as root (jailer chroot/uid drop, cgroup v2, tap/nft cleanup)"
    else
      row FAIL privileges "needs root or passwordless sudo (jailer, cgroup, tap/nft). Or LAB_FC_PRIVILEGED=0 (no jailer, limits unverified)"
    fi
  else
    row ok privileges "no root needed (process provider / unprivileged firecracker)"
  fi
  # --- ports -----------------------------------------------------------------
  local p name
  for name in GATEWAY_PORT NATS_PORT NATS_HTTP_PORT; do
    p="$(manifest_get "$name")"
    if [ -z "$p" ]; then
      row ok "port:$name" "not assigned yet (up picks a free 127.0.0.1 port)"
    elif port_free "$p"; then
      row ok "port:$name" "127.0.0.1:$p free"
    elif [ -n "$(pid_alive "$GATEWAY_PID_FILE" || true)" ] || [ -n "$(pid_alive "$NATS_DIR/nats-server.pid" || true)" ]; then
      row ok "port:$name" "127.0.0.1:$p held by this lab"
    else
      row FAIL "port:$name" "127.0.0.1:$p is used by another process (lab.sh teardown, or edit ${name} in $MANIFEST)"
    fi
  done
  # --- network egress for pinned downloads -------------------------------------
  if [ -x "$NATS_BIN_DIR/nats-server-$NATS_SERVER_VERSION-$(nats_platform)/nats-server" ] \
    && { [ "$PROVIDER" = process ] || [ -f "$KVM_CACHE/vmlinux" ]; }; then
    row ok network "pinned downloads already cached in $CACHE_DIR"
  elif curl -fsSI --max-time 10 "https://github.com/nats-io/nats-server/releases/download/$NATS_SERVER_VERSION/SHA256SUMS" >/dev/null 2>&1; then
    row ok network "github.com releases reachable (nats-server$([ "$PROVIDER" = firecracker ] && echo ', firecracker'))"
    if [ "$PROVIDER" = firecracker ]; then
      if curl -fsSI --max-time 10 "$GUEST_KERNEL_BASE_URL/$(eval "printf '%s' \"\${GUEST_KERNEL_KEY_$arch:-}\"")" >/dev/null 2>&1; then
        row ok network:kernel "S3 guest kernel reachable"
      else
        row FAIL network:kernel "cannot reach $GUEST_KERNEL_BASE_URL (guest kernel download)"
      fi
    fi
  else
    row FAIL network "cannot reach github.com releases (pinned nats-server download); crates.io is needed by cargo too"
  fi
  echo
  if [ "$PF_FAILED" -eq 0 ]; then
    [ "$PF_WARNED" -eq 0 ] || echo "READY with warnings (read the WARN rows)"
    [ "$PF_WARNED" -ne 0 ] || echo "READY"
    echo "next: scripts/lab/lab.sh${OPT_PROVIDER:+ --provider $OPT_PROVIDER} bootstrap"
    return 0
  fi
  echo "NOT READY: fix the FAIL rows (docs/runbook.md §6)"
  return 1
}

nats_platform() {
  local os arch
  case "$(host_os)" in Darwin) os=darwin ;; *) os=linux ;; esac
  case "$(host_arch)" in aarch64) arch=arm64 ;; *) arch=amd64 ;; esac
  printf '%s-%s\n' "$os" "$arch"
}

# ---------------------------------------------------------------------------
# bootstrap
# ---------------------------------------------------------------------------

# fetch_pinned URL DEST SHA256 : download to DEST unless it already has SHA256; refuse a mismatch.
fetch_pinned() {
  local url="$1" dest="$2" want="$3" got tmp
  if [ -f "$dest" ] && [ "$(sha256_of "$dest")" = "$want" ]; then
    lab_log "cached and verified: $dest"
    return 0
  fi
  mkdir -p "$(dirname "$dest")"
  tmp="$dest.download.$$"
  lab_log "downloading $url"
  if ! curl -fSL --retry 3 --connect-timeout 20 -o "$tmp" "$url"; then
    rm -f "$tmp"
    lab_die "download failed: $url (network egress to this host is required; retry, or see docs/runbook.md §6.1)"
  fi
  got="$(sha256_of "$tmp")"
  if [ "$got" != "$want" ]; then
    rm -f "$tmp"
    lab_die "sha256 mismatch for $url: expected $want, got $got. The file was deleted and is NOT used (docs/runbook.md §6.1)"
  fi
  mv "$tmp" "$dest"
  lab_log "sha256 verified: $(basename "$dest") $got"
}

cmd_bootstrap() {
  local console=false skip_build=false a
  for a in "$@"; do
    case "$a" in
      --console) console=true ;;
      --skip-build) skip_build=true ;;
      *) lab_die "unknown bootstrap option: $a" ;;
    esac
  done
  lab_init
  lab_provider
  manifest_set PROVIDER "$PROVIDER"
  lab_binaries
  [ "$ARCH" = aarch64 ] || [ "$ARCH" = x86_64 ] || lab_die "unsupported architecture $ARCH"
  if [ "$PROVIDER" = firecracker ] && [ "$(host_os)" != Linux ]; then
    lab_die "--provider firecracker needs Linux/KVM (on macOS: docs/kvm.md §5 Lima VM, or --provider process)"
  fi
  lab_log "lab $LAB_ID provider=$PROVIDER arch=$ARCH"

  # 1. nats-server (pinned, verified by scripts/queue/lib.sh against deploy/nats/versions.env)
  lab_log "step 1/4: nats-server $NATS_SERVER_VERSION"
  local nats_bin
  nats_bin="$(QUEUE_BIN_DIR="$NATS_BIN_DIR" QUEUE_STATE_DIR="$NATS_DIR" bash -c '. "$1/scripts/queue/lib.sh"; queue_binary' _ "$REPO_ROOT")" \
    || lab_die "nats-server download or checksum failed (docs/runbook.md §6.1)"
  manifest_set NATS_BINARY "$nats_bin"
  manifest_set NATS_TARBALL_SHA256 "$(sha256_of "$NATS_BIN_DIR/nats-server-$NATS_SERVER_VERSION-$(nats_platform).tar.gz")"
  lab_log "nats-server: $("$nats_bin" --version 2>&1 | head -n 1)"

  # 2. Firecracker, jailer, guest kernel (Linux/KVM)
  if [ "$PROVIDER" = firecracker ]; then
    lab_log "step 2/4: Firecracker $FIRECRACKER_VERSION + jailer + guest kernel (pinned sha256)"
    local tgz fsha key ksha rel
    tgz="firecracker-$FIRECRACKER_VERSION-$ARCH.tgz"
    fsha="$(eval "printf '%s' \"\${FIRECRACKER_TGZ_SHA256_$ARCH}\"")"
    fetch_pinned "https://github.com/firecracker-microvm/firecracker/releases/download/$FIRECRACKER_VERSION/$tgz" "$KVM_CACHE/dl/$tgz" "$fsha"
    rel="$KVM_CACHE/dl/release-$FIRECRACKER_VERSION-$ARCH"
    if [ ! -x "$FC_BIN" ] || ! "$FC_BIN" --version 2>/dev/null | head -n 1 | grep -q "$FIRECRACKER_VERSION\$"; then
      rm -rf "$rel"
      tar -xzf "$KVM_CACHE/dl/$tgz" -C "$KVM_CACHE/dl"
      mkdir -p "$KVM_CACHE/bin"
      install -m 0755 "$rel/firecracker-$FIRECRACKER_VERSION-$ARCH" "$FC_BIN"
      install -m 0755 "$rel/jailer-$FIRECRACKER_VERSION-$ARCH" "$JAILER_BIN"
    fi
    lab_log "$("$FC_BIN" --version | head -n 1); $("$JAILER_BIN" --version | head -n 1)"
    key="$(eval "printf '%s' \"\${GUEST_KERNEL_KEY_$ARCH}\"")"
    ksha="$(eval "printf '%s' \"\${GUEST_KERNEL_SHA256_$ARCH}\"")"
    fetch_pinned "$GUEST_KERNEL_BASE_URL/$key" "$FC_KERNEL" "$ksha"
    manifest_set FIRECRACKER_TGZ_SHA256 "$fsha"
    manifest_set FIRECRACKER_BINARY_SHA256 "$(sha256_of "$FC_BIN")"
    manifest_set GUEST_KERNEL "$key"
    manifest_set GUEST_KERNEL_SHA256 "$ksha"
  else
    lab_log "step 2/4: skipped (process provider: no Firecracker, no kernel)"
  fi

  # 3. build
  if [ "$skip_build" = true ]; then
    lab_log "step 3/4: --skip-build"
  else
    lab_log "step 3/4: cargo build (toolchain $RUST_TOOLCHAIN from rust-toolchain.toml)"
    (cd "$REPO_ROOT" && lab_cmd cargo build -p tachyon-serverless-gateway -p tachyon-serverless-cli)
    if [ "$PROVIDER" = firecracker ]; then
      (cd "$REPO_ROOT" && lab_cmd rustup target add "$ARCH-unknown-linux-musl")
      (cd "$REPO_ROOT" && lab_cmd cargo build --release --target "$ARCH-unknown-linux-musl" \
        -p tachyon-serverless-runtime-bridge -p example-hello -p example-http-axum -p example-cpu-burn)
    else
      (cd "$REPO_ROOT" && lab_cmd cargo build -p tachyon-serverless-runtime-bridge \
        -p example-hello -p example-http-axum -p example-cpu-burn -p example-idempotent-async)
    fi
  fi
  local b missing=0
  for b in "$GATEWAY_BIN" "$TSLS_BIN" "$BRIDGE_BIN" "$GUEST_DIR/example-hello" "$GUEST_DIR/example-http-axum" "$GUEST_DIR/example-cpu-burn"; do
    [ -x "$b" ] || { lab_warn "missing binary: $b"; missing=1; }
  done
  if [ "$PROVIDER" = process ]; then
    [ -x "$GUEST_DIR/example-idempotent-async" ] || { lab_warn "missing binary: $GUEST_DIR/example-idempotent-async"; missing=1; }
  fi
  [ "$missing" = 0 ] || lab_die "binaries missing (run bootstrap without --skip-build)"

  # 4. rootfs (firecracker) / console
  if [ "$PROVIDER" = firecracker ]; then
    lab_log "step 4/4: base rootfs from this checkout's musl bridge"
    lab_cmd env BRIDGE_BIN="$BRIDGE_BIN" ROOTFS="$FC_ROOTFS" ROOTFS_SIZE="$ROOTFS_SIZE" "$REPO_ROOT/scripts/kvm/build-rootfs.sh"
    manifest_set ROOTFS_SHA256 "$(sha256_of "$FC_ROOTFS")"
  else
    lab_log "step 4/4: skipped (process provider: no rootfs)"
  fi
  if [ "$console" = true ]; then
    lab_log "console: pnpm $CONSOLE_PNPM_VERSION / node $CONSOLE_NODE_VERSION (apps/console/mise.toml), frozen lockfile"
    have pnpm || lab_die "pnpm not found: install node $CONSOLE_NODE_VERSION + pnpm $CONSOLE_PNPM_VERSION (cd apps/console && mise install)"
    v="$(cd "$REPO_ROOT/apps/console" || exit 0; pnpm --version 2>/dev/null)" || v=""
    [ "$v" = "$CONSOLE_PNPM_VERSION" ] || lab_warn "pnpm is $v, pinned $CONSOLE_PNPM_VERSION"
    (cd "$REPO_ROOT/apps/console" && lab_cmd pnpm install --frozen-lockfile && lab_cmd pnpm build)
    [ -f "$REPO_ROOT/apps/console/out/index.html" ] || lab_die "console build produced no out/index.html"
    manifest_set CONSOLE_BUILT 1
  fi
  manifest_set BOOTSTRAPPED_AT "$(lab_now)"
  manifest_set GIT_COMMIT "$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
  [ "$(manifest_get STATE)" = up ] || manifest_set STATE bootstrapped
  echo
  lab_log "bootstrap complete. next: scripts/lab/lab.sh up"
}

# ---------------------------------------------------------------------------
# up
# ---------------------------------------------------------------------------

LAB_SUDO=""
lab_sudo_setup() {
  LAB_SUDO=""
  if [ "$PROVIDER" = firecracker ] && [ "${LAB_FC_PRIVILEGED:-1}" = 1 ] && [ "$(id -u)" != 0 ]; then
    sudo -n true 2>/dev/null || lab_die "firecracker (privileged) needs passwordless sudo; or LAB_FC_PRIVILEGED=0"
    LAB_SUDO="sudo -n"
  fi
}

write_secrets() {
  umask 077
  mkdir -p "$SECRETS_DIR"
  chmod 700 "$SECRETS_DIR"
  if [ ! -s "$TOKENS_FILE" ]; then
    {
      echo "# Generated by scripts/lab/lab.sh up for lab $(cat "$MARKER"). Throwaway values, never reuse."
      echo "TENANT_A=tn_01hzzzzzzzzzzzzzzzzzzzzzza"
      echo "TENANT_B=tn_01hzzzzzzzzzzzzzzzzzzzzzzb"
      echo "TOKEN_A=lab-a-$(random_hex 16)"
      echo "TOKEN_A_ONCALL=lab-a-oncall-$(random_hex 16)"
      echo "TOKEN_B=lab-b-$(random_hex 16)"
      echo "METRICS_TOKEN=lab-metrics-$(random_hex 16)"
      echo "DEMO_SECRET_VALUE=lab-secret-$(random_hex 16)"
    } >"$TOKENS_FILE"
    lab_log "generated throwaway tokens and demo secret: $TOKENS_FILE (0600)"
  fi
  [ -s "$OBJECT_KEY" ] || { random_hex 32 >"$OBJECT_KEY"; lab_log "generated object store key: $OBJECT_KEY (0600)"; }
  [ -s "$TRIGGERS_KEY" ] || { random_hex 32 >"$TRIGGERS_KEY"; lab_log "generated trigger sealing key: $TRIGGERS_KEY (0600)"; }
  chmod 600 "$TOKENS_FILE" "$OBJECT_KEY" "$TRIGGERS_KEY"
  umask 022
}

write_budgets_default() {
  cat >"$BUDGET_FILE" <<EOF
# Budget file of lab $LAB_ID (re-read by the gateway at every configuration publication).
# Provisional amounts in micro-units of the dev price table: nothing is billed (billing disabled).
# Every tenant without an entry gets this default, so no tenant is refused as Host.BudgetUnknown.
[default_tenant]
hard_limit_micros = 1000000000000
EOF
}

write_config() {
  local port="$1" nats_port="$2" console="$3"
  # shellcheck disable=SC1090
  . "$TOKENS_FILE"
  mkdir -p "$CONFIG_DIR" "$DATA_DIR" "$RUN_DIR" "$DEMO_DIR"
  [ -f "$BUDGET_FILE" ] || write_budgets_default
  umask 077
  {
    echo "# Gateway config of lab $LAB_ID, generated by scripts/lab/lab.sh up at $(lab_now)."
    echo "# Contains throwaway credentials (mode 0600). Regenerated by every 'up'; do not edit by hand."
    echo "listen = \"127.0.0.1:$port\""
    if [ "$PROVIDER" = firecracker ] && [ "${LAB_FC_PRIVILEGED:-1}" = 1 ]; then
      echo 'profile = "production"'
    else
      echo 'profile = "dev"'
    fi
    echo "data_dir = \"$DATA_DIR\""
    echo
    echo '[provider]'
    echo "kind = \"$PROVIDER\""
    echo
    if [ "$PROVIDER" = firecracker ]; then
      mkdir -p "$FC_RUN_DIR" "$FC_JAIL_DIR"
      echo '[provider.firecracker]'
      echo "firecracker_binary = \"$FC_BIN\""
      echo "kernel = \"$FC_KERNEL\""
      echo "rootfs = \"$FC_ROOTFS\""
      echo "workdir = \"$FC_RUN_DIR\""
      echo 'vsock_port = 5000'
      echo
      echo '[provider.firecracker.cgroup]'
      if [ "${LAB_FC_PRIVILEGED:-1}" = 1 ]; then echo 'mode = "required"'; else echo 'mode = "best-effort"'; fi
      echo "parent = \"tsls-$LAB_ID\""
      echo
      if [ "${LAB_FC_PRIVILEGED:-1}" = 1 ]; then
        echo '[provider.firecracker.jailer]'
        echo 'enabled = true'
        echo "binary = \"$JAILER_BIN\""
        echo 'uid = 64000'
        echo 'gid = 64000'
        echo "chroot_base = \"$FC_JAIL_DIR\""
        echo
      fi
    else
      mkdir -p "$DATA_DIR/process"
      echo '[provider.process]'
      echo "bridge_binary = \"$BRIDGE_BIN\""
      echo "workdir = \"$DATA_DIR/process\""
      echo
    fi
    echo '[capacity]'
    echo 'max_concurrency = 8'
    echo 'max_queue = 32'
    echo 'queue_timeout_seconds = 60'
    echo
    echo '[capacity.node]'
    echo "name = \"$LAB_ID\""
    echo '# A placement LABEL. It routes jp-only revisions to this node; it proves nothing about where data is.'
    echo 'region = "jp"'
    if [ "$PROVIDER" = firecracker ]; then
      echo 'vmm_overhead_memory_mib = 64'
      echo 'bridge_overhead_memory_mib = 0'
    fi
    echo
    echo '[scaling]'
    echo 'reconcile_interval_ms = 500'
    echo 'scale_down_cooldown_seconds = 1'
    echo
    echo '[dispatcher]'
    echo "instance = \"$LAB_ID\""
    echo
    echo '[queue]'
    echo 'backend = "nats"'
    echo
    echo '[queue.nats]'
    echo "url = \"nats://127.0.0.1:$nats_port\""
    echo 'user = "gateway"'
    echo "password_file = \"$NATS_DIR/gateway.password\""
    echo
    echo '[objects]'
    echo 'backend = "filesystem"'
    echo "root = \"$OBJECTS_ROOT\""
    echo "key_file = \"$OBJECT_KEY\""
    echo 'regions = ["local"]'
    echo
    echo '[invoke_async]'
    echo 'publish_interval_ms = 100'
    echo
    echo '[async_dispatch]'
    echo 'workers = 2'
    echo 'max_attempts = 3'
    echo 'backoff_initial_ms = 300'
    echo 'backoff_max_ms = 1000'
    echo
    echo '[triggers]'
    echo 'scheduler_interval_ms = 500'
    echo "secret_key_file = \"$TRIGGERS_KEY\""
    echo
    echo '[usage]'
    echo 'collect_interval_ms = 500'
    echo
    echo '[budget]'
    echo 'enabled = true'
    echo "file = \"$BUDGET_FILE\""
    echo
    echo '[metrics]'
    echo "bearer_token = \"$METRICS_TOKEN\""
    echo
    if [ "$console" = true ]; then
      echo '[console]'
      echo 'enabled = true'
      echo "dir = \"$REPO_ROOT/apps/console/out\""
      echo
    fi
    echo '[[identity.tokens]]'
    echo "token = \"$TOKEN_A\""
    echo "tenant_id = \"$TENANT_A\""
    echo 'subject = "lab-a"'
    echo 'roles = ["deploy", "invoke"]'
    echo
    echo '[[identity.tokens]]'
    echo "token = \"$TOKEN_A_ONCALL\""
    echo "tenant_id = \"$TENANT_A\""
    echo 'subject = "lab-a-oncall"'
    echo 'roles = ["invoke", "redrive"]'
    echo
    echo '[[identity.tokens]]'
    echo "token = \"$TOKEN_B\""
    echo "tenant_id = \"$TENANT_B\""
    echo 'subject = "lab-b"'
    echo 'roles = ["deploy", "invoke"]'
    echo
    echo '[[secrets.bindings]]'
    echo "tenant_id = \"$TENANT_A\""
    echo 'binding_ref = "demo-secret"'
    echo "value = \"$DEMO_SECRET_VALUE\""
  } >"$GATEWAY_CONFIG"
  chmod 600 "$GATEWAY_CONFIG"
  umask 022
}

start_nats() {
  local out
  if pid_alive "$NATS_DIR/nats-server.pid" >/dev/null; then
    lab_log "nats-server already running (pid $(cat "$NATS_DIR/nats-server.pid"))"
    return 0
  fi
  out="$(QUEUE_STATE_DIR="$NATS_DIR" QUEUE_BIN_DIR="$NATS_BIN_DIR" \
    QUEUE_PORT="$(manifest_get NATS_PORT)" QUEUE_HTTP_PORT="$(manifest_get NATS_HTTP_PORT)" \
    "$REPO_ROOT/scripts/queue/up.sh" </dev/null 2>&1)" || {
    printf '%s\n' "$out"
    tail -n 30 "$NATS_DIR/nats-server.log" 2>/dev/null || true
    lab_die "nats-server did not start (docs/runbook.md §6.6)"
  }
  printf '%s\n' "$out" | grep -v '^export ' || true
}

gateway_pid() { pid_alive "$GATEWAY_PID_FILE"; }

start_gateway() {
  local pid i
  if pid="$(gateway_pid)"; then
    lab_log "gateway already running (pid $pid)"
    return 0
  fi
  mkdir -p "$RUN_DIR" "$LOG_DIR"
  printf '\n===== gateway start %s (lab %s) =====\n' "$(lab_now)" "$LAB_ID" >>"$GATEWAY_LOG"
  if [ -n "$LAB_SUDO" ]; then
    # root gateway (jailer + cgroup). The shell writes its own pid, then execs the gateway.
    # shellcheck disable=SC2016 # expanded by the root shell, not here
    $LAB_SUDO sh -c 'echo $$ > "$1"; cd "$2"; exec env LOG_FORMAT=json "$3" --config "$4"' _ \
      "$GATEWAY_PID_FILE" "$REPO_ROOT" "$GATEWAY_BIN" "$GATEWAY_CONFIG" </dev/null >>"$GATEWAY_LOG" 2>&1 &
    for i in 1 2 3 4 5 6 7 8 9 10; do [ -s "$GATEWAY_PID_FILE" ] && break; sleep 0.2; done
  else
    # No `cd && cmd &` (that backgrounds a subshell which keeps the transcript pipe open and whose
    # pid is not the gateway's): change directory first, then start the gateway itself.
    cd "$REPO_ROOT"
    LOG_FORMAT=json nohup "$GATEWAY_BIN" --config "$GATEWAY_CONFIG" </dev/null >>"$GATEWAY_LOG" 2>&1 &
    echo $! >"$GATEWAY_PID_FILE"
  fi
  pid="$(cat "$GATEWAY_PID_FILE" 2>/dev/null || true)"
  lab_log "gateway pid ${pid:-?}, log $GATEWAY_LOG"
  for i in $(seq 1 240); do
    if ! gateway_pid >/dev/null; then
      lab_warn "gateway exited during startup; last log lines:"
      tail -n 25 "$GATEWAY_LOG" | sed 's/^/    | /'
      if grep -q 'Address already in use' "$GATEWAY_LOG" 2>/dev/null; then
        lab_die "port $(manifest_get GATEWAY_PORT) is in use (docs/runbook.md §6.5)"
      fi
      if grep -q 'newer than this binary supports' "$GATEWAY_LOG" 2>/dev/null; then
        lab_die "state.db has a newer schema than this gateway (docs/runbook.md §6.4)"
      fi
      lab_die "gateway did not start (docs/runbook.md §6: config invalid / migration / provider)"
    fi
    if [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "http://127.0.0.1:$(manifest_get GATEWAY_PORT)/healthz" || true)" = 200 ]; then
      return 0
    fi
    sleep 0.25
  done
  lab_die "gateway /healthz did not answer within 60 s (log: $GATEWAY_LOG)"
}

cmd_up() {
  local console=false a
  for a in "$@"; do
    case "$a" in
      --console) console=true ;;
      *) lab_die "unknown up option: $a" ;;
    esac
  done
  lab_require_init
  lab_provider
  lab_binaries
  lab_sudo_setup
  [ -n "$(manifest_get BOOTSTRAPPED_AT)" ] || lab_die "lab not bootstrapped (run: scripts/lab/lab.sh bootstrap)"
  local b
  for b in "$GATEWAY_BIN" "$TSLS_BIN" "$BRIDGE_BIN"; do
    [ -x "$b" ] || lab_die "missing $b (run: scripts/lab/lab.sh bootstrap)"
  done
  if [ "$PROVIDER" = firecracker ]; then
    for b in "$FC_BIN" "$FC_KERNEL" "$FC_ROOTFS"; do
      [ -f "$b" ] || lab_die "missing $b (run: scripts/lab/lab.sh --provider firecracker bootstrap)"
    done
    [ "$(sha256_of "$FC_KERNEL")" = "$(manifest_get GUEST_KERNEL_SHA256)" ] || lab_die "guest kernel sha256 changed since bootstrap (docs/runbook.md §6.1)"
  fi
  if [ "$console" = true ] && [ ! -f "$REPO_ROOT/apps/console/out/index.html" ]; then
    lab_die "--console needs apps/console/out (run: scripts/lab/lab.sh bootstrap --console)"
  fi
  local name p
  for name in GATEWAY_PORT NATS_PORT NATS_HTTP_PORT; do
    p="$(manifest_get "$name")"
    if [ -z "$p" ]; then
      p="$(pick_free_port)"
      manifest_set "$name" "$p"
    fi
  done
  lab_log "step 1/5: throwaway secrets and config (lab $LAB_ID, provider $PROVIDER)"
  write_secrets
  write_config "$(manifest_get GATEWAY_PORT)" "$(manifest_get NATS_PORT)" "$console"
  manifest_set CONSOLE_ENABLED "$console"
  lab_log "config: $GATEWAY_CONFIG (0600)"
  if [ "$PROVIDER" = process ]; then
    lab_warn "process provider: functions run as plain child processes of the gateway. NOT a microVM, NO isolation (dev mode)."
  fi
  lab_log "step 2/5: nats-server $NATS_SERVER_VERSION on 127.0.0.1:$(manifest_get NATS_PORT) (store $NATS_DIR/jetstream)"
  start_nats
  lab_log "step 3/5: gateway on 127.0.0.1:$(manifest_get GATEWAY_PORT) (applies forward-only migrations to $DATA_DIR/state.db before listening)"
  start_gateway
  manifest_set STATE up
  manifest_set UP_AT "$(lab_now)"
  lab_log "step 4/5: health of every component"
  local i ok=1
  for i in $(seq 1 40); do
    if health_table quiet; then ok=0; break; fi
    sleep 0.5
  done
  health_table || true
  [ "$ok" = 0 ] || lab_die "health checks failed after start (docs/runbook.md §5, §6)"
  lab_log "step 5/5: ready"
  # shellcheck disable=SC1090
  . "$TOKENS_FILE"
  echo
  echo "  API:     http://127.0.0.1:$(manifest_get GATEWAY_PORT)"
  [ "$console" = false ] || echo "  console: http://127.0.0.1:$(manifest_get GATEWAY_PORT)/console/ (paste a token from $TOKENS_FILE)"
  echo "  tokens:  $TOKENS_FILE (source it; values are never printed)"
  echo "  CLI:     set -a; . $TOKENS_FILE; set +a; TSLS_API_URL=http://127.0.0.1:$(manifest_get GATEWAY_PORT) TSLS_TOKEN=\$TOKEN_A target/debug/tsls functions list"
  echo "  next:    scripts/lab/lab.sh demo all"
}

# ---------------------------------------------------------------------------
# health / status
# ---------------------------------------------------------------------------

H_FAILED=0
H_QUIET=0
hrow() { # STATUS COMPONENT CHECK DETAIL
  [ "$1" = ok ] || [ "$1" = skip ] || H_FAILED=1
  [ "$H_QUIET" = 1 ] || printf '%-5s %-22s %-44s %s\n' "$1" "$2" "$3" "$4"
}

metric_value() { # BODY NAME_WITH_LABELS -> value
  printf '%s\n' "$1" | awk -v m="$2" '$1 == m {print $2; exit}'
}

# health_table [quiet] -> 0 when every component is healthy
health_table() {
  H_FAILED=0
  H_QUIET=0
  [ "${1:-}" = quiet ] && H_QUIET=1
  lab_load_tokens
  local pid ready code body metrics want have_mig v
  [ "$H_QUIET" = 1 ] || printf '%-5s %-22s %-44s %s\n' STATUS COMPONENT CHECK DETAIL
  # nats
  if pid="$(pid_alive "$NATS_DIR/nats-server.pid")"; then
    hrow ok nats-server "process" "pid $pid"
  else
    hrow FAIL nats-server "process" "not running (lab.sh up)"
  fi
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://127.0.0.1:$(manifest_get NATS_HTTP_PORT)/healthz?js-enabled-only=true" || true)"
  if [ "$code" = 200 ]; then hrow ok nats-server "GET /healthz?js-enabled-only=true" "200 (JetStream enabled)"; else hrow FAIL nats-server "GET /healthz?js-enabled-only=true" "${code:-no answer}"; fi
  # gateway
  if pid="$(gateway_pid)"; then hrow ok gateway "process" "pid $pid"; else hrow FAIL gateway "process" "not running (lab.sh up)"; fi
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "$API/healthz" || true)"
  if [ "$code" = 200 ]; then hrow ok gateway "GET /healthz" 200; else hrow FAIL gateway "GET /healthz" "${code:-no answer}"; fi
  body="$(curl -s --max-time 10 "$API/readyz" || true)"
  ready="$(printf '%s' "$body" | jq -r '.ready' 2>/dev/null || true)"
  if [ "$ready" = true ]; then hrow ok gateway "GET /readyz .ready" true; else hrow FAIL gateway "GET /readyz .ready" "${ready:-no answer}"; fi
  v="$(printf '%s' "$body" | jq -r '"\(.preflight.provider) ok=\(.preflight.ok) " + ([.preflight.checks[]? | select(.ok != true) | .name] | join(","))' 2>/dev/null || true)"
  case "$v" in
    "$PROVIDER ok=true "*) hrow ok provider "readyz .preflight" "$v" ;;
    *) hrow FAIL provider "readyz .preflight (failing checks listed)" "${v:-no answer}" ;;
  esac
  v="$(printf '%s' "$body" | jq -r '.dispatcher.fenced' 2>/dev/null || true)"
  if [ "$v" = false ]; then hrow ok dispatcher "readyz .dispatcher.fenced" false; else hrow FAIL dispatcher "readyz .dispatcher.fenced" "${v:-?}"; fi
  v="$(printf '%s' "$body" | jq -r '"\(.control_plane.new_invocations) config=\(.control_plane.config_state)"' 2>/dev/null || true)"
  case "$v" in accepted*) hrow ok config-cache "readyz .control_plane.new_invocations" "$v" ;; *) hrow FAIL config-cache "readyz .control_plane.new_invocations" "${v:-?}" ;; esac
  v="$(printf '%s' "$body" | jq -r '"accepting=\(.usage.accepting) journal_healthy=\(.usage.journal.healthy) collector_error=\(.usage.collector.last_error)"' 2>/dev/null || true)"
  case "$v" in "accepting=true journal_healthy=true collector_error=null") hrow ok usage-journal "readyz .usage" "$v" ;; *) hrow FAIL usage-journal "readyz .usage" "${v:-?}" ;; esac
  v="$(printf '%s' "$body" | jq -r '"accepting=\(.budget.accepting) store_healthy=\(.budget.store.healthy) stalled=\(.budget.collector_stalled) file_error=\(.budget.publication.last_error)"' 2>/dev/null || true)"
  case "$v" in "accepting=true store_healthy=true stalled=false file_error=null") hrow ok budget "readyz .budget" "$v" ;; *) hrow FAIL budget "readyz .budget" "${v:-?}" ;; esac
  # ledger + migrations
  want="$(find "$REPO_ROOT/crates/application/src/repository/sqlite/migrations" -name '*.sql' | wc -l | tr -d ' ')"
  have_mig="$(sqlite_ro "$DATA_DIR/state.db" 'SELECT MAX(version) FROM schema_version' 2>/dev/null || true)"
  if [ -n "$have_mig" ] && [ "$have_mig" = "$want" ]; then
    hrow ok ledger "state.db schema_version = migrations" "$have_mig/$want"
  else
    hrow FAIL ledger "state.db schema_version = migrations" "${have_mig:-unreadable}/$want"
  fi
  # metrics-based components
  metrics="$(curl -s --max-time 10 -H "authorization: Bearer $METRICS_TOKEN" "$API/metrics" || true)"
  v="$(metric_value "$metrics" 'tsls_async_queue_condition{condition="healthy"}')"
  if [ "$v" = 1 ]; then hrow ok queue "tsls_async_queue_condition{healthy}" 1; else hrow FAIL queue "tsls_async_queue_condition{healthy}" "${v:-missing}"; fi
  v="$(metric_value "$metrics" 'tsls_trigger_scheduler_owner')"
  if [ "$v" = 1 ]; then hrow ok trigger-scheduler "tsls_trigger_scheduler_owner" 1; else hrow FAIL trigger-scheduler "tsls_trigger_scheduler_owner" "${v:-missing}"; fi
  v="$(metric_value "$metrics" 'tsls_async_dispatch_runs_in_flight')"
  if [ -n "$v" ]; then hrow ok async-dispatcher "tsls_async_dispatch_runs_in_flight" "$v"; else hrow FAIL async-dispatcher "tsls_async_dispatch_runs_in_flight" missing; fi
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 -H "authorization: Bearer $TOKEN_A" "$API/metrics" || true)"
  if [ -n "$metrics" ] && [ "$code" = 401 ]; then hrow ok metrics "operator token 200, tenant token 401" "401"; else hrow FAIL metrics "operator token 200, tenant token 401" "tenant=$code"; fi
  # object store
  if [ -d "$OBJECTS_ROOT" ] && [ "$(file_mode "$OBJECT_KEY" 2>/dev/null)" = 600 ] && grep -q '"objects":"filesystem"' "$GATEWAY_LOG" 2>/dev/null; then
    hrow ok object-store "root exists, key 0600, backend filesystem" "$OBJECTS_ROOT"
  else
    hrow FAIL object-store "root exists, key 0600, backend filesystem" "$OBJECTS_ROOT"
  fi
  # console
  if [ "$(manifest_get CONSOLE_ENABLED)" = true ]; then
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "$API/console/" || true)"
    if [ "$code" = 200 ]; then hrow ok console "GET /console/" 200; else hrow FAIL console "GET /console/" "${code:-no answer}"; fi
  else
    hrow skip console "GET /console/" "not enabled (up --console)"
  fi
  return "$H_FAILED"
}

cmd_status() {
  lab_require_init
  lab_provider
  lab_binaries
  echo "lab $LAB_ID  provider=$PROVIDER  state=$(manifest_get STATE)  dir=$LAB_DIR"
  [ "$PROVIDER" = firecracker ] || echo "(process provider: dev mode, NOT a microVM, no isolation)"
  if [ ! -f "$TOKENS_FILE" ]; then
    echo "not up (no generated config yet)"
    return 1
  fi
  echo
  health_table
}

# ---------------------------------------------------------------------------
# down
# ---------------------------------------------------------------------------

stop_gateway() {
  local pid waited=0
  if ! pid="$(gateway_pid)"; then
    lab_log "gateway is not running"
    rm -f "$GATEWAY_PID_FILE" 2>/dev/null || $LAB_SUDO rm -f "$GATEWAY_PID_FILE" 2>/dev/null || true
    return 0
  fi
  lab_log "stopping gateway pid $pid (SIGTERM: in-flight invocations are cancelled and environments destroyed)"
  kill -TERM "$pid" 2>/dev/null || $LAB_SUDO kill -TERM "$pid" 2>/dev/null || true
  while gateway_pid >/dev/null && [ "$waited" -lt 120 ]; do sleep 0.25; waited=$((waited + 1)); done
  if gateway_pid >/dev/null; then
    lab_warn "gateway did not exit within 30 s; SIGKILL (environments may be left: teardown checks for them)"
    kill -KILL "$pid" 2>/dev/null || $LAB_SUDO kill -KILL "$pid" 2>/dev/null || true
    sleep 1
  fi
  rm -f "$GATEWAY_PID_FILE" 2>/dev/null || $LAB_SUDO rm -f "$GATEWAY_PID_FILE" 2>/dev/null || true
}

stop_nats() {
  QUEUE_STATE_DIR="$NATS_DIR" QUEUE_BIN_DIR="$NATS_BIN_DIR" "$REPO_ROOT/scripts/queue/down.sh" </dev/null 2>&1 || true
}

cmd_down() {
  lab_require_init
  lab_provider
  if [ "$PROVIDER" = firecracker ] && [ "${LAB_FC_PRIVILEGED:-1}" = 1 ] && [ "$(id -u)" != 0 ]; then LAB_SUDO="sudo -n"; fi
  stop_gateway
  stop_nats
  manifest_set STATE down
  lab_log "down: processes stopped; data kept in $LAB_DIR (up again, or teardown)"
}

# ---------------------------------------------------------------------------
# teardown + orphan check
# ---------------------------------------------------------------------------

DRY_RUN=false
# owned_path PATH -> 0 when PATH is strictly inside the lab directory (after resolving its parent)
owned_path() {
  local p="$1" parent lab
  lab="$(abs_dir "$LAB_DIR")" || return 1
  parent="$(abs_dir "$(dirname "$p")")" || return 1
  case "$parent/$(basename "$p")/" in
    "$lab"/?*/) return 0 ;;
  esac
  return 1
}

remove_path() { # PATH
  local p="$1"
  [ -e "$p" ] || [ -L "$p" ] || return 0
  owned_path "$p" || lab_die "refusing to remove $p: not inside the lab directory $LAB_DIR"
  if [ "$DRY_RUN" = true ]; then
    echo "  [dry-run] would remove $p ($(du -sh "$p" 2>/dev/null | awk '{print $1}'))"
    return 0
  fi
  echo "  remove $p"
  rm -rf "$p" 2>/dev/null || { [ -n "$LAB_SUDO" ] && $LAB_SUDO rm -rf "$p"; } || lab_die "cannot remove $p (files owned by root? rerun with sudo available)"
}

# ledger_environment_ids -> environment ids this lab's ledger ever recorded (empty without a ledger)
ledger_environment_ids() {
  [ -f "$DATA_DIR/state.db" ] || return 0
  sqlite_ro "$DATA_DIR/state.db" 'SELECT id FROM environments' 2>/dev/null || $LAB_SUDO python3 - "$DATA_DIR/state.db" <<'PY' 2>/dev/null || true
import sqlite3, sys
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
for (i,) in con.execute("SELECT id FROM environments"):
    print(i)
PY
}

tap_of() { # ENV_ID -> tsls + 11 hex of sha256(env_id) (crates/providers/firecracker/src/network.rs)
  printf 'tsls%s\n' "$(printf '%s' "$1" | { if have sha256sum; then sha256sum; else shasum -a 256; fi; } | cut -c1-11)"
}

cmd_teardown() {
  local keep_cache=false purge=false a
  for a in "$@"; do
    case "$a" in
      --dry-run) DRY_RUN=true ;;
      --keep-cache) keep_cache=true ;;
      --purge) purge=true ;;
      *) lab_die "unknown teardown option: $a" ;;
    esac
  done
  if [ ! -f "$MARKER" ]; then
    lab_log "no lab at $LAB_DIR (nothing owned by a lab to remove)"
    return 0
  fi
  lab_require_init
  lab_provider
  lab_binaries
  if [ "$PROVIDER" = firecracker ] && [ "${LAB_FC_PRIVILEGED:-1}" = 1 ] && [ "$(id -u)" != 0 ]; then
    if sudo -n true 2>/dev/null; then LAB_SUDO="sudo -n"; else lab_warn "no passwordless sudo: root-owned jails / cgroups / taps cannot be removed"; fi
  fi
  echo "teardown of lab $LAB_ID (provider $PROVIDER, dir $LAB_DIR)$([ "$DRY_RUN" = true ] && echo ' [dry-run: nothing is changed]')"
  local envs env tap cg
  envs="$(ledger_environment_ids)"
  echo "ledger environments recorded by this lab: $(printf '%s\n' "$envs" | grep -c . || true)"

  echo "1. processes"
  if [ "$DRY_RUN" = true ]; then
    echo "  [dry-run] would stop gateway ($(gateway_pid || echo 'not running')) and nats-server ($(pid_alive "$NATS_DIR/nats-server.pid" || echo 'not running'))"
    lab_processes | sed 's/^/  [dry-run] would SIGKILL leftover: /'
  else
    stop_gateway
    stop_nats
    local left pid
    left="$(lab_processes)"
    if [ -n "$left" ]; then
      printf '%s\n' "$left" | while read -r pid _; do
        echo "  SIGKILL leftover process $pid (its command line names $LAB_DIR)"
        kill -KILL "$pid" 2>/dev/null || $LAB_SUDO kill -KILL "$pid" 2>/dev/null || true
      done
      sleep 1
    fi
  fi

  echo "2. Linux host resources named after this lab (jails, cgroups, taps, nft chains)"
  if [ "$(host_os)" = Linux ]; then
    cg="/sys/fs/cgroup/tsls-$LAB_ID"
    if [ -d "$cg" ]; then
      if [ "$DRY_RUN" = true ]; then
        echo "  [dry-run] would rmdir $cg and its children"
      else
        find "$cg" -mindepth 1 -depth -type d -exec sh -c '${1:+$1 }rmdir "$2"' _ "$LAB_SUDO" {} \; 2>/dev/null || true
        if rmdir "$cg" 2>/dev/null || $LAB_SUDO rmdir "$cg" 2>/dev/null; then
          echo "  removed cgroup $cg"
        else
          lab_warn "cgroup $cg not removed (processes still inside?)"
        fi
      fi
    fi
    for env in $envs; do
      tap="$(tap_of "$env")"
      if have ip && ip link show "$tap" >/dev/null 2>&1; then
        if [ "$DRY_RUN" = true ]; then echo "  [dry-run] would delete tap $tap ($env)"; else $LAB_SUDO ip link del "$tap" && echo "  deleted tap $tap ($env)"; fi
      fi
      if have nft && $LAB_SUDO nft list chain inet tachyon_egress "g_$tap" >/dev/null 2>&1; then
        if [ "$DRY_RUN" = true ]; then echo "  [dry-run] would delete nft chain inet tachyon_egress g_$tap"; else $LAB_SUDO nft delete chain inet tachyon_egress "g_$tap" && echo "  deleted nft chain g_$tap"; fi
      fi
    done
  else
    echo "  skip: not Linux (no jails, cgroups, taps or nft on this host)"
  fi

  echo "3. files owned by this lab"
  remove_path "$FC_JAIL_DIR"
  remove_path "$LAB_DIR/fc"
  remove_path "$NATS_DIR"
  remove_path "$OBJECTS_ROOT"
  remove_path "$DATA_DIR"
  remove_path "$DEMO_DIR"
  remove_path "$RUN_DIR"
  remove_path "$CONFIG_DIR"
  remove_path "$SECRETS_DIR"
  if [ "$keep_cache" = true ]; then echo "  keep $CACHE_DIR (--keep-cache)"; else remove_path "$CACHE_DIR"; fi

  if [ "$DRY_RUN" = false ]; then
    manifest_set STATE torn_down
    manifest_set TORN_DOWN_AT "$(lab_now)"
    local k
    for k in GATEWAY_PORT NATS_PORT NATS_HTTP_PORT CONSOLE_ENABLED; do manifest_set "$k" ""; done
    # Without the cache the pinned downloads are gone: the next `up` asks for a bootstrap first.
    [ "$keep_cache" = true ] || manifest_set BOOTSTRAPPED_AT ""
  fi

  echo "4. orphan check"
  orphan_check "$envs" "$keep_cache"
  local orc=$?
  if [ "$purge" = true ] && [ "$DRY_RUN" = false ]; then
    if [ "$orc" = 0 ]; then
      echo "5. --purge: removing $LAB_DIR (manifest and command logs included)"
      owned_path "$LAB_DIR/logs" || lab_die "refusing to purge $LAB_DIR"
      rm -rf "${LAB_DIR:?}"
    else
      lab_warn "--purge skipped: leftovers exist (the manifest and logs are kept for the investigation)"
    fi
  fi
  return "$orc"
}

# orphan_check ENV_IDS KEEP_CACHE -> prints `orphan check: clean` or the leftovers; 1 when any
orphan_check() {
  local envs="$1" keep_cache="$2" found=0 left env tap p
  report() { echo "  LEFTOVER $1"; found=1; }
  left="$(lab_processes)"
  [ -z "$left" ] || report "processes naming $LAB_DIR: $(printf '%s' "$left" | tr '\n' ';')"
  if [ "$PROVIDER" = process ] && [ -x "$BRIDGE_BIN" ]; then
    # Bridges of this checkout's build (another lab of the same checkout would also show up here),
    # and user processes started from this lab's artifact store only.
    left="$(TSLS_BRIDGE_BIN="$BRIDGE_BIN" \
      TSLS_ORPHAN_USER_PATTERN="^$(printf '%s' "$DATA_DIR/" | sed 's/[][\.*^$/+?(){}|]/\\&/g')" \
      "$REPO_ROOT/scripts/e2e/orphan-check.sh" process 2>&1 >/dev/null || true)"
    [ -z "$left" ] || report "runtime bridge / user processes (scripts/e2e/orphan-check.sh): $(printf '%s' "$left" | tr '\n' ';')"
  fi
  [ -z "$(pid_alive "$GATEWAY_PID_FILE" || true)" ] || report "gateway pid file with a live process"
  if [ "$(host_os)" = Linux ]; then
    [ ! -d "/sys/fs/cgroup/tsls-$LAB_ID" ] || report "cgroup /sys/fs/cgroup/tsls-$LAB_ID"
    for env in $envs; do
      tap="$(tap_of "$env")"
      if have ip && ip link show "$tap" >/dev/null 2>&1; then report "tap $tap ($env)"; fi
      if have nft && $LAB_SUDO nft list chain inet tachyon_egress "g_$tap" >/dev/null 2>&1; then report "nft chain g_$tap ($env)"; fi
    done
    if have losetup; then
      left="$(losetup -a 2>/dev/null | grep -F "$LAB_DIR/" || true)"
      [ -z "$left" ] || report "loop devices: $left"
    fi
  fi
  if [ "$DRY_RUN" = false ]; then
    for p in "$LAB_DIR"/* "$LAB_DIR"/.[!.]*; do
      [ -e "$p" ] || continue
      case "$(basename "$p")" in
        logs | manifest.env | .tsls-lab) ;;
        cache) [ "$keep_cache" = true ] || report "path $p" ;;
        *) report "path $p" ;;
      esac
    done
  else
    echo "  (dry-run: the paths listed in step 3 still exist by design)"
  fi
  if [ "$found" = 0 ]; then
    echo "orphan check: clean (lab $LAB_ID)"
    return 0
  fi
  echo "orphan check: leftovers found (docs/runbook.md §7.2)"
  return 1
}

# ---------------------------------------------------------------------------
# logs
# ---------------------------------------------------------------------------

cmd_logs() {
  local what="${1:-last}" f
  case "$what" in
    gateway) tail -n "${LINES_N:-200}" "$GATEWAY_LOG" ;;
    nats) tail -n "${LINES_N:-200}" "$NATS_DIR/nats-server.log" ;;
    commands) ls -1 "$CMD_LOG_DIR" 2>/dev/null ;;
    last)
      f="$(find "$CMD_LOG_DIR" -name '*.log' 2>/dev/null | sort | tail -n 1)"
      [ -n "$f" ] || { echo "no command logs in $CMD_LOG_DIR"; return 1; }
      echo "== $f"
      cat "$f" ;;
    *) echo "logs gateway|nats|commands|last" >&2; return 2 ;;
  esac
}

# ---------------------------------------------------------------------------
# demo
# ---------------------------------------------------------------------------

cmd_demo() {
  local phase="${1:-}"
  case "$phase" in
    p1 | p2 | p3 | all) ;;
    *) lab_die "demo needs p1|p2|p3|all" ;;
  esac
  lab_require_init
  lab_provider
  lab_binaries
  gateway_pid >/dev/null || lab_die "gateway is not running (run: scripts/lab/lab.sh up)"
  health_table quiet || { health_table || true; lab_die "health checks fail; fix them before the demo (docs/runbook.md §5)"; }
  # shellcheck source=scripts/lab/demo.sh
  . "$(dirname "$SCRIPT_PATH")/demo.sh"
  demo_main "$phase"
}

case "$COMMAND" in
  preflight) cmd_preflight "$@" ;;
  bootstrap) cmd_bootstrap "$@" ;;
  up) cmd_up "$@" ;;
  demo) cmd_demo "$@" ;;
  status) cmd_status "$@" ;;
  logs) cmd_logs "$@" ;;
  down) cmd_down "$@" ;;
  teardown) cmd_teardown "$@" ;;
esac
