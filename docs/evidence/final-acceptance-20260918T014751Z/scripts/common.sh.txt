# sourced by the PLT-4649 final acceptance run scripts inside the Lima VM
set -uo pipefail
source ~/.cargo/env 2>/dev/null || true
export PATH="$HOME/.cargo/bin:$PATH"
REPO=~/lab
RUNS=~/plt4649/runs
MACLOAD=/Users/takanorifukuyama/git/tachyon-serverless/.kvm/vm/plt4649-mac-load.tsv
mkdir -p "$RUNS" "$HOME/w"
cd "$REPO"

# mkcfg OUT PORT DATA_DIR [EXTRA_TOML_FILE]
mkcfg() {
  local out="$1" port="$2" data="$3" extra="${4:-}"
  sed -e "s#^listen = .*#listen = \"127.0.0.1:$port\"#" \
      -e "s#^data_dir = .*#data_dir = \"$data\"#" \
      config/gateway.firecracker.toml > "$out"
  if [ -n "$extra" ]; then cat "$extra" >> "$out"; fi
}

hostinfo() {
  {
    echo "== $2 $(date -u +%FT%TZ)"
    echo "vm: $(uptime)"
    free -m | head -2
    df -h / | tail -1
  } >> "$1"
}

vmload_sampler() {
  ( printf 'epoch\tutc\tload1\tload5\tload15\n'; while :; do read -r l1 l5 l15 _ < /proc/loadavg; printf '%s\t%s\t%s\t%s\t%s\n' "$(date -u +%s)" "$(date -u +%FT%TZ)" "$l1" "$l5" "$l15"; sleep 5; done ) > "$1" &
}

leftovers() {
  echo "-- processes (gateway, firecracker, jailer, nats-server)"
  pgrep -a -f '(^|/)(firecracker|jailer|tachyon-serverless-gateway|nats-server)( |$)' | grep -v pgrep || echo none
  echo "-- cgroups"; find /sys/fs/cgroup/tachyon /sys/fs/cgroup -maxdepth 1 -mindepth 1 -type d \( -name 'env_*' -o -name 'tsls-*' -o -path '/sys/fs/cgroup/tachyon/*' \) 2>/dev/null || true; echo "(end cgroups)"
  echo "-- jails"; sudo find /srv/jailer -mindepth 1 -maxdepth 2 2>/dev/null; echo "(end jails)"
  echo "-- run dir"; find "$REPO/.kvm/run" -mindepth 1 -maxdepth 1 ! -name _archive 2>/dev/null; echo "(end run dir)"
  echo "-- taps"; ip -br link | awk '$1 ~ /^(tsls|effx)/'; echo "(end taps)"
  echo "-- netns"; ip netns list; echo "(end netns)"
  echo "-- nft"; sudo nft list tables
  echo "-- disk"; df -h / | tail -1
}

fix_owner() { sudo chown -R "$(id -u):$(id -g)" "$@" 2>/dev/null; }
