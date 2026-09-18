#!/bin/bash
# Collect the PLT-4649 evidence into the rw-mounted host directory (the Mac copies it into the repo).
. "$(dirname "$0")/common.sh"
OUT=/Users/takanorifukuyama/git/tachyon-serverless/.kvm/vm/plt4649-out
rm -rf "$OUT"; mkdir -p "$OUT/runs" "$OUT/evidence"
sudo chown -R "$(id -u):$(id -g)" ~/plt4649 2>/dev/null
cp -r ~/plt4649/lab "$OUT/runs/lab-run1"
cp -r ~/plt4649/lab2 "$OUT/runs/lab-run2"
cp -r ~/plt4649/runs/. "$OUT/runs/"
cp ~/plt4649/progress.txt "$OUT/progress.txt"
cp -r ~/plt4649/scripts "$OUT/scripts"
# the evidence directories the scripts wrote inside the clone today
for d in "$REPO"/docs/evidence/*20260918*; do [ -d "$d" ] && cp -r "$d" "$OUT/evidence/"; done
du -sh "$OUT"
find "$OUT" -maxdepth 2 -type d | sort
