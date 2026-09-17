Aborted first full run of scripts/x1/restore-verify.sh (PLT-4654), commit 4076afe, 2026-09-17T13:03:28Z.

Stopped with exit 2 in the first/second cycles: snapshot C2 failed ("snapshot source checkpoint:
timed out waiting for checkpoint", the source VMM booted but its bridge did not connect within
30 s while the physical host's load average rose to 8-11) and the script then aborted on the empty
snapshot id. The script now records a failed creation, retries once and never aborts on a missing
artifact (commit dd45f2e). The complete run is docs/evidence/x1-restore-verify-20260917T131439Z/.

Kept here: checks.tsv (14 PASS, cycle-2 FAIL), summary.md (regenerated offline from the raw data of
that run, which is not kept), snapshots.jsonl, calibration.jsonl, versions.txt. Warm and the later
negative checks did not run. Teardown memory columns are empty because teardown-stats.jsonl is
written at the end of a run. See docs/x1-results.md section 6.
