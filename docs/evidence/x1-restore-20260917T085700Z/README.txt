X1 (PLT-4652) snapshot / restore feasibility - evidence index

Decision and interpretation: docs/adr/0015-snapshot-restore-feasibility.md
Host: Apple M4 -> Lima VM (vz, nested virtualization), Linux 7.0.0-31-generic aarch64.
Same host / same CPU only. Timings are nested-virtualization reference values, not SLAs.

fc/                    scripts/x1/fc-restore.sh, final run (Firecracker v1.17.0). summary.tsv: 0 FAIL.
                       cold 2623 / 2763 / 2979 ms; snapshot create 232 ms; load API 8-10 ms;
                       load -> Ready 334 / 303 / 259 / 255 ms with the doorbell, 3952 ms without;
                       2 concurrent clones share snap/mem and write only their own scratch copy;
                       the product bridge dies after a restore.
fc-run2-no-doorbell/   earlier run of the same script before the doorbell existed: all 5 restores
                       noticed the vsock reset only at the 5 s heartbeat write (load -> Ready
                       4013-4256 ms). Its source-resume-vsock-reset FAIL was an x1-host bug (it kept
                       reading the old connection; fixed); the guest did reconnect (source/host.jsonl).
ch-v53.0/, ch-v51.1/   scripts/x1/ch-restore.sh final runs: the guest never reaches the bridge
                       handshake; VMM-only pause / snapshot / --restore / resume were accepted on the
                       stalled guest (guest liveness not verified, not counted as a restore).
ch-attempts/           intermediate Cloud Hypervisor runs in order: hvc0 console stall; scratch mount
                       EIO (sector 0 write protection of auto-detected raw images); vsock connected but
                       no Hello; no connection without a console; v51.1 the same.
kata-sources.txt       Kata Containers 3.32.0 source / docs citations (nothing installed or run).
scripts/               copies of scripts/x1/*.sh, the wrappers used to run them in the VM
                       (vm-run-*.sh.txt) and the CH console probe. Not run by CI.
fc-run.log, ch-run.log output of the last run of each script.
cleanup.txt            the verification VM after the runs (no VMM / host process, work dirs removed).

Reading fc/
  summary.tsv / summary.json   one row per check; metric_ms is host wall time (see detail).
  <scenario>/host.jsonl        x1-host events. A restore is proven by restore_identified after an
                               x1_reconnect frame whose guest_boot_id equals the source's hello.
                               A cold_boot_detected event would mean the guest booted instead
                               (none occurred).
  restore-clocks.jsonl         guest clocks and 16 bytes of /dev/urandom at reconnect + host wall time.
  responses.tsv                first invoke response of each restored copy.
  clones-concurrent.txt        /proc/<pid>/maps and smaps_rollup of the two concurrent clone VMMs.
  clones-disk.txt              debugfs cat /x1-instance per scratch copy; snapshot sha256 afterwards.
  snapshot-files.txt           snapshot sizes and sha256 right after creation.
  <scenario>/api.log, fc.log, console.log   Firecracker API exchanges, VMM log, guest serial console.

Known recording defects (the data are otherwise unchanged)
  - versions.txt "commit d6e729f..." is the stale .git left in the VM's ~/tsls (the tree is rsynced
    without .git). The code that ran is branch x1/plt-4652-restore-feasibility on origin/main e807dcf.
  - "host_cpu 0x000" is the CPU part field the nested VM reports.
  - ch-*: mechanics-restore metric_ms includes a fixed 3 s sleep before reading the VMM state.
  - Two early Cloud Hypervisor attempts (ttyAMA0 without root=: reboot loop; earlycon=pl011: power off
    at 0.176 s) were overwritten before attempts were archived; they are described in the ADR.
