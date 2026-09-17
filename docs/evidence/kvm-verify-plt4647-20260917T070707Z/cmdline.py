"""Does /proc/<pid>/cmdline read back empty while a live (non-zombie) process execs?

The child execs `sleep` with an argument that marks it (like the jailer's `--id <instance id>`);
the parent reads /proc/<pid>/cmdline in a tight loop from fork until the child is a zombie and
counts reads that came back empty while /proc/<pid>/stat said the process was not a zombie.
"""
import os, sys, time

N = int(sys.argv[1]) if len(sys.argv) > 1 else 500
MARK = b"env-01m2probe"
empty_live = 0
iters_with_empty = 0
reads = 0
for i in range(N):
    pid = os.fork()
    if pid == 0:
        os.execv("/usr/bin/sleep", ["/usr/bin/sleep", "0.02"])
    seen_empty = False
    exec_done = False
    while True:
        try:
            raw = open(f"/proc/{pid}/cmdline", "rb").read()
            st = open(f"/proc/{pid}/stat", "rb").read()
        except OSError:
            break
        reads += 1
        state = st[st.rfind(b")") + 2:st.rfind(b")") + 3]
        if state == b"Z":
            break
        if b"sleep" in raw:
            exec_done = True
        if raw == b"" and not exec_done:
            # empty while the exec is in progress (before the new image's argv is visible)
            empty_live += 1
            seen_empty = True
    os.waitpid(pid, 0)
    if seen_empty:
        iters_with_empty += 1
print(f"execs={N} reads={reads} empty_reads_during_exec={empty_live} execs_with_an_empty_read_during_exec={iters_with_empty}")
