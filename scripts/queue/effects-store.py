#!/usr/bin/env python3
"""The external side-effect store of examples/idempotent-async for microVM runs.

A function in a Firecracker microVM cannot reach the host file system, and its /tmp
disappears with the environment, so the idempotency records of the example live here,
outside every environment (IDEMPOTENT_ASYNC_URL=http://<address>:<port>). The layout is
the one the example writes in directory mode, so the end-to-end checks read the same files:

  <dir>/executions/<key>.count   execution counter per business key
  <dir>/executions.log           one line per execution (applied|skipped|failed|oversized)
  <dir>/effects/<key>.json       the side effect, created at most once

  POST /count/<key>    -> 200 "<n>"       (n = executions of <key> including this one)
  POST /log            -> 200             (body = one line, appended with one write)
  POST /effect/<key>   -> 201 | 409       (body = the record; 409 when it already existed)

The server handles one request at a time (http.server.HTTPServer is not threaded), which
makes each operation atomic. Test infrastructure only: no authentication, bind it to an
address only the test's guests can reach (scripts/queue/effects-netns.sh).
"""

import argparse
import os
import re
from http.server import BaseHTTPRequestHandler, HTTPServer

KEY = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dir", required=True)
    parser.add_argument("--bind", required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    root = os.path.abspath(args.dir)
    os.makedirs(os.path.join(root, "executions"), exist_ok=True)
    os.makedirs(os.path.join(root, "effects"), exist_ok=True)

    class Handler(BaseHTTPRequestHandler):
        def answer(self, code, body=b""):
            self.send_response(code)
            self.send_header("content-length", str(len(body)))
            self.send_header("connection", "close")
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):  # noqa: N802 (http.server naming)
            length = int(self.headers.get("content-length") or 0)
            body = self.rfile.read(length).decode("utf-8", "replace") if length else ""
            parts = self.path.strip("/").split("/")
            if parts == ["log"]:
                if "\n" in body:
                    return self.answer(400, b"one line")
                with open(os.path.join(root, "executions.log"), "a", encoding="utf-8") as f:
                    f.write(body + "\n")
                return self.answer(200)
            if len(parts) == 2 and KEY.match(parts[1]):
                kind, key = parts
                if kind == "count":
                    path = os.path.join(root, "executions", key + ".count")
                    try:
                        with open(path, encoding="utf-8") as f:
                            n = int(f.read().strip() or 0) + 1
                    except (FileNotFoundError, ValueError):
                        n = 1
                    with open(path, "w", encoding="utf-8") as f:
                        f.write(str(n))
                    return self.answer(200, str(n).encode())
                if kind == "effect":
                    path = os.path.join(root, "effects", key + ".json")
                    try:
                        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
                    except FileExistsError:
                        return self.answer(409)
                    with os.fdopen(fd, "w", encoding="utf-8") as f:
                        f.write(body + "\n")
                    return self.answer(201)
            return self.answer(404)

        def log_message(self, fmt, *a):
            print("effects-store %s %s" % (self.address_string(), fmt % a), flush=True)

    server = HTTPServer((args.bind, args.port), Handler)
    print("effects-store listening on %s:%d dir=%s" % (args.bind, args.port, root), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
