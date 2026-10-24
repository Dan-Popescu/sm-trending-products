"""Private loopback test transport; synthetic overrides deliberately require localhost."""

import os
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from sm_trending_products.cli import main


class Relay(BaseHTTPRequestHandler):
    def do_POST(self):
        request = urllib.request.Request(
            os.environ["FIXTURE_SOURCE_URL"] + self.path,
            data=self.rfile.read(int(self.headers["Content-Length"])),
            headers={"Content-Type": "application/json"},
        )
        try:
            response = urllib.request.urlopen(request, timeout=90)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = response.read()
            self.send_response(response.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", 19443), Relay)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    raise SystemExit(
        main(["capture", "--synthetic", "--loop", "--batch-size", "1", "--poll-seconds", "0.1"])
    )
