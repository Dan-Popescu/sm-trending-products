"""Absolute request deadlines with real controlled TCP streams; no live stores."""

import socket
import socketserver
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from typing import Any

import pytest

from sm_trending_products.commerce_providers import ConnectionConfig, ProviderError
from sm_trending_products.commerce_providers._http import HttpTransport


@contextmanager
def streaming_server(stage: str) -> Iterator[tuple[str, list[bytes]]]:
    stopped = threading.Event()
    requests: list[bytes] = []

    class Handler(socketserver.BaseRequestHandler):
        def handle(self) -> None:
            self.request.settimeout(1)
            if stage == "tls":
                # Accept TCP, but never complete the TLS handshake.
                stopped.wait(timeout=2)
                return
            request = bytearray()
            while b"\r\n\r\n" not in request:
                chunk = self.request.recv(4096)
                if not chunk:
                    return
                request.extend(chunk)
            requests.append(bytes(request))
            try:
                if stage == "status":
                    parts = [bytes([byte]) for byte in b"HTTP/1.1 200 OK\r\n"]
                elif stage == "headers":
                    self.request.sendall(b"HTTP/1.1 200 OK\r\n")
                    parts = [f"X-Progress-{index}: ok\r\n".encode() for index in range(20)]
                elif stage == "chunk":
                    self.request.sendall(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                    parts = [bytes([byte]) for byte in b"0000000000000002\r\n"]
                else:
                    self.request.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n")
                    parts = [b"x"] * 1000
                for part in parts:
                    self.request.sendall(part)
                    if stopped.wait(timeout=0.03):
                        return
                self.request.sendall(b"Content-Length: 2\r\n\r\n{}")
            except (BrokenPipeError, ConnectionResetError, TimeoutError):
                pass

    server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01))
    worker.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", requests
    finally:
        stopped.set()
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)
        assert not worker.is_alive()


def config(url: str) -> ConnectionConfig:
    return ConnectionConfig(
        "woocommerce",
        "deadline-fixture",
        url,
        enabled=True,
        test_mode=True,
        consumer_key="fixture",
        consumer_secret="fixture",
        webhook_secret="fixture",
        timeout_seconds=0.1,
    )


@pytest.mark.parametrize("stage", ["status", "headers", "body", "chunk"])
@pytest.mark.parametrize("write", [False, True])
def test_slow_progress_cannot_extend_status_headers_body_or_chunk_deadline(
    stage: str, write: bool
) -> None:
    with streaming_server(stage) as (url, requests):
        transport = HttpTransport(config(url))
        started = time.monotonic()
        with pytest.raises(ProviderError) as caught:
            transport.request(
                "POST" if write else "GET", "/", {}, {} if write else None, write=write
            )
        elapsed = time.monotonic() - started
        assert caught.value.code == "transport_failed"
        assert caught.value.ambiguous is write
        assert elapsed < 0.25, f"0.1 second deadline extended to {elapsed:.3f} seconds"
        assert len(requests) == 1  # A timeout never reconnects/retries a POST.


def test_tls_handshake_shares_remaining_budget_after_tcp_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with streaming_server("tls") as (url, _):
        port = int(url.rsplit(":", 1)[1])
        original_connect = socket.socket.connect
        connect_timeouts: list[float | None] = []

        def controlled_connect(sock: socket.socket, address: Any) -> None:
            connect_timeouts.append(sock.gettimeout())
            # Account for real connection progress before the stalled handshake.
            time.sleep(0.12)
            original_connect(sock, ("127.0.0.1", port))

        monkeypatch.setattr(socket.socket, "connect", controlled_connect)
        monkeypatch.setattr(
            socket, "getaddrinfo", lambda *args, **kwargs: [(2, 1, 6, "", ("93.184.216.34", 443))]
        )
        # TLS remains certificate-verifying; the controlled peer never gets as
        # far as presenting a certificate. No remote provider can be contacted.
        live = replace(
            config(url), base_url="https://deadline.example", test_mode=False, timeout_seconds=0.2
        )
        started = time.monotonic()
        with pytest.raises(ProviderError) as caught:
            HttpTransport(live).request("POST", "/", {}, {"draft": True}, write=True)
        elapsed = time.monotonic() - started
        assert elapsed < 0.29, f"TCP+TLS phases exceeded one budget: {elapsed:.3f}s"
        assert caught.value.ambiguous
        assert len(connect_timeouts) == 1


def test_write_phases_use_one_deadline_not_a_fresh_timeout_per_send(monkeypatch):
    from sm_trending_products.commerce_providers import _http

    now = [100.0]
    monkeypatch.setattr(_http.time, "monotonic", lambda: now[0])
    observed_timeouts: list[float] = []

    class ControlledSocket:
        def settimeout(self, remaining: float) -> None:
            observed_timeouts.append(remaining)

        def send(self, data: Any) -> int:
            now[0] += 0.06
            return min(len(data), 2)

    connection = _http._DeadlineConnection(
        _http.Origin("127.0.0.1", 9, "", False), "127.0.0.1", _http._Deadline(100.1)
    )
    connection.sock = ControlledSocket()
    with pytest.raises(TimeoutError):
        connection.send(b"abcde")
    assert observed_timeouts == pytest.approx([0.1, 0.04])
