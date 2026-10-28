"""One stdlib attempt with one TCP/TLS/send/response deadline and verified TLS.

Every blocking stream read consumes the same monotonic budget. System DNS uses
host resolver limits: stdlib resolution cannot be interrupted by this deadline.
Expired DNS work prevents subsequent connection, rather than starting a new budget.
"""

import http.client
import io
import ipaddress
import json
import math
import socket
import ssl
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from .contracts import ConnectionConfig, ProviderError


def parse_json(raw: bytes) -> Any:
    def reject_constant(value: str) -> None:
        raise ValueError("invalid JSON constant")

    def unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        return json.loads(raw, parse_constant=reject_constant, object_pairs_hook=unique_pairs)
    except (UnicodeError, ValueError, RecursionError):
        raise ProviderError("malformed_response") from None


@dataclass(frozen=True)
class Origin:
    host: str
    port: int
    prefix: str
    tls: bool


def validate_config(config: ConnectionConfig) -> Origin:
    try:
        url = urlsplit(config.base_url)
        host = url.hostname or ""
        port = url.port or (443 if url.scheme == "https" else 80)
        invalid = (
            not config.connection_id
            or len(config.connection_id) > 200
            or url.username is not None
            or url.password is not None
            or url.query
            or url.fragment
            or "\\" in config.base_url
            or any(ord(c) < 33 for c in config.base_url)
            or not host
            or "%" in url.path
            or ".." in url.path.split("/")
            or (url.path and not url.path.startswith("/"))
        )
        if invalid:
            raise ValueError()
        if config.test_mode:
            if url.scheme != "http" or not ipaddress.ip_address(host).is_loopback:
                raise ValueError()
        elif url.scheme != "https" or port != 443:
            raise ValueError()
        if config.provider == "shopify" and not config.test_mode:
            if (
                not host.endswith(".myshopify.com")
                or host.count(".") != 2
                or url.path
                not in (
                    "",
                    "/",
                )
            ):
                raise ValueError()
        if config.api_version != "2026-10":
            raise ValueError()
        if not math.isfinite(config.timeout_seconds) or not 0 < config.timeout_seconds <= 60:
            raise ValueError()
        if not 1 <= config.max_response_bytes <= 4_194_304:
            raise ValueError()
        if not 1 <= config.max_webhook_bytes <= 1_048_576:
            raise ValueError()
        secrets = (
            config.consumer_key,
            config.consumer_secret,
            config.access_token,
            config.webhook_secret,
        )
        if any("\r" in s or "\n" in s or len(s) > 4096 for s in secrets):
            raise ValueError()
        if config.enabled:
            credentials = (
                (config.consumer_key and config.consumer_secret)
                if config.provider == "woocommerce"
                else config.access_token
            )
            if not credentials or not config.webhook_secret:
                raise ValueError()
        return Origin(host, port, url.path.rstrip("/"), url.scheme == "https")
    except (ValueError, TypeError):
        raise ProviderError("invalid_configuration") from None


def require_enabled(config: ConnectionConfig) -> None:
    if not config.enabled:
        raise ProviderError("connection_disabled")


@dataclass(frozen=True)
class _Deadline:
    expires_at: float

    def remaining(self) -> float:
        remaining = self.expires_at - time.monotonic()
        if remaining <= 0:
            raise TimeoutError()
        return remaining

    def arm(self, sock: socket.socket) -> None:
        sock.settimeout(self.remaining())


class _DeadlineReader(io.RawIOBase):
    """Re-arm before EVERY recv, including buffered status/header/chunk reads."""

    def __init__(self, sock: socket.socket, deadline: _Deadline) -> None:
        super().__init__()
        self.sock = sock
        self.deadline = deadline
        # Preserve socket.makefile's ownership semantics: HTTP/1.0 may close its
        # connection before the body is read. The raw file retains that socket.
        self.raw = sock.makefile("rb", buffering=0)

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int | None:
        self.deadline.arm(self.sock)
        received = self.raw.readinto(buffer)
        self.deadline.remaining()
        return received

    def close(self) -> None:
        try:
            self.raw.close()
        finally:
            super().close()


class _ResponseSocket:
    """Only HTTPResponse's makefile boundary, with no change to TLS validation."""

    def __init__(self, sock: socket.socket, deadline: _Deadline) -> None:
        self.sock = sock
        self.deadline = deadline

    def makefile(self, mode: str) -> io.BufferedReader:
        if mode != "rb":
            raise ValueError("unsupported response stream")
        return io.BufferedReader(_DeadlineReader(self.sock, self.deadline))


def _response_class(deadline: _Deadline) -> type[http.client.HTTPResponse]:
    class DeadlineResponse(http.client.HTTPResponse):
        def __init__(
            self,
            sock: socket.socket,
            debuglevel: int = 0,
            method: str | None = None,
            url: str | None = None,
        ) -> None:
            # HTTPResponse consumes only makefile("rb"); the stdlib's type
            # annotation unnecessarily requires a concrete socket object here.
            super().__init__(
                _ResponseSocket(sock, deadline),  # type: ignore[arg-type]
                debuglevel,
                method=method,
                url=url,
            )

    return DeadlineResponse


class _DeadlineConnection(http.client.HTTPConnection):
    def __init__(self, origin: Origin, address: str, deadline: _Deadline) -> None:
        super().__init__(origin.host, origin.port, timeout=deadline.remaining())
        self.origin = origin
        self.address = ipaddress.ip_address(address)
        self.deadline = deadline
        self.default_port = 443 if origin.tls else 80
        self.auto_open = 0  # No implicit reconnects after a partially sent request.
        self.response_class = _response_class(deadline)

    def connect(self) -> None:
        # A numeric, already validated address avoids another DNS resolution and
        # pins the socket against rebinding. DNS itself uses OS resolver limits.
        family = socket.AF_INET6 if self.address.version == 6 else socket.AF_INET
        sock = socket.socket(family, socket.SOCK_STREAM)
        try:
            self.deadline.arm(sock)
            sock.connect((str(self.address), self.origin.port))
            self.deadline.remaining()
            if self.origin.tls:
                context = ssl.create_default_context()
                self.deadline.arm(sock)
                sock = context.wrap_socket(
                    sock, server_hostname=self.origin.host, do_handshake_on_connect=False
                )
                self.deadline.arm(sock)
                sock.do_handshake()
                self.deadline.remaining()
            self.sock = sock
        except BaseException:
            sock.close()
            raise

    def send(self, data: Any) -> None:
        if self.sock is None:
            raise http.client.NotConnected()
        if not isinstance(data, bytes | bytearray | memoryview):
            raise ValueError("unsupported request stream")
        view = memoryview(data)
        while view:
            self.deadline.arm(self.sock)
            sent = self.sock.send(view[:65_536])
            self.deadline.remaining()
            if not sent:
                raise ConnectionError()
            view = view[sent:]


class HttpTransport:
    def __init__(self, config: ConnectionConfig) -> None:
        self.config = config
        self.origin = validate_config(config)

    def request(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        payload: dict[str, Any] | None = None,
        *,
        write: bool = False,
    ) -> tuple[Any, dict[str, str]]:
        require_enabled(self.config)
        if not path.startswith("/") or path.startswith("//") or "\r" in path or "\n" in path:
            raise ProviderError("invalid_request_path")
        deadline = _Deadline(time.monotonic() + self.config.timeout_seconds)
        conn: http.client.HTTPConnection | None = None
        response: http.client.HTTPResponse | None = None
        try:
            if self.origin.tls:
                addresses = socket.getaddrinfo(
                    self.origin.host, self.origin.port, type=socket.SOCK_STREAM
                )
                if not addresses or any(
                    not ipaddress.ip_address(item[4][0]).is_global for item in addresses
                ):
                    raise ProviderError("unsafe_store_address")
                address = str(addresses[0][4][0])
            else:
                address = self.origin.host
            conn = _DeadlineConnection(self.origin, address, deadline)
            body = None if payload is None else json.dumps(payload, allow_nan=False).encode()
            if body is not None and len(body) > self.config.max_response_bytes:
                raise ProviderError("request_too_large")
            conn.connect()
            deadline.remaining()
            conn.request(
                method,
                self.origin.prefix + path,
                body=body,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    **headers,
                },
            )
            deadline.remaining()
            response = conn.getresponse()
            deadline.remaining()
            response_headers = {key.lower(): value for key, value in response.getheaders()}
            if 300 <= response.status < 400:
                raise ProviderError("redirect_refused", ambiguous=write)
            if response.status == 429:
                raise ProviderError("rate_limited", retryable=True)
            if response.status >= 500:
                raise ProviderError("provider_unavailable", retryable=True, ambiguous=write)
            if response.status == 404:
                raise ProviderError("not_found")
            if not 200 <= response.status < 300:
                raise ProviderError("provider_rejected")
            if response_headers.get("content-encoding", "identity") != "identity":
                raise ProviderError("unsupported_response_encoding", ambiguous=write)
            length = response_headers.get("content-length")
            if length is not None and (
                not length.isdecimal() or int(length) > self.config.max_response_bytes
            ):
                raise ProviderError("response_too_large", ambiguous=write)
            chunks = bytearray()
            while len(chunks) <= self.config.max_response_bytes:
                deadline.remaining()
                chunk = response.read1(
                    min(65_536, self.config.max_response_bytes + 1 - len(chunks))
                )
                if not chunk:
                    break
                chunks.extend(chunk)
            if len(chunks) > self.config.max_response_bytes:
                raise ProviderError("response_too_large", ambiguous=write)
            if length is not None and len(chunks) != int(length):
                raise ProviderError("truncated_response", retryable=True, ambiguous=write)
            try:
                parsed = parse_json(bytes(chunks))
                deadline.remaining()
                return parsed, response_headers
            except ProviderError as exc:
                raise ProviderError(exc.code, ambiguous=write) from None
        except ProviderError:
            raise
        except (OSError, http.client.HTTPException, ValueError):
            # Never include exception text: URLs, server messages and credentials
            # may be present. A lost write response cannot establish its outcome.
            raise ProviderError("transport_failed", retryable=True, ambiguous=write) from None
        finally:
            if response is not None:
                response.close()
            if conn is not None:
                conn.close()
