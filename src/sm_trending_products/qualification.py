"""Own disposable qualification resources without owning workload/report semantics."""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import tempfile
from pathlib import Path
from types import TracebackType
from typing import Any, TextIO
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
from moto.server import ThreadedMotoServer
from psycopg import sql

from .contracts import canonical


def scoped_database_url(database_url: str, schema: str) -> str:
    parsed = urlsplit(database_url)
    query = dict(parse_qsl(parsed.query))
    query.update(options=f"-c search_path={schema}", connect_timeout="5")
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urlencode(query, quote_via=quote),
            parsed.fragment,
        )
    )


def stop_session(process: subprocess.Popen) -> None:
    """Stop only a child session created with start_new_session=True, including descendants."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        pass
    # The leader may have exited while a descendant ignored SIGTERM.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=2)


class QualificationResources:
    """One scratch directory/schema/Moto and worker; callers retain their checkpoint rules."""

    def __init__(self, database_url: str, *, schema_prefix: str, directory_prefix: str):
        self.schema = schema_prefix + uuid4().hex
        self.database_url = scoped_database_url(database_url, self.schema)
        self.directory = Path(tempfile.mkdtemp(prefix=directory_prefix))
        self.database_version: str | None = None
        self.endpoint: str | None = None
        self.cleanup: dict[str, Any] = {
            "schema_dropped": None,
            "files_removed": None,
            "errors": [],
        }
        self._database_url = database_url
        self._server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
        self._schema_created = False
        self._server_start_attempted = False
        self._process: subprocess.Popen[str] | None = None
        self._worker_stopped = False
        self._closed = False

    @property
    def schema_created(self) -> bool:
        return self._schema_created

    @property
    def worker_exit_code(self) -> int | None:
        return self._process.returncode if self._process is not None else None

    def start(self) -> None:
        """Provision the owned stores inside the caller's ordinary failure/report handling."""
        if self._closed or self._schema_created:
            raise RuntimeError("Qualification resources already started or closed")
        with psycopg.connect(self._database_url, autocommit=True, connect_timeout=5) as connection:
            connection.execute("SET statement_timeout='5000ms'")
            connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))
            self._schema_created = True
            version = connection.execute("SELECT version()").fetchone()
            assert version is not None
            self.database_version = version[0]
        # A start failure can happen after Moto has created its thread/server.
        self._server_start_attempted = True
        self._server.start()
        host, port = self._server.get_host_and_port()
        self.endpoint = f"http://{host}:{port}"

    def launch(
        self,
        arguments: list[str],
        payload: dict,
        *,
        environment: dict[str, str],
        log: TextIO | None = None,
    ) -> subprocess.Popen[str]:
        if self._closed or self._process is not None:
            raise RuntimeError("Qualification worker already launched or resources closed")
        self._process = subprocess.Popen(
            arguments,
            stdin=subprocess.PIPE,
            stdout=log if log is not None else subprocess.DEVNULL,
            stderr=log if log is not None else subprocess.DEVNULL,
            text=True,
            env=environment,
            start_new_session=True,
        )
        # Retain ownership before stdin I/O, which can fail after the child starts.
        assert self._process.stdin is not None
        try:
            self._process.stdin.write(canonical(payload))
        finally:
            self._process.stdin.close()
        return self._process

    def stop_worker(self) -> None:
        """Leave files/stores readable for the caller's final checkpoint and measurements."""
        if self._process is not None and not self._worker_stopped:
            stop_session(self._process)
            self._worker_stopped = True

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self.stop_worker()
        except Exception as error:
            self.cleanup["errors"].append(type(error).__name__)
        if self._server_start_attempted:
            try:
                self._server.stop()
            except Exception as error:
                self.cleanup["errors"].append(type(error).__name__)
        if self._schema_created:
            try:
                with psycopg.connect(
                    self._database_url, autocommit=True, connect_timeout=5
                ) as connection:
                    connection.execute("SET statement_timeout='5000ms'")
                    connection.execute(
                        sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema))
                    )
                self.cleanup["schema_dropped"] = True
            except Exception as error:
                self.cleanup["schema_dropped"] = False
                self.cleanup["errors"].append(type(error).__name__)
        try:
            shutil.rmtree(self.directory)
            self.cleanup["files_removed"] = not self.directory.exists()
        except Exception as error:
            self.cleanup["files_removed"] = False
            self.cleanup["errors"].append(type(error).__name__)

    def __enter__(self) -> QualificationResources:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
