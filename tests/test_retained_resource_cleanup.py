"""Setup failures preserve diagnostics and release only owned measurement state."""

import errno
import importlib.util
import json
import os
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "retained_cleanup", ROOT / "scripts/retained-history.py"
)
assert SPEC and SPEC.loader
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


@pytest.mark.integration
@pytest.mark.parametrize("failure", ["archive", "configuration", "log"])
def test_preparation_disk_failure_releases_schema_and_preserves_original_error(
    pipeline, tmp_path, monkeypatch, failure
):
    output = tmp_path / "failed.json"
    injected = OSError(errno.ENOSPC, "fixture setup disk full")
    connections = []
    statements = []
    original_connect = RUNNER.psycopg.connect

    class ConnectionObserver:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, query):
            statements.append(query.as_string(self.connection))
            return self.connection.execute(query)

    @contextmanager
    def connect(*args, **kwargs):
        connections.append(args)
        with original_connect(*args, **kwargs) as connection:
            yield ConnectionObserver(connection)

    original_write = Path.write_text
    original_open = Path.open

    def write(path, *args, **kwargs):
        if failure == "configuration" and path.name == "configuration.json":
            raise injected
        return original_write(path, *args, **kwargs)

    def open_file(path, *args, **kwargs):
        if failure == "archive" and path == output.with_suffix(".harness.py"):
            raise injected
        if failure == "log" and path == output.with_suffix(".log"):
            raise injected
        return original_open(path, *args, **kwargs)

    def spawn(*args, **kwargs):
        pytest.fail("A worker must not start after a preparation failure")

    monkeypatch.setattr(Path, "write_text", write)
    monkeypatch.setattr(Path, "open", open_file)
    monkeypatch.setattr(RUNNER, "psycopg", SimpleNamespace(connect=connect))
    monkeypatch.setattr(RUNNER, "subprocess", SimpleNamespace(Popen=spawn))
    options = RUNNER.arguments(["--products", "1", "--output", str(output)])
    with pytest.raises(OSError) as caught:
        RUNNER.run(options)
    assert caught.value is injected
    report = json.loads(output.read_text())
    assert not report["ok"] and report["status"] == "preparation_error"
    assert report["error_type"] == "OSError"
    assert report["requested"]["products"] == 1
    assert report["cleanup"] == {"schema_removed": True, "working_directory_removed": True}
    assert not list(tmp_path.glob("retained-*"))
    assert os.environ["TEST_DATABASE_URL"] not in output.read_text()
    if failure in {"archive", "configuration"}:
        assert connections == []
    else:
        assert len(connections) == 2
        assert statements[0].startswith("CREATE SCHEMA ")
        assert statements[1].startswith("DROP SCHEMA ")
        schema = statements[0].split('"')[1]
        with original_connect(os.environ["TEST_DATABASE_URL"]) as connection:
            assert (
                connection.execute(
                    "SELECT 1 FROM pg_namespace WHERE nspname=%s", (schema,)
                ).fetchone()
                is None
            )
