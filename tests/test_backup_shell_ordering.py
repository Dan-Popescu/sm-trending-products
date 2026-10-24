"""Real capture script and HTTP/SQL, with controlled service/dump process adapters."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from sm_trending_products.contracts import now

ROOT = Path(__file__).parents[1]


@pytest.mark.parametrize("online", [False, True])
def test_actual_shell_orders_checkpoint_restart_and_real_conditional_copy(
    pipeline, tmp_path, monkeypatch, online
):
    stamp = now().isoformat()
    pipeline.import_records(
        [
            {
                "source": "shell_backup_fixture",
                "entity_type": "video",
                "entity_id": "v",
                "synthetic": True,
                "observed_at": stamp,
                "available_at": stamp,
                "metrics": {},
            }
        ],
        synthetic=True,
    )
    binary = tmp_path / "bin"
    binary.mkdir()
    fake = binary / "docker"
    fake.write_text(
        "#!"
        + sys.executable
        + "\n"
        + """import json,os,runpy,sys,time
from pathlib import Path
args=sys.argv[1:]
state=Path(os.environ['PROBE_STATE'])
trace=Path(os.environ['PROBE_TRACE'])
def note(operation):
    with trace.open('a') as out:
        item={'operation':operation,'time':time.monotonic()}
        out.write(json.dumps(item)+'\\n')
if args[0]=='compose':
    args=args[1:]
    command=args[0]
    if command=='config': pass
    elif command=='ps':
        print('worker\\nworker-green\\ncapture-planner\\nnormalizer\\ndagster-daemon\\ndashboard')
    elif command in ('stop','start'):
        expected={'worker','worker-green','capture-planner','normalizer','dagster-daemon','dashboard'}
        assert expected<=set(args[1:])
        state.write_text('stopped' if command=='stop' else 'running')
        note(command)
    elif command=='exec':
        assert state.read_text()==('running' if os.environ['PROBE_ONLINE']=='true' else 'stopped')
        if any(item.startswith('BACKUP_SNAPSHOT_ID=') for item in args):
            assert '--snapshot="$BACKUP_SNAPSHOT_ID"' in args[-1]
            from sm_trending_products.catalog import Catalog
            with_snapshot=next(
                item.split('=',1)[1] for item in args if item.startswith('BACKUP_SNAPSHOT_ID=')
            )
            catalog=Catalog(os.environ['DATABASE_URL'])
            try: catalog.export_state(with_snapshot)
            finally: catalog.close()
        note('dump')
        sys.stdout.buffer.write(b'controlled-native-checkpoint-dump')
    elif command=='run' and 'scripts/object_snapshot.py' in args:
        mapping=args[args.index('--volume')+1].split(':',1)[0]
        command_args=args[args.index('scripts/object_snapshot.py')+1:]
        command_args=[item.replace('/backups/',mapping+'/') for item in command_args]
        ns=runpy.run_path(str(Path(os.environ['PROBE_ROOT'])/'scripts/object_snapshot.py'))
        make=ns['client']
        def checked_client():
            client=make()
            def check(**kwargs):
                if state.read_text()!='running':
                    raise RuntimeError('object_copy_before_writer_restart')
                note('get_object')
            client.meta.events.register('before-call.s3.GetObject',check)
            def inspect(**kwargs):
                if state.read_text()!='running':
                    raise RuntimeError('object_head_before_writer_restart')
                note('head_object')
            client.meta.events.register('before-call.s3.HeadObject',inspect)
            return client
        ns['capture'].__globals__['client']=checked_client
        sys.argv=['object_snapshot.py']+command_args
        ns['main']()
    elif command=='run' and 'python' in args:
        assert state.read_text()=='stopped'
        note('catalog')
        exec(compile(sys.stdin.read(),'<checkpoint>','exec'))
    else: raise RuntimeError('Unexpected docker compose invocation')
else: raise RuntimeError('Unexpected docker invocation')
"""
    )
    fake.chmod(0o755)
    family = tmp_path / "backups"
    family.mkdir()
    state = tmp_path / "state"
    state.write_text("running")
    trace = tmp_path / "trace.jsonl"
    for key, value in {
        "PATH": str(binary) + ":" + os.environ["PATH"],
        "PROBE_ROOT": str(ROOT),
        "PROBE_STATE": str(state),
        "PROBE_TRACE": str(trace),
        "DATABASE_URL": pipeline.settings.database_url.get_secret_value(),
        "S3_ENDPOINT_URL": pipeline.settings.s3_endpoint_url,
        "S3_BUCKET": pipeline.history.bucket,
        "PIPELINE_NAMESPACE": pipeline.settings.pipeline_namespace,
        "CACHE_DIR": str(tmp_path / "cache"),
        "SPOOL_DIR": str(tmp_path / "spool"),
        "PYTHONPATH": str(ROOT / "src"),
        "BACKUP_DEADLINE_SECONDS": "20",
        "PROBE_ONLINE": "true" if online else "false",
    }.items():
        monkeypatch.setenv(key, value)
    full = subprocess.run(
        [
            "bash",
            "scripts/backup.sh",
            str(family / "base"),
            "--copy-objects",
            "--online" if online else "--offline",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert full.returncode == 0, full.stdout + full.stderr
    lines = [json.loads(line)["operation"] for line in trace.read_text().splitlines()]
    if online:
        assert not {"stop", "start", "catalog"} & set(lines)
        assert lines.index("dump") < lines.index("head_object") < lines.index("get_object")
    else:
        assert lines[0] == "stop"
        assert (
            lines.index("catalog")
            < lines.index("dump")
            < lines.index("start")
            < lines.index("head_object")
            < lines.index("get_object")
        )
    assert lines.count("get_object") == 3
    assert json.loads((family / "base/backup.json").read_text())["complete"]
    trace.write_text("")
    delta = subprocess.run(
        [
            "bash",
            "scripts/backup.sh",
            str(family / "delta"),
            "--copy-objects",
            "--base",
            str(family / "base"),
            "--online" if online else "--offline",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert delta.returncode == 0, delta.stdout + delta.stderr
    lines = [json.loads(line)["operation"] for line in trace.read_text().splitlines()]
    assert "get_object" not in lines and "head_object" not in lines
    assert lines[-1] == ("dump" if online else "start")
    metadata = json.loads((family / "delta/backup.json").read_text())
    assert metadata["application_checkpoint"] == (
        "exported_snapshot" if online else "paused_writers"
    )
    assert metadata["dagster_checkpoint"] == "independent_database_snapshot"
    result = json.loads((family / "delta/object-summary.json").read_text())
    assert (
        result["downloaded_bytes"] == result["copied_object_count"] == result["head_requests"] == 0
    )
    assert result["reused_object_count"] == 3
    print(
        json.dumps(
            {
                "full_gets": 3,
                "unchanged_incremental_gets": 0,
                "checkpoint_then_restart_then_copy": True,
                "postgres_checkpoint_fingerprints": "native",
                "object_copy": "nativeHTTPMoto",
                "docker_service_control_and_pg_dump": "simulated",
            }
        )
    )
