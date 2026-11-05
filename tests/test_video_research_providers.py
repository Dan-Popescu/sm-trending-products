"""Provider boundaries use controlled responses, never third-party access."""

import hashlib
import io
import json
import sys
from datetime import timedelta
from types import SimpleNamespace

import pytest

from sm_trending_products.contracts import canonical, now
from sm_trending_products.video_research import providers
from sm_trending_products.video_research.cli import main
from sm_trending_products.video_research.contracts import (
    ExecutionConfig,
    ResearchApproval,
    VideoRequest,
    VideoResearchDenied,
    VideoSelection,
)
from sm_trending_products.video_research.execution import (
    ProcessExecutor,
    admit_workspace,
    workspace,
)


def inputs(tmp_path, *, operation="comments", provider="research"):
    config = ExecutionConfig(
        work_dir=tmp_path / "private",
        research=ResearchApproval(
            reference="approved-test", project_id="test", expires_at=now() + timedelta(days=1)
        ),
    )
    request = VideoRequest(
        request_id="request-1",
        operation=operation,
        provider=provider,
        selection=VideoSelection(
            product_id="product",
            entity_key="tiktok:video:123",
            dataset_id="dataset",
            observation_id="observation",
            permission_reference="test-permission",
        ),
        video_url="https://www.tiktok.com/@fixture/video/123",
        media_job_id="b" * 64 if operation == "transcribe" else None,
    )
    return request, config


class Responses:
    def __init__(self, pages):
        self.pages, self.requests = iter(pages), []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        return io.BytesIO(json.dumps(next(self.pages)).encode())


def page(rows, cursor=1, more=False):
    return {"error": {"code": "ok"}, "data": {"comments": rows, "cursor": cursor, "has_more": more}}


def row(identifier="1", **changes):
    return {
        "id": identifier,
        "video_id": "123",
        "text": "Quel prix ? user@example.org",
        "author": "not persisted",
        **changes,
    }


def test_official_fixed_origin_auth_page_limits_and_minimal_fields(tmp_path, monkeypatch):
    request, config = inputs(tmp_path)
    monkeypatch.setenv("TIKTOK_RESEARCH_ACCESS_TOKEN", "test-token")
    client = Responses([page([row()], more=True), page([row("2")], cursor=2)])
    output = providers.research_comments(request, config, opener=client)
    assert output["pages_read"] == 2 and output["coverage"] == "returned_pages_exhausted"
    assert output["comments"][0]["text"] == "Quel prix ? [email]"
    assert "author" not in output["comments"][0]
    for message, timeout in client.requests:
        assert message.full_url == providers.RESEARCH_URL + "?fields=" + providers.COMMENT_FIELDS
        assert message.get_header("Authorization") == "Bearer test-token"
        assert message.method == "POST" and timeout <= 20
    assert json.loads(client.requests[0][0].data) == {
        "video_id": 123,
        "max_count": 100,
        "cursor": 0,
    }
    assert json.loads(client.requests[1][0].data)["cursor"] == 1


@pytest.mark.parametrize(
    "response,reason",
    [
        (page([row(video_id="456")]), "video_mismatch"),
        (page([row()], cursor=0, more=True), "cursor_did_not_advance"),
        (page([row(), row(text="changed")]), "changed_within_sample"),
        (page([], more="yes"), "pagination"),
        ({"error": {"code": "access_denied"}}, "request_refused"),
    ],
)
def test_official_refuses_wrong_or_ambiguous_responses(tmp_path, monkeypatch, response, reason):
    request, config = inputs(tmp_path)
    monkeypatch.setenv("TIKTOK_RESEARCH_ACCESS_TOKEN", "test-token")
    with pytest.raises(VideoResearchDenied, match=reason):
        providers.research_comments(request, config, opener=Responses([response]))


def test_official_stops_at_page_budget_and_does_not_follow_redirect(tmp_path, monkeypatch):
    request, config = inputs(tmp_path)
    monkeypatch.setenv("TIKTOK_RESEARCH_ACCESS_TOKEN", "test-token")
    client = Responses([page([row()], more=True)])
    output = providers.research_comments(
        request, config.model_copy(update={"max_pages": 1}), opener=client
    )
    assert output["has_more"] and output["coverage"] == "bounded_sample"
    with pytest.raises(VideoResearchDenied, match="redirect_refused"):
        providers._NoRedirect().redirect_request(None, None, 302, "", {}, "http://127.0.0.1")


def test_unofficial_is_one_bounded_session_with_unknown_coverage(tmp_path, monkeypatch):
    request, config = inputs(tmp_path, provider="unofficial")
    calls = {}

    class Video:
        async def comments(self, count):
            calls["count"] = count
            for index in range(10):
                yield SimpleNamespace(
                    as_dict={"cid": str(index + 1), "aweme_id": "123", "text": "Où acheter ?"}
                )

    class Api:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            calls["closed"] = True

        async def create_sessions(self, **kwargs):
            calls["sessions"] = kwargs

        def video(self, **kwargs):
            calls["video"] = kwargs
            return Video()

    monkeypatch.setitem(sys.modules, "TikTokApi", SimpleNamespace(TikTokApi=Api))
    output = providers.unofficial_comments(request, config.model_copy(update={"max_comments": 2}))
    assert len(output["comments"]) == 2 and output["pages_read"] is None
    assert output["coverage"] == "bounded_sample" and output["total_comments_on_platform"] is None
    assert calls["sessions"]["num_sessions"] == 1
    assert calls["sessions"]["enable_session_recovery"] is False
    assert calls["closed"] and calls["count"] == 2


def test_large_v3_local_model_identity_and_machine_timestamps(tmp_path, monkeypatch):
    request, config = inputs(tmp_path, operation="transcribe")
    model_dir = tmp_path / "provisioned-large-v3"
    model_dir.mkdir()
    (model_dir / "model.bin").write_bytes(b"controlled-model-not-real-weights")
    checksum = hashlib.sha256((model_dir / "model.bin").read_bytes()).hexdigest()
    config = config.model_copy(update={"model_dir": model_dir, "model_sha256": checksum})
    media_path = tmp_path / "selected.mp4"
    media_path.write_bytes(b"selected-media")
    media_sha = hashlib.sha256(b"selected-media").hexdigest()
    calls = {}

    class Model:
        def __init__(self, path, **kwargs):
            calls.update(path=path, **kwargs)

        def transcribe(self, path, **kwargs):
            calls["speech"] = kwargs
            return iter([SimpleNamespace(start=0.0, end=1.2, text="Bonjour")]), SimpleNamespace(
                duration=2, language="fr"
            )

    monkeypatch.setitem(sys.modules, "faster_whisper", SimpleNamespace(WhisperModel=Model))
    monkeypatch.setattr(providers, "version", lambda name: "1.2.1")
    output = providers.transcribe_media(
        {
            "media_path": str(media_path),
            "media_sha256": media_sha,
            "request": request.model_dump(mode="json"),
        },
        config,
    )
    assert calls["path"] == str(model_dir) and calls["local_files_only"] is True
    assert calls["device"] == "cpu" and calls["compute_type"] == "int8"
    assert output["model"] == "large-v3" and output["source_kind"] == "machine_transcript"
    assert output["segments"][0]["end"] == 1.2 and output["speech_verified"] is False
    with pytest.raises(VideoResearchDenied, match="checksum_mismatch"):
        providers.transcribe_media(
            {"media_path": str(media_path), "media_sha256": media_sha},
            config.model_copy(update={"model_sha256": "f" * 64}),
        )
    original = Model.transcribe

    def mutate(self, path, **kwargs):
        media_path.write_bytes(b"changed-during-asr")
        return original(self, path, **kwargs)

    monkeypatch.setattr(Model, "transcribe", mutate)
    with pytest.raises(VideoResearchDenied, match="media_changed_during_transcription"):
        providers.transcribe_media(
            {
                "media_path": str(media_path),
                "media_sha256": media_sha,
                "request": request.model_dump(mode="json"),
            },
            config,
        )


def test_prepare_and_doctor_are_offline_and_do_not_open_database(tmp_path, monkeypatch, capsys):
    request, config = inputs(tmp_path)
    request_path, config_path = tmp_path / "request.json", tmp_path / "config.json"
    request_path.write_text(canonical(request.model_dump(mode="json")))
    config_path.write_text(canonical(config.model_dump(mode="json")))
    monkeypatch.setenv("DATABASE_URL", "invalid-never-connect")
    assert main(["run", "--request", str(request_path), "--config", str(config_path)]) == 0
    assert json.loads(capsys.readouterr().out)["external_execution"] is False
    assert main(["doctor", "--config", str(config_path)]) == 0
    assert json.loads(capsys.readouterr().out)["live_access_verified"] is False


def test_worker_uses_bounded_result_file_and_redacts_unknown_errors(tmp_path):
    request, config = inputs(tmp_path)
    directory = config.work_dir / "worker"
    directory.mkdir(mode=0o700, parents=True)
    # No token is forwarded. This executes the real isolated worker without network.
    with pytest.raises(VideoResearchDenied, match="research_token_required"):
        ProcessExecutor().execute({"request": request.model_dump(mode="json")}, directory, config)
    assert not (directory / "video.mp4").exists()
    assert b"token_required" in (directory / "result.json").read_bytes()


def test_capacity_is_separate_from_cached_workspace_access(tmp_path):
    _, config = inputs(tmp_path)
    config = config.model_copy(update={"max_workspace_bytes": 1024, "max_media_bytes": 1024})
    with workspace(config):
        with pytest.raises(VideoResearchDenied, match="workspace_capacity"):
            admit_workspace(config)
    with workspace(config) as root:
        assert root == config.work_dir


@pytest.mark.parametrize("failure", ["timeout", "memory", "disk"])
def test_supervisor_stops_owned_worker_and_child_on_limits(tmp_path, monkeypatch, failure):
    import os
    import subprocess
    import time

    from sm_trending_products.video_research import execution

    request, config = inputs(tmp_path)
    config = config.model_copy(update={"timeout_seconds": 1, "memory_mb": 512})
    directory = config.work_dir / "worker"
    directory.mkdir(mode=0o700, parents=True)
    pid_file = directory / "owned-child"
    script = (
        "import os,signal,subprocess,sys,time; "
        "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
        "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'], "
        "start_new_session=True); "
        f"open({str(pid_file)!r},'w').write(str(child.pid)); "
        "sys.stdin.buffer.read(); time.sleep(60)"
    )
    original = subprocess.Popen
    workers = []

    def controlled(_arguments, **kwargs):
        process = original([sys.executable, "-c", script], **kwargs)
        workers.append(process)
        return process

    monkeypatch.setattr(execution.subprocess, "Popen", controlled)
    original_session = execution._session
    if failure == "memory":

        def excessive(pid, known=None):
            members = original_session(pid, known)
            if pid_file.exists():
                return {key: (value[0], 600 * 1024**2) for key, value in members.items()}
            return members

        monkeypatch.setattr(execution, "_session", excessive)
    if failure == "disk":
        original_inventory = execution._inventory
        monkeypatch.setattr(
            execution,
            "_inventory",
            lambda root: (
                (config.max_workspace_bytes + 1, 1)
                if pid_file.exists()
                else original_inventory(root)
            ),
        )
    started = time.monotonic()
    with pytest.raises(
        VideoResearchDenied,
        match={
            "timeout": "timeout",
            "memory": "memory_or_process_limit",
            "disk": "workspace_runtime_limit",
        }[failure],
    ):
        ProcessExecutor().execute({"request": request.model_dump(mode="json")}, directory, config)
    assert time.monotonic() - started < 5
    assert workers[0].poll() is not None
    child = int(pid_file.read_text())
    # A dead orphan may remain a zombie until the environment's init reaps it.
    if (child_stat := __import__("pathlib").Path(f"/proc/{child}/stat")).exists():
        assert child_stat.read_text().rsplit(")", 1)[1].split()[0] == "Z"
    assert not hasattr(os, "waitpid") or workers[0].returncode is not None


def test_host_admission_refuses_insufficient_headroom_before_execution(tmp_path, monkeypatch):
    from sm_trending_products import release_capacity
    from sm_trending_products.video_research import execution

    request, config = inputs(tmp_path)
    monkeypatch.setattr(release_capacity, "_host_limits", lambda: {"memory_available_bytes": 512})
    monkeypatch.setattr(execution.importlib.util, "find_spec", lambda _name: True)
    with pytest.raises(VideoResearchDenied, match="memory_headroom_required"):
        ProcessExecutor().preflight(request, config)


def test_dashboard_load_is_metadata_only_and_action_is_explicit(tmp_path):
    from streamlit.testing.v1 import AppTest

    _, config = inputs(tmp_path)
    path = tmp_path / "config.json"
    path.write_text(canonical(config.model_dump(mode="json")))
    source = """
import streamlit as st
from sm_trending_products.video_research.dashboard import render
class Service:
    def videos(self, product, **kwargs):
        selection = {
            "product_id": product, "entity_key": "tiktok:video:123",
            "dataset_id": "dataset", "observation_id": "observation", "synthetic": False
        }
        return {"videos": [{"selection": selection, "title": "Selected video",
            "video_url": "https://www.tiktok.com/@fixture/video/123"}], "next_after": None}
    def run(self, request, config):
        st.session_state["executed"] = request.model_dump(mode="json")
        return {"state": "controlled"}
class Pipeline:
    def video_research(self):
        return Service()
render(Pipeline(), [{"product_id": "product"}], False)
"""
    app = AppTest.from_string(source).run(timeout=20)
    assert not app.exception and "executed" not in app.session_state
    for widget in app.text_input:
        if widget.label == "Fichier privé de configuration des enrichissements":
            widget.set_value(str(path))
        elif widget.label == "Référence de permission pour ces vidéos":
            widget.set_value("test-permission")
        elif widget.label == "Identifiant stable de la demande":
            widget.set_value("sample-1")
    app.button[0].click().run(timeout=20)
    assert not app.exception
    assert app.session_state["executed"]["operation"] == "comments"
    assert app.session_state["executed"]["selection"]["permission_reference"] == "test-permission"


@pytest.mark.parametrize("terminal", [True, False])
def test_empty_process_snapshot_rechecks_exit_and_still_refuses_lost_tracking(
    tmp_path, monkeypatch, terminal
):
    from sm_trending_products.video_research import execution

    request, config = inputs(tmp_path)
    directory = config.work_dir / "worker"
    directory.mkdir(mode=0o700, parents=True)
    (directory / "result.json").write_text('{"ok":false,"reason":"research_token_required"}')

    class Worker:
        pid = 999999999
        stdin = io.BytesIO()
        returncode = None
        polls = 0

        def poll(self):
            self.polls += 1
            if terminal and self.polls >= 3:
                self.returncode = 0
            return self.returncode

    worker = Worker()
    snapshots = 0
    stopped = []

    def snapshot(pid, known=None):
        nonlocal snapshots
        snapshots += 1
        return {worker.pid: (1, 1)} if snapshots == 1 else {}

    monkeypatch.setattr(execution.subprocess, "Popen", lambda *_args, **_kwargs: worker)
    monkeypatch.setattr(execution, "_session", snapshot)
    monkeypatch.setattr(execution, "_stop", lambda process, _known: stopped.append(process.pid))
    reason = "research_token_required" if terminal else "memory_or_process_limit"
    with pytest.raises(VideoResearchDenied, match=reason):
        ProcessExecutor().execute({"request": request.model_dump(mode="json")}, directory, config)
    assert stopped == [worker.pid]
