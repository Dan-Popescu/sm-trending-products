"""Native catalog checks protect acquisition while replayable services refresh."""

import hashlib
import json

import pytest
from psycopg.types.json import Jsonb
from test_release_control import release

from sm_trending_products.capture_coordinator import CaptureCoordinator
from sm_trending_products.release_control import ReleaseBlocked, VerifiedRelease
from sm_trending_products.release_host import ComposeReleaseHost
from sm_trending_products.release_processing import (
    PLANNERS,
    PROCESSING_SERVICES,
    ROLE_VARIABLES,
    ProcessingRefresh,
)
from sm_trending_products.worker_control import WorkerControl

pytestmark = pytest.mark.integration
PROOF = {"method": "cosign_bundle", "bundle": {"accepted": True}}


class FixtureTrust:
    def verify(self, manifest, proof):
        if proof != PROOF:
            raise ReleaseBlocked("release_signature_invalid")
        return VerifiedRelease(manifest, proof)


@pytest.fixture
def processing(pipeline, tmp_path, monkeypatch):
    directory = tmp_path / "private"
    directory.mkdir(mode=0o700)
    active = release("a").manifest
    WorkerControl(pipeline.catalog).register("old", synthetic=True)
    pipeline.catalog.execute(
        "INSERT INTO deployment_targets(target,active_manifest,active_proof,active_slot,"
        "active_worker_id,synthetic) VALUES('fixture',%s,%s,'blue','old',true)",
        (Jsonb(active.model_dump(mode="json")), Jsonb(PROOF)),
    )
    host = ComposeReleaseHost(
        "fixture",
        tmp_path / "compose.yaml",
        tmp_path / "runtime.env",
        directory,
        {"blue": active.image, "green": active.image},
        {"blue": "old", "green": "unused"},
    )
    state = {
        service: {
            "image": active.image,
            "running": True,
            "worker": "old" if service == "worker" else None,
        }
        for service in ("worker", "capture-planner", *PROCESSING_SERVICES)
    }
    images, commands, probe = {active.image}, [], {"busy": False}

    def identifier(service):
        return hashlib.sha256(service.encode()).hexdigest()

    def command(self, args, **kwargs):
        commands.append(args)
        if args[0] == "pull":
            images.add(args[1])
            return ""
        if args[:2] == ["image", "inspect"]:
            if args[-1] not in images:
                raise ReleaseBlocked("deployment_host_command_failed")
            return json.dumps([args[-1]])
        if args[0] == "compose" and "ps" in args:
            return identifier(args[-1]) if args[-1] in state else ""
        if args[0] == "inspect":
            service = next(name for name in state if identifier(name) == args[-1])
            value = state[service]
            return "\n".join(json.dumps(value[key]) for key in ("image", "running", "worker"))
        if args[0] == "compose" and "up" in args:
            service = args[-1]
            assert service in (*PROCESSING_SERVICES, *PLANNERS.values())
            assert "--no-deps" in args and "--no-build" in args and "--pull" in args
            variable = ROLE_VARIABLES.get(service)
            if variable is None:
                slot = "BLUE" if service == "capture-planner" else "GREEN"
                variable = f"ACQUISITION_PLANNER_{slot}_IMAGE"
            state[service] = {"image": self.role_images[variable], "running": True, "worker": None}
            return ""
        if args[0] == "exec":
            assert args[2:] == ["python", "-m", "sm_trending_products.capture_coordinator"]
            result = (
                {"status": "coordinator_busy"}
                if probe["busy"]
                else CaptureCoordinator(pipeline).tick(True)
            )
            return json.dumps(result)
        if args[0] == "stop":
            service = next(name for name in state if identifier(name) == args[-1])
            assert service in PLANNERS.values() and "--time=-1" in args
            state[service]["running"] = False
            return ""
        raise AssertionError(args)

    monkeypatch.setattr(ComposeReleaseHost, "_command", command)
    refresh = ProcessingRefresh(
        pipeline.catalog, host, FixtureTrust(), directory, allow_synthetic=True
    )
    return refresh, state, commands, probe


def signed(character="b"):
    return VerifiedRelease(release(character).manifest, PROOF)


def test_refresh_overlaps_planner_and_keeps_capture_generation_quotas_and_raw_state(
    pipeline, processing
):
    refresh, state, commands, _ = processing
    before = pipeline.catalog.all("SELECT * FROM request_budgets ORDER BY source,account_id")
    result = refresh.apply(signed())
    assert result["status"] == "completed"
    assert result["completed_roles"] == list(PROCESSING_SERVICES)
    assert state["capture-planner-green"]["running"] and not state["capture-planner"]["running"]
    assert state["worker"]["running"] and state["worker"]["image"] == release("a").manifest.image
    assert WorkerControl(pipeline.catalog).status("old")["generation"] == 1
    assert (
        pipeline.catalog.all("SELECT * FROM request_budgets ORDER BY source,account_id") == before
    )
    starts = [args[-1] for args in commands if args[0] == "compose" and "up" in args]
    assert starts == ["capture-planner-green", *PROCESSING_SERVICES]
    stop = next(index for index, args in enumerate(commands) if args[0] == "stop")
    probe = next(index for index, args in enumerate(commands) if args[0] == "exec")
    assert probe < stop
    pins = (refresh.directory / "processing.env").read_text()
    assert "NORMALIZER_IMAGE=" + signed().manifest.image in pins
    assert refresh.journal.stat().st_mode & 0o077 == 0
    assert not refresh.pending()


def test_unqualified_planner_never_stops_predecessor_and_retry_resumes_private_intent(processing):
    refresh, state, commands, probe = processing
    probe["busy"] = True
    assert refresh.apply(signed())["reason"] == "processing_planner_busy"
    assert refresh.pending() and state["capture-planner"]["running"]
    assert not any(args[0] == "stop" for args in commands)
    starts = len([args for args in commands if args[0] == "compose" and "up" in args])
    probe["busy"] = False
    result = refresh.apply()
    assert result["status"] == "completed"
    assert len([args for args in commands if args[0] == "compose" and "up" in args]) == starts + 4


@pytest.mark.parametrize("change", ["stale", "draining", "generation"])
def test_acquisition_change_blocks_further_host_mutations(
    pipeline, processing, monkeypatch, change
):
    refresh, state, commands, _ = processing
    original = refresh._start

    def change_after_planner(service, *args):
        result = original(service, *args)
        if change == "stale":
            pipeline.catalog.execute(
                "UPDATE collection_workers SET heartbeat_at=clock_timestamp()-interval '1 hour'"
            )
        elif change == "draining":
            WorkerControl(pipeline.catalog).request_drain("old", 1)
        else:
            pipeline.catalog.execute("UPDATE collection_workers SET generation=2")
        return result

    monkeypatch.setattr(refresh, "_start", change_after_planner)
    result = refresh.apply(signed())
    assert result["status"] == "blocked"
    assert result["reason"] in {
        "processing_acquisition_changed",
        "processing_acquisition_unavailable",
    }
    assert state["capture-planner"]["running"]
    assert not any(args[0] == "stop" for args in commands)
    assert all(
        state[service]["image"] == release("a").manifest.image for service in PROCESSING_SERVICES
    )


@pytest.mark.parametrize("invalid", ["signature", "schema", "protocol"])
def test_untrusted_or_incompatible_manifest_is_rejected_before_mutation(processing, invalid):
    refresh, _, commands, _ = processing
    candidate = signed()
    if invalid == "signature":
        candidate = VerifiedRelease(candidate.manifest, {})
    else:
        values = (
            {"schema_min": 100, "schema_max": 100}
            if invalid == "schema"
            else {"capture_protocol": 2}
        )
        candidate = VerifiedRelease(candidate.manifest.model_copy(update=values), PROOF)
    with pytest.raises(ReleaseBlocked):
        refresh.apply(candidate)
    assert not any(args[0] in {"pull", "stop"} or "up" in args for args in commands)


def test_role_refresh_retries_exact_running_digests_without_recreating_them(processing):
    refresh, _, commands, _ = processing
    assert refresh.apply(signed())["status"] == "completed"
    previous = len([args for args in commands if args[0] == "compose" and "up" in args])
    assert refresh.apply()["status"] == "completed"
    assert len([args for args in commands if args[0] == "compose" and "up" in args]) == previous


def test_pending_private_intent_does_not_accept_an_unrelated_new_release(processing):
    refresh, _, _, probe = processing
    probe["busy"] = True
    assert refresh.apply(signed())["status"] == "blocked"
    with pytest.raises(ReleaseBlocked, match="processing_refresh_pending"):
        refresh.apply(signed("c"))
