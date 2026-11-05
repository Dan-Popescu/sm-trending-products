"""Native optional-work admission, source fences and reusable selected-video outputs."""

from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event

import pytest
from test_transcript_insights import selected

from sm_trending_products.contracts import now
from sm_trending_products.video_research import (
    ExecutionConfig,
    ResearchApproval,
    VideoRequest,
    VideoResearch,
    VideoResearchDenied,
    VideoSelection,
)

MEDIA = b"controlled-selected-video-fixture"


class FixtureExecutor:
    """Exercises persistence/authority with deterministic, explicitly synthetic outputs."""

    def __init__(self, callback=None):
        self.calls = []
        self.callback = callback
        self.ready = 0

    def preflight(self, request, config):
        self.ready += 1

    def execute(self, payload, directory, config):
        self.calls.append(payload)
        if self.callback:
            self.callback()
        operation = payload["request"]["operation"]
        if operation == "comments":
            return {
                "comments": [
                    {
                        "comment_id": str(index),
                        "video_id": "123",
                        "text": "Où acheter ? Quel prix et livraison ?",
                    }
                    for index in range(1, 4)
                ]
            }
        if operation == "download":
            (directory / "video.mp4").write_bytes(MEDIA)
            return {"video_id": "123", "duration_seconds": 30}
        return {
            "model": "large-v3",
            "model_sha256": config.model_sha256,
            "media_sha256": payload["media_sha256"],
            "language": "fr",
            "text": "Cela permet de gagner du temps.",
            "engine_version": "fixture-v1",
            "segments": [{"start": 0, "end": 2, "text": "Cela permet de gagner du temps."}],
        }


def setup(pipeline, tmp_path, *, executor=None):
    _, transcript = selected(pipeline, entity="123")
    selection = VideoSelection.model_validate(
        transcript.model_dump(exclude={"text", "source_kind", "language", "context"})
    )
    config = ExecutionConfig(
        enabled=True,
        allow_download=True,
        allow_unofficial=True,
        allow_transcription=True,
        work_dir=tmp_path / "private-video-research",
        model_sha256="a" * 64,
        research=ResearchApproval(
            reference="synthetic-approved-fixture",
            project_id="fixture",
            expires_at=now() + timedelta(days=1),
        ),
    )
    executor = executor if executor is not None else FixtureExecutor()
    service = VideoResearch(pipeline.catalog, pipeline.provenance, executor=executor)
    service.initialize()
    return service, selection, config, executor


def request(selection, *, action="comments", identifier="run-1", **kwargs):
    return VideoRequest(
        request_id=identifier,
        selection=selection,
        operation=action,
        video_url="https://www.tiktok.com/@fixture/video/123",
        **kwargs,
    )


def test_comments_both_backends_share_guarded_schema_but_not_request_identity(pipeline, tmp_path):
    service, selection, config, executor = setup(pipeline, tmp_path)
    first = service.run(request(selection), config)
    second = service.run(request(selection, identifier="research-1", provider="research"), config)
    assert first["job_id"] != second["job_id"]
    assert first["result"]["provider"] == "unofficial"
    assert second["result"]["provider"] == "research"
    assert len(executor.calls) == 2
    assert first["result"]["captured_comments"] == 3
    assert first["result"]["insights"]["counts"]["price_question_candidate"] == 3
    assert first["result"]["insights"]["measured_purchases"] is None
    assert first["result"]["total_comments_on_platform"] is None


def test_retry_and_alias_reuse_without_sdk_or_budget_reset(pipeline, tmp_path):
    service, selection, config, executor = setup(pipeline, tmp_path)
    first = service.run(request(selection), config)
    again = service.run(request(selection), config)
    alias = service.run(request(selection, identifier="alias-1"), config)
    assert again["reused"] and alias["reused"]
    assert first["job_id"] == again["job_id"] == alias["job_id"]
    assert executor.ready == len(executor.calls) == 1
    assert (
        pipeline.catalog.one("SELECT operations_reserved FROM video_research_budgets")[
            "operations_reserved"
        ]
        == 1
    )
    with pytest.raises(VideoResearchDenied, match="identity_conflict"):
        service.run(request(selection, refresh_key="next"), config)


@pytest.mark.parametrize("field", ["enabled", "allow_unofficial"])
def test_disabled_work_never_reserves_or_executes(pipeline, tmp_path, field):
    service, selection, config, executor = setup(pipeline, tmp_path)
    config = config.model_copy(update={field: False})
    with pytest.raises(VideoResearchDenied, match="disabled"):
        service.run(request(selection), config)
    assert executor.calls == []
    assert pipeline.catalog.one("SELECT count(*) AS n FROM video_research_requests")["n"] == 0


@pytest.mark.parametrize("approval", ["absent", "expired"])
def test_official_approval_is_not_inferred_from_synthetic_success(pipeline, tmp_path, approval):
    service, selection, config, executor = setup(pipeline, tmp_path)
    grant = (
        None
        if approval == "absent"
        else config.research.model_copy(update={"expires_at": now() - timedelta(seconds=1)})
    )
    config = config.model_copy(update={"research": grant})
    with pytest.raises(VideoResearchDenied, match="research_approval_required"):
        service.run(request(selection, provider="research"), config)
    assert executor.calls == []


def test_daily_budget_cannot_be_reset_by_alias_configuration_or_provider(pipeline, tmp_path):
    service, selection, config, executor = setup(pipeline, tmp_path)
    config = config.model_copy(update={"max_daily_operations": 1})
    service.run(request(selection), config)
    with pytest.raises(VideoResearchDenied, match="daily_budget_exhausted"):
        service.run(request(selection, identifier="next", refresh_key="next"), config)
    with pytest.raises(VideoResearchDenied, match="daily_configuration_already_defined"):
        service.run(
            request(selection, identifier="raised", refresh_key="next"),
            config.model_copy(update={"max_daily_operations": 10}),
        )
    assert len(executor.calls) == 1


def test_uncertain_result_cannot_trigger_automatic_second_request(pipeline, tmp_path):
    def fail():
        raise OSError("remote private diagnostics must not be stored")

    service, selection, config, executor = setup(pipeline, tmp_path, executor=FixtureExecutor(fail))
    with pytest.raises(OSError):
        service.run(request(selection), config)
    with pytest.raises(VideoResearchDenied, match="uncertain_no_automatic_retry"):
        service.run(request(selection, identifier="alias"), config)
    row = pipeline.catalog.one("SELECT * FROM video_research_jobs")
    assert row["state"] == "uncertain" and row["descriptor"] is None
    assert not (config.work_dir / row["job_id"]).exists()
    assert len(executor.calls) == 1


def test_completed_and_inflight_results_obey_current_ownership(pipeline, tmp_path):
    service, selection, config, executor = setup(pipeline, tmp_path)
    service.run(request(selection), config)
    pipeline.create_product("other", "Other", synthetic=True)
    pipeline.review_link("other", selection.entity_key)
    with pytest.raises(VideoResearchDenied, match="not_owned"):
        service.result("run-1", "p", config, synthetic=True)
    with pytest.raises(VideoResearchDenied, match="not_owned"):
        service.run(request(selection, identifier="alias"), config)
    assert len(executor.calls) == 1


def test_withdrawal_during_external_execution_never_publishes_result(pipeline, tmp_path):
    entered, release = Event(), Event()

    def wait():
        entered.set()
        assert release.wait(5)

    service, selection, config, _ = setup(pipeline, tmp_path, executor=FixtureExecutor(wait))
    with ThreadPoolExecutor(max_workers=1) as pool:
        task = pool.submit(service.run, request(selection), config)
        try:
            assert entered.wait(5)
            # This mutation completes while the provider is blocked: no source DB lock is held.
            pipeline.catalog.invalidate_dataset(selection.dataset_id, "source_removed")
        finally:
            release.set()
        with pytest.raises(VideoResearchDenied, match="source_unavailable"):
            task.result(timeout=5)
    job = pipeline.catalog.one("SELECT * FROM video_research_jobs")
    assert job["state"] == "uncertain" and job["descriptor"] is None
    assert not (config.work_dir / job["job_id"]).exists()


def test_private_workspace_allows_only_one_concurrent_worker(pipeline, tmp_path):
    entered, release = Event(), Event()

    def wait():
        entered.set()
        assert release.wait(5)

    service, selection, config, executor = setup(pipeline, tmp_path, executor=FixtureExecutor(wait))
    with ThreadPoolExecutor(max_workers=1) as pool:
        task = pool.submit(service.run, request(selection), config)
        try:
            assert entered.wait(5)
            with pytest.raises(VideoResearchDenied, match="worker_busy"):
                service.run(request(selection, identifier="parallel"), config)
        finally:
            release.set()
        task.result(timeout=5)
    assert len(executor.calls) == 1


def test_comment_pages_do_not_hydrate_unselected_text_or_cross_product(pipeline, tmp_path):
    service, selection, config, _ = setup(pipeline, tmp_path)
    service.run(request(selection), config)
    first = service.result("run-1", "p", config, synthetic=True, limit=2)
    second = service.result(
        "run-1", "p", config, synthetic=True, limit=2, after=first["next_after"]
    )
    assert [row["comment_id"] for row in first["comments"]] == ["1", "2"]
    assert [row["comment_id"] for row in second["comments"]] == ["3"]
    assert second["next_after"] is None
    with pytest.raises(VideoResearchDenied, match="not_found"):
        service.result("run-1", "other-product", config, synthetic=True)


def test_download_export_transcribe_and_existing_insights_keep_machine_provenance(
    pipeline, tmp_path
):
    service, selection, config, executor = setup(pipeline, tmp_path)
    media = service.run(request(selection, action="download"), config)
    output = tmp_path / "operator-video.mp4"
    exported = service.export_media(media["job_id"], selection, config, output)
    assert output.read_bytes() == MEDIA
    assert exported["sha256"] == hashlib.sha256(MEDIA).hexdigest()
    with pytest.raises(FileExistsError):
        service.export_media(media["job_id"], selection, config, output)
    speech = request(
        selection, action="transcribe", identifier="speech-1", media_job_id=media["job_id"]
    )
    result = service.run(speech, config)
    assert result["result"]["model"] == "large-v3"
    assert result["result"]["source_kind"] == "machine_transcript"
    assert not result["result"]["speech_verified"]
    imported = service.import_transcription("speech-1", "p", config, synthetic=True)
    assert imported["source_kind"] == "machine_transcript"
    stored = pipeline.catalog.one(
        "SELECT metadata FROM enrichment_transcripts WHERE transcript_id=%s",
        (imported["transcript_id"],),
    )["metadata"]
    assert stored["machine_origin"]["model"] == "large-v3"
    assert stored["machine_origin"]["media_sha256"] == hashlib.sha256(MEDIA).hexdigest()
    assert len(executor.calls) == 2


def test_tampered_cached_media_is_not_transcribed_or_exported(pipeline, tmp_path):
    service, selection, config, executor = setup(pipeline, tmp_path)
    media = service.run(request(selection, action="download"), config)
    (config.work_dir / media["job_id"] / "video.mp4").write_bytes(b"corrupted")
    with pytest.raises(VideoResearchDenied, match="checksum_mismatch"):
        service.run(
            request(
                selection, action="transcribe", identifier="speech", media_job_id=media["job_id"]
            ),
            config,
        )
    assert len(executor.calls) == 1


def test_selected_video_listing_is_metadata_only_and_requires_explicit_permission(
    pipeline, tmp_path
):
    service, selection, _, executor = setup(pipeline, tmp_path)
    output = service.videos("p", synthetic=True)
    assert output["videos"][0]["selection"]["entity_key"] == selection.entity_key
    assert "permission_reference" not in output["videos"][0]["selection"]
    assert executor.calls == []


def test_full_workspace_still_reuses_completed_results(pipeline, tmp_path):
    service, selection, config, executor = setup(pipeline, tmp_path)
    original = service.run(request(selection), config)
    constrained = config.model_copy(update={"max_workspace_bytes": 1024, "max_media_bytes": 1024})
    again = service.run(request(selection, identifier="alias"), constrained)
    assert again["reused"] and again["job_id"] == original["job_id"]
    assert len(executor.calls) == 1


def test_listing_redacts_source_text_after_policy_withdrawal(pipeline, tmp_path):
    _, transcript = selected(pipeline, entity="123", source="tiktok_display", synthetic=False)
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,verified) VALUES('tiktok_display',true)"
    )
    pipeline.catalog.execute(
        "UPDATE entities SET title='private title',attributes=attributes||"
        '\'{"share_url":"https://www.tiktok.com/@fixture/video/123"}\'::jsonb '
        "WHERE entity_key=%s",
        (transcript.entity_key,),
    )
    service = VideoResearch(pipeline.catalog, pipeline.provenance)
    visible = service.videos("live-p")["videos"][0]
    assert visible["available"] and visible["title"] == "private title"
    pipeline.catalog.execute(
        "UPDATE source_policies SET verified=false,retention_days=1 WHERE source='tiktok_display'"
    )
    hidden = service.videos("live-p")["videos"][0]
    assert (
        not hidden["available"]
        and hidden["unavailable_reason"] == "enrichment_source_policy_unverified"
    )
    assert hidden["title"] is None and hidden["video_url"] is None
