"""Selected enrichment shares source authority, not acquisition scheduling or workers."""

from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path
from typing import Any, Protocol

from psycopg.types.json import Jsonb

from ..contracts import digest
from ..enrichment._eligibility import source_checks
from ..identity_rules import LINK_STATE_CTES
from ..module_schema import initialize_module, require_module
from .contracts import (
    CommentSample,
    ExecutionConfig,
    SpeechResult,
    VideoRequest,
    VideoResearchDenied,
    VideoSelection,
)
from .execution import ProcessExecutor, admit_workspace, file_hash, workspace
from .insights import describe_comments
from .schema import MIGRATIONS


class Executor(Protocol):
    def preflight(self, request: VideoRequest, config: ExecutionConfig) -> None: ...
    def execute(self, payload: dict, directory: Path, config: ExecutionConfig) -> dict: ...


class VideoResearch:
    def __init__(self, catalog, provenance, *, executor: Executor | None = None):
        self.catalog, self.provenance = catalog, provenance
        self.executor = executor if executor is not None else ProcessExecutor()

    def initialize(self) -> dict:
        return initialize_module(self.catalog, "video_research", MIGRATIONS)

    def _scope(self, connection, selection: VideoSelection):
        self.catalog.guard_namespace(connection, selection.synthetic)
        self.catalog.guard_retention(connection)
        clock = connection.execute("SELECT clock_timestamp() AS at").fetchone()["at"]
        item = source_checks(
            connection,
            selection.product_id,
            [(selection.entity_key, selection.dataset_id, selection.observation_id)],
            selection.synthetic,
            clock,
        )[0]
        if item["reason"]:
            raise VideoResearchDenied(item["reason"])
        return clock

    @staticmethod
    def _permission(request: VideoRequest, config: ExecutionConfig, clock, *, execute=True) -> None:
        if execute and not config.enabled:
            raise VideoResearchDenied("video_research_disabled")
        if execute and request.operation == "download" and not config.allow_download:
            raise VideoResearchDenied("media_download_disabled")
        if execute and request.operation == "transcribe" and not config.allow_transcription:
            raise VideoResearchDenied("transcription_disabled")
        if request.operation == "comments":
            if request.provider == "unofficial" and not config.allow_unofficial:
                raise VideoResearchDenied("unofficial_comments_disabled")
            if request.provider == "research" and (
                config.research is None or config.research.expires_at <= clock
            ):
                raise VideoResearchDenied("research_approval_required")

    def videos(
        self, product_id: str, *, synthetic: bool = False, after: str = "", limit: int = 20
    ) -> dict:
        if type(synthetic) is not bool or type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("Invalid selected-video page")
        if not isinstance(product_id, str) or not 0 < len(product_id) <= 128 or len(after) > 512:
            raise ValueError("Invalid product or cursor")
        with self.catalog.evidence_transaction(synthetic) as connection:
            self.catalog.guard_retention(connection)
            clock = connection.execute("SELECT clock_timestamp() AS at").fetchone()["at"]
            rows = connection.execute(
                "WITH "
                + LINK_STATE_CTES
                + " SELECT e.entity_key,e.title,e.attributes,d.dataset_id,d.observation_id "
                "FROM resolved_link_owners owner JOIN entities e USING(entity_key) "
                "JOIN LATERAL (SELECT n.dataset_id,o.observation_id FROM observation_deliveries o "
                "JOIN normalized_objects n USING(object_id) JOIN datasets d USING(dataset_id) "
                "WHERE o.entity_key=e.entity_key AND d.state='committed' AND d.synthetic=%s "
                "ORDER BY o.available_at DESC,o.observation_id,n.dataset_id LIMIT 1) d ON true "
                "WHERE owner.product_id=%s AND owner.confidence>=0.9 "
                "AND e.entity_type='video' AND e.synthetic=%s AND e.entity_key>%s "
                "ORDER BY e.entity_key LIMIT %s",
                (synthetic, clock, clock, synthetic, product_id, synthetic, after, limit + 1),
            ).fetchall()
            page = []
            for row in rows[:limit]:
                selection = VideoSelection(
                    product_id=product_id,
                    entity_key=row["entity_key"],
                    dataset_id=row["dataset_id"],
                    observation_id=row["observation_id"],
                    synthetic=synthetic,
                    permission_reference="operator-must-supply-permission",
                )
                reason = source_checks(
                    connection,
                    product_id,
                    [(selection.entity_key, selection.dataset_id, selection.observation_id)],
                    synthetic,
                    clock,
                )[0]["reason"]
                page.append(
                    {
                        "selection": selection.model_dump(
                            mode="json", exclude={"permission_reference"}
                        ),
                        "title": row["title"] if reason is None else None,
                        "video_url": row["attributes"].get("share_url") if reason is None else None,
                        "available": reason is None,
                        "unavailable_reason": reason,
                    }
                )
        return {
            "product_id": product_id,
            "videos": page,
            "next_after": rows[limit - 1]["entity_key"] if len(rows) > limit else None,
            "scope": "reviewed_product_videos_only",
        }

    def run(self, request: VideoRequest, config: ExecutionConfig) -> dict:
        request, config = (
            VideoRequest.model_validate(request),
            ExecutionConfig.model_validate(config),
        )
        require_module(self.catalog, "video_research")
        selection = request.selection
        # Recheck authority before even inspecting a local model or workspace.
        with self.catalog.evidence_transaction(selection.synthetic) as connection:
            self._permission(request, config, self._scope(connection, selection))
        payload: dict[str, Any] = {"request": request.model_dump(mode="json")}
        if request.operation == "transcribe":
            media = self._media_job(request.media_job_id, selection, config)
            payload.update(
                media_path=str(config.work_dir / media["job_id"] / "video.mp4"),
                media_sha256=media["result"]["sha256"],
            )
        processing = (
            {
                "max_comments": config.max_comments,
                "max_pages": config.max_pages,
                "project": config.research.project_id
                if config.research and request.provider == "research"
                else None,
                "approval": config.research.reference
                if config.research and request.provider == "research"
                else None,
            }
            if request.operation == "comments"
            else {
                "model": config.model,
                "weights": config.model_sha256,
                "precision": config.compute_type,
            }
            if request.operation == "transcribe"
            else {}
        )
        content_key = digest(
            {
                "version": "selected-video-v1",
                "request": request.model_dump(mode="json", exclude={"request_id"}),
                "processing": processing,
            }
        )
        request_sha = digest({"content_key": content_key})
        with workspace(config) as root:
            with self.catalog.evidence_transaction(selection.synthetic) as connection:
                self._permission(request, config, self._scope(connection, selection))
                # One authoritative lock spans aliases and daily admission, not external I/O.
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended("
                    "current_schema()||':selected-video-admission',729115))"
                )
                alias = connection.execute(
                    "SELECT * FROM video_research_requests WHERE synthetic=%s AND request_id=%s",
                    (selection.synthetic, request.request_id),
                ).fetchone()
                if alias and alias["request_sha256"] != request_sha:
                    raise VideoResearchDenied("video_request_identity_conflict")
                row = connection.execute(
                    "SELECT * FROM video_research_jobs WHERE synthetic=%s AND content_key=%s",
                    (selection.synthetic, content_key),
                ).fetchone()
                if row is not None:
                    if row["state"] != "completed":
                        raise VideoResearchDenied("video_outcome_uncertain_no_automatic_retry")
                    connection.execute(
                        "INSERT INTO video_research_requests VALUES(%s,%s,%s,%s) "
                        "ON CONFLICT DO NOTHING",
                        (selection.synthetic, request.request_id, request_sha, row["job_id"]),
                    )
                    cached = True
                else:
                    admit_workspace(config)
                    self.executor.preflight(request, config)
                    day = connection.execute(
                        "SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date AS day"
                    ).fetchone()["day"]
                    policy = digest(
                        {
                            "daily_limit": config.max_daily_operations,
                            "workspace": str(config.work_dir),
                        }
                    )
                    connection.execute(
                        "INSERT INTO video_research_budgets VALUES(%s,%s,%s,0) "
                        "ON CONFLICT DO NOTHING",
                        (selection.synthetic, day, policy),
                    )
                    budget = connection.execute(
                        "SELECT * FROM video_research_budgets "
                        "WHERE synthetic=%s AND day=%s FOR UPDATE",
                        (selection.synthetic, day),
                    ).fetchone()
                    if budget["configuration_sha256"] != policy:
                        raise VideoResearchDenied("video_daily_configuration_already_defined")
                    if budget["operations_reserved"] >= config.max_daily_operations:
                        raise VideoResearchDenied("video_daily_budget_exhausted")
                    connection.execute(
                        "UPDATE video_research_budgets "
                        "SET operations_reserved=operations_reserved+1 "
                        "WHERE synthetic=%s AND day=%s",
                        (selection.synthetic, day),
                    )
                    job_id = uuid.uuid4().hex + uuid.uuid4().hex
                    row = connection.execute(
                        "INSERT INTO video_research_jobs(job_id,synthetic,content_key,selection,"
                        "operation,provider,configuration,state) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,'running') "
                        "RETURNING *",
                        (
                            job_id,
                            selection.synthetic,
                            content_key,
                            Jsonb(selection.model_dump(mode="json")),
                            request.operation,
                            request.provider,
                            Jsonb(config.model_dump(mode="json")),
                        ),
                    ).fetchone()
                    connection.execute(
                        "INSERT INTO video_research_requests VALUES(%s,%s,%s,%s)",
                        (selection.synthetic, request.request_id, request_sha, job_id),
                    )
                    cached = False
            if cached:
                return self.result(
                    request.request_id,
                    selection.product_id,
                    config,
                    synthetic=selection.synthetic,
                    reused=True,
                )
            directory = root / row["job_id"]
            cleanup, owned_directory = False, False
            try:
                directory.mkdir(mode=0o700)
                owned_directory = True
                result = self.executor.execute(payload, directory, config)
                records = self._validate_output(request, config, directory, result, payload)
                with self.catalog.evidence_transaction(selection.synthetic) as connection:
                    self._permission(request, config, self._scope(connection, selection))
                records["metadata"]["input_provenance"] = self.provenance.references(
                    [selection.observation_id],
                    {selection.observation_id: selection.dataset_id},
                    selection.synthetic,
                )
                descriptor = self.provenance.records(records, selection.synthetic)
                with self.catalog.evidence_transaction(selection.synthetic) as connection:
                    self._permission(request, config, self._scope(connection, selection))
                    self.provenance.guard(connection, self.provenance.roots(descriptor))
                    updated = connection.execute(
                        "UPDATE video_research_jobs SET state='completed',descriptor=%s,"
                        "finished_at=clock_timestamp() WHERE job_id=%s AND state='running' "
                        "RETURNING job_id",
                        (Jsonb(descriptor), row["job_id"]),
                    ).fetchone()
                    if updated is None:
                        raise VideoResearchDenied("video_completion_identity_conflict")
            except BaseException:
                try:
                    with self.catalog.transaction() as connection:
                        failed = connection.execute(
                            "UPDATE video_research_jobs SET state='uncertain',"
                            "finished_at=clock_timestamp() "
                            "WHERE job_id=%s AND state='running' RETURNING job_id",
                            (row["job_id"],),
                        ).fetchone()
                    # Only acknowledged durable failure authorizes deleting owned files.
                    # A lost completion/failure ACK must preserve potentially committed media.
                    cleanup = failed is not None
                except Exception:
                    pass  # Preserve private bytes and the original failure if SQL is unavailable.
                raise
            finally:
                if (
                    cleanup
                    and owned_directory
                    and directory.is_dir()
                    and not directory.is_symlink()
                ):
                    try:
                        shutil.rmtree(directory)
                    except OSError:
                        pass  # Capacity checks bound leftovers; retain the original failure.
            return self.result(
                request.request_id, selection.product_id, config, synthetic=selection.synthetic
            )

    @staticmethod
    def _validate_output(request, config, directory, result, payload) -> dict:
        metadata = {
            "operation": request.operation,
            "provider": request.provider
            if request.operation == "comments"
            else "yt-dlp"
            if request.operation == "download"
            else "faster-whisper",
            "source_kind": "selected_video_enrichment",
            "purchase_evidence": False,
        }
        if request.operation == "comments":
            sample = CommentSample.model_validate(result)
            if len(sample.comments) > config.max_comments:
                raise VideoResearchDenied("comment_count_limit")
            records = {}
            for item in sample.comments:
                if item.video_id != request.selection.video_id():
                    raise VideoResearchDenied("comment_video_mismatch")
                if "comment:" + item.comment_id in records:
                    raise VideoResearchDenied("duplicate_comment_result")
                records["comment:" + item.comment_id] = item.model_dump(mode="json")
            metadata.update(
                sample.model_dump(mode="json", exclude={"comments"}),
                captured_comments=len(sample.comments),
                comment_ids=sorted(item.comment_id for item in sample.comments),
                insights=describe_comments(sample.comments),
            )
            return {**records, "metadata": metadata}
        if request.operation == "download":
            if result.get("video_id") != request.selection.video_id():
                raise VideoResearchDenied("media_video_mismatch")
            duration = result.get("duration_seconds")
            if (
                type(duration) not in {int, float}
                or not 0 < duration <= config.max_duration_seconds
            ):
                raise VideoResearchDenied("invalid_media_duration")
            checksum, size = file_hash(directory / "video.mp4", config.max_media_bytes)
            metadata.update(sha256=checksum, bytes=size, duration_seconds=duration)
        else:
            speech = SpeechResult.model_validate(result)
            if (
                speech.model_sha256 != config.model_sha256
                or speech.media_sha256 != payload["media_sha256"]
            ):
                raise VideoResearchDenied("speech_input_identity_mismatch")
            metadata.update(speech.model_dump(mode="json"))
        return {"metadata": metadata}

    def result(
        self,
        request_id: str,
        product_id: str,
        config: ExecutionConfig,
        *,
        synthetic: bool = False,
        reused: bool = False,
        limit: int = 20,
        after: str = "",
    ) -> dict:
        config = ExecutionConfig.model_validate(config)
        if (
            type(synthetic) is not bool
            or type(limit) is not int
            or not 1 <= limit <= 200
            or not isinstance(after, str)
            or len(after) > 32
            or not isinstance(request_id, str)
            or not 0 < len(request_id) <= 128
            or not isinstance(product_id, str)
            or not 0 < len(product_id) <= 128
        ):
            raise ValueError("Invalid comment result page")
        require_module(self.catalog, "video_research")
        with self.catalog.evidence_transaction(synthetic) as connection:
            row = connection.execute(
                "SELECT j.* FROM video_research_requests r "
                "JOIN video_research_jobs j USING(job_id) "
                "WHERE r.synthetic=%s AND r.request_id=%s",
                (synthetic, request_id),
            ).fetchone()
            if row is None or row["selection"]["product_id"] != product_id:
                raise VideoResearchDenied("video_result_not_found")
            selection = VideoSelection.model_validate(row["selection"])
            request = VideoRequest(
                request_id=request_id,
                selection=selection,
                operation=row["operation"],
                provider=row["provider"],
                # Only authority is checked here, never another network request.
                video_url=("https://www.tiktok.com/@selected/video/" + selection.video_id()),
                media_job_id=row["job_id"] if row["operation"] == "transcribe" else None,
            )
            self._permission(request, config, self._scope(connection, selection), execute=False)
            saved = ExecutionConfig.model_validate(row["configuration"])
            if saved.work_dir != config.work_dir:
                raise VideoResearchDenied("video_workspace_configuration_mismatch")
            if row["provider"] == "research" and row["operation"] == "comments":
                if (
                    saved.research is None
                    or config.research is None
                    or saved.research.project_id != config.research.project_id
                    or saved.research.reference != config.research.reference
                ):
                    raise VideoResearchDenied("research_project_authority_mismatch")
            if row["state"] != "completed":
                raise VideoResearchDenied("video_outcome_uncertain_no_automatic_retry")
            self.provenance.guard(connection, self.provenance.roots(row["descriptor"]))
            metadata = self.provenance.selected_records(
                row["descriptor"], ["metadata"], connection=connection
            )["metadata"]
            output = {
                "job_id": row["job_id"],
                "state": row["state"],
                "reused": reused,
                "selection": row["selection"],
                "available_at": row["finished_at"],
                "result": metadata,
            }
            if row["operation"] == "comments":
                candidates = [item for item in metadata["comment_ids"] if item > after]
                keys = ["comment:" + item for item in candidates[:limit]]
                values = self.provenance.selected_records(
                    row["descriptor"], keys, connection=connection
                )
                output["comments"] = [values[key] for key in keys]
                output["next_after"] = candidates[limit - 1] if len(candidates) > limit else None
            self._scope(connection, selection)
            return output

    def _media_job(self, job_id, selection, config) -> dict:
        require_module(self.catalog, "video_research")
        row = self.catalog.one(
            "SELECT r.request_id,j.selection FROM video_research_jobs j "
            "JOIN video_research_requests r USING(job_id) WHERE j.job_id=%s "
            "AND j.synthetic=%s AND j.operation='download' LIMIT 1",
            (job_id, selection.synthetic),
        )
        if (
            not row
            or row["selection"]["product_id"] != selection.product_id
            or row["selection"]["entity_key"] != selection.entity_key
        ):
            raise VideoResearchDenied("owned_media_job_required")
        output = self.result(
            row["request_id"], selection.product_id, config, synthetic=selection.synthetic
        )
        path = config.work_dir / output["job_id"] / "video.mp4"
        if path.parent.is_symlink() or path.parent.resolve().parent != config.work_dir.resolve():
            raise VideoResearchDenied("media_path_escaped")
        actual, size = file_hash(path, config.max_media_bytes)
        if actual != output["result"]["sha256"] or size != output["result"]["bytes"]:
            raise VideoResearchDenied("media_checksum_mismatch")
        return output

    def export_media(
        self, job_id: str, selection: VideoSelection, config: ExecutionConfig, destination: Path
    ) -> dict:
        selection, config = (
            VideoSelection.model_validate(selection),
            ExecutionConfig.model_validate(config),
        )
        metadata = self._media_job(job_id, selection, config)
        source = config.work_dir / metadata["job_id"] / "video.mp4"
        temporary = destination.with_name("." + destination.name + "." + uuid.uuid4().hex)
        created = False
        try:
            with self.catalog.evidence_transaction(selection.synthetic) as connection:
                self._scope(connection, selection)
                fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                created = True
                original = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(fd, "wb") as target, os.fdopen(original, "rb") as origin:
                    shutil.copyfileobj(origin, target, length=1024 * 1024)
                    target.flush()
                    os.fsync(target.fileno())
                checksum, size = file_hash(temporary, config.max_media_bytes)
                if checksum != metadata["result"]["sha256"] or size != metadata["result"]["bytes"]:
                    raise VideoResearchDenied("media_changed_during_export")
                self._scope(connection, selection)
                os.link(temporary, destination)  # Atomic, and never overwrites an existing path.
            return {"job_id": job_id, "bytes": size, "sha256": checksum, "exported": True}
        finally:
            if created:
                temporary.unlink(missing_ok=True)

    def import_transcription(
        self, request_id: str, product_id: str, config: ExecutionConfig, *, synthetic: bool = False
    ) -> dict:
        from ..enrichment import MachineTranscriptOrigin, TranscriptInput, TranscriptInsights

        output = self.result(request_id, product_id, config, synthetic=synthetic)
        if output["result"]["operation"] != "transcribe":
            raise VideoResearchDenied("machine_transcript_result_required")
        speech = SpeechResult.model_validate(
            {key: output["result"][key] for key in SpeechResult.model_fields}
        )
        job = self.catalog.one(
            "SELECT configuration FROM video_research_jobs WHERE job_id=%s", (output["job_id"],)
        )
        origin = MachineTranscriptOrigin(
            model=speech.model,
            engine_version=speech.engine_version,
            compute_type=job["configuration"]["compute_type"],
            model_sha256=speech.model_sha256,
            media_sha256=speech.media_sha256,
        )
        value = TranscriptInput(
            **output["selection"],
            source_kind="machine_transcript",
            text=speech.text,
            language=speech.language,
            machine_origin=origin,
        )
        imported = TranscriptInsights(self.catalog, self.provenance).import_transcript(value)
        return {
            **imported,
            "source_kind": "machine_transcript",
            "model": speech.model,
            "speech_verified": False,
        }
