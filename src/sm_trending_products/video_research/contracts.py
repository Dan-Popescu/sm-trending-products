"""Explicit selected-video authority, execution limits and backend-neutral results."""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from pydantic import ConfigDict, Field, field_validator, model_validator

from ..contracts import Contract, utc

ID = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
ENV = r"^[A-Z][A-Z0-9_]{0,127}$"
HEX = r"^[a-f0-9]{64}$"


class VideoResearchDenied(ValueError):
    """Safe public reason; provider diagnostics and credentials never leave workers."""


class Checked(Contract):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")


class VideoSelection(Checked):
    product_id: str = Field(pattern=ID)
    entity_key: str = Field(pattern=r"^[A-Za-z0-9_.-]+:video:\S{1,384}$", max_length=512)
    dataset_id: str = Field(min_length=1, max_length=128)
    observation_id: str = Field(min_length=1, max_length=256)
    synthetic: bool = Field(default=False, strict=True)
    permission_reference: str = Field(min_length=1, max_length=512)

    @field_validator("permission_reference")
    @classmethod
    def explicit_permission(cls, value: str) -> str:
        if not value.strip() or any(ord(c) < 32 for c in value):
            raise ValueError("An explicit printable permission reference is required")
        return value.strip()

    def video_id(self) -> str:
        source, _, value = self.entity_key.split(":", 2)
        if not self.synthetic and source not in {
            "tiktok",
            "tiktok_display",
            "tiktok_research",
            "tiktok_ads",
            "tiktok_commercial",
        }:
            raise VideoResearchDenied("tiktok_source_required")
        if not re.fullmatch(r"[1-9][0-9]{0,18}", value) or int(value) >= 2**63:
            raise VideoResearchDenied("numeric_tiktok_video_id_required")
        return value


class ResearchApproval(Checked):
    reference: str = Field(min_length=1, max_length=256)
    project_id: str = Field(pattern=ID)
    expires_at: datetime
    scope: Literal["research.data.basic"] = "research.data.basic"
    token_env: str = Field(default="TIKTOK_RESEARCH_ACCESS_TOKEN", pattern=ENV)

    @field_validator("reference")
    @classmethod
    def explicit_reference(cls, value: str) -> str:
        if not value.strip() or any(ord(char) < 32 for char in value):
            raise ValueError("Record an explicit approval reference")
        return value.strip()

    @field_validator("expires_at")
    @classmethod
    def aware(cls, value: datetime) -> datetime:
        return utc(value)


class ExecutionConfig(Checked):
    """Private operator configuration. No token, proxy or model is provisioned here."""

    enabled: bool = Field(default=False, strict=True)
    allow_download: bool = Field(default=False, strict=True)
    allow_unofficial: bool = Field(default=False, strict=True)
    allow_transcription: bool = Field(default=False, strict=True)
    research: ResearchApproval | None = None
    work_dir: Path
    max_daily_operations: int = Field(default=20, ge=1, le=10000, strict=True)
    max_comments: int = Field(default=100, ge=1, le=500, strict=True)
    max_pages: int = Field(default=3, ge=1, le=10, strict=True)
    max_media_bytes: int = Field(default=100_000_000, ge=1024, le=500_000_000, strict=True)
    max_workspace_bytes: int = Field(
        default=2_000_000_000, ge=1024, le=100_000_000_000, strict=True
    )
    timeout_seconds: int = Field(default=180, ge=1, le=3600, strict=True)
    transcription_timeout_seconds: int = Field(default=1800, ge=1, le=7200, strict=True)
    cpu_threads: int = Field(default=2, ge=1, le=32, strict=True)
    memory_mb: int = Field(default=8192, ge=512, le=65536, strict=True)
    max_duration_seconds: int = Field(default=300, ge=1, le=1800, strict=True)
    model: Literal["large-v3"] = "large-v3"
    model_dir: Path | None = None
    model_sha256: str | None = Field(default=None, pattern=HEX)
    compute_type: Literal["int8", "float32"] = "int8"
    unofficial_token_env: str = Field(default="TIKTOK_MS_TOKEN", pattern=ENV)

    @field_validator("work_dir", "model_dir")
    @classmethod
    def absolute(cls, value: Path | None) -> Path | None:
        if value is not None and not value.is_absolute():
            raise ValueError("Use a private absolute directory")
        return value

    @model_validator(mode="after")
    def limits_fit(self):
        if self.max_media_bytes > self.max_workspace_bytes:
            raise ValueError("A media file must fit the workspace budget")
        return self


class VideoRequest(Checked):
    request_id: str = Field(pattern=ID)
    selection: VideoSelection
    operation: Literal["comments", "download", "transcribe"]
    provider: Literal["research", "unofficial"] = "unofficial"
    video_url: str | None = Field(default=None, max_length=2048)
    media_job_id: str | None = Field(default=None, pattern=HEX)
    refresh_key: str = Field(default="initial", min_length=1, max_length=128)
    language: str | None = Field(default=None, pattern=r"^[a-z]{2,3}$")

    @model_validator(mode="after")
    def action_inputs(self):
        self.selection.video_id()
        if self.operation == "download" or (
            self.operation == "comments" and self.provider == "unofficial"
        ):
            if self.video_url is None:
                raise ValueError("A selected TikTok video URL is required")
            parts = urlsplit(self.video_url)
            if (
                parts.scheme != "https"
                or parts.hostname not in {"www.tiktok.com", "tiktok.com"}
                or parts.port not in {None, 443}
                or parts.username is not None
                or parts.password is not None
                or parts.query
                or parts.fragment
                or not re.fullmatch(
                    r"/@[A-Za-z0-9_.-]{1,128}/video/" + self.selection.video_id(), parts.path
                )
            ):
                raise ValueError("Use the matching canonical TikTok video URL")
        if self.operation == "transcribe" and self.media_job_id is None:
            raise ValueError("Select a completed media job")
        if self.operation != "transcribe" and self.media_job_id is not None:
            raise ValueError("Media input is only accepted for transcription")
        return self


class Comment(Checked):
    comment_id: str = Field(pattern=r"^[0-9]{1,32}$")
    video_id: str = Field(pattern=r"^[0-9]{1,32}$")
    text: str = Field(min_length=1, max_length=4000)
    created_at: datetime | None = None
    likes: int | None = Field(default=None, ge=0, strict=True)
    replies: int | None = Field(default=None, ge=0, strict=True)
    parent_id: str | None = Field(default=None, pattern=r"^[0-9]{1,32}$")

    @field_validator("created_at")
    @classmethod
    def aware(cls, value: datetime | None) -> datetime | None:
        return utc(value) if value is not None else None


class CommentSample(Checked):
    comments: tuple[Comment, ...] = Field(max_length=500)
    pages_read: int | None = Field(default=None, ge=0, le=10, strict=True)
    has_more: bool | None = Field(default=None, strict=True)
    coverage: Literal["bounded_sample", "returned_pages_exhausted"] = "bounded_sample"
    total_comments_on_platform: None = None


class SpeechSegment(Checked):
    start: float = Field(ge=0)
    end: float = Field(ge=0)
    text: str = Field(min_length=1, max_length=16000)

    @model_validator(mode="after")
    def interval(self):
        if self.end < self.start:
            raise ValueError("Invalid speech interval")
        return self


class SpeechResult(Checked):
    model: Literal["large-v3"] = "large-v3"
    model_sha256: str = Field(pattern=HEX)
    media_sha256: str = Field(pattern=HEX)
    language: str = Field(pattern=r"^[a-z]{2,3}$")
    text: str = Field(min_length=1, max_length=16000)
    segments: tuple[SpeechSegment, ...] = Field(max_length=1000)
    source_kind: Literal["machine_transcript"] = "machine_transcript"
    speech_verified: Literal[False] = False
    engine_version: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9.+_-]+$")
