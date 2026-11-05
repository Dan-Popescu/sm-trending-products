"""Optional free adapters; fixed official origin and explicitly sampled public comments."""

from __future__ import annotations

import asyncio
import json
import os
import re
import urllib.request
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

from .contracts import (
    Comment,
    CommentSample,
    ExecutionConfig,
    SpeechResult,
    VideoRequest,
    VideoResearchDenied,
)

RESEARCH_URL = "https://open.tiktokapis.com/v2/research/video/comment/list/"
COMMENT_FIELDS = "id,video_id,text,like_count,reply_count,parent_comment_id,create_time"


def _number(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str) and re.fullmatch(r"[0-9]{1,19}", value):
        value = int(value)
    if type(value) is not int or not 0 <= value < 2**63:
        raise VideoResearchDenied("invalid_comment_counter")
    return value


def normalize_comment(row: dict, video_id: str, *, unofficial: bool = False) -> Comment:
    identifier = str(row.get("cid") if unofficial else row.get("id"))
    actual_video = str(row.get("aweme_id", row.get("video_id", video_id)))
    if actual_video != video_id:
        raise VideoResearchDenied("comment_video_mismatch")
    stamp = _number(row.get("create_time"))
    text = row.get("text")
    if not isinstance(text, str):
        raise VideoResearchDenied("invalid_comment_text")
    # Keep text useful for product research without persisting contact addresses
    # or author profiles. This is a limited pattern filter, not full PII detection.
    text = re.sub(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", "[email]", text)
    parent = row.get("reply_id") if unofficial else row.get("parent_comment_id")
    return Comment(
        comment_id=identifier,
        video_id=video_id,
        text=text,
        created_at=datetime.fromtimestamp(stamp, UTC) if stamp is not None else None,
        likes=_number(row.get("digg_count") if unofficial else row.get("like_count")),
        replies=_number(row.get("reply_comment_total") if unofficial else row.get("reply_count")),
        parent_id=None if parent in {None, 0, "0", ""} else str(parent),
    )


def _add(items: dict[str, Comment], value: Comment) -> None:
    previous = items.get(value.comment_id)
    if previous is not None and previous != value:
        raise VideoResearchDenied("comment_changed_within_sample")
    items[value.comment_id] = value


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise VideoResearchDenied("research_redirect_refused")


def research_comments(request: VideoRequest, config: ExecutionConfig, *, opener=None) -> dict:
    approval = config.research
    if approval is None or approval.expires_at <= datetime.now(UTC):
        raise VideoResearchDenied("research_approval_required")
    token = os.environ.get(approval.token_env)
    if not token or "\n" in token or "\r" in token:
        raise VideoResearchDenied("research_token_required")
    client = opener or urllib.request.build_opener(_NoRedirect())
    items: dict[str, Comment] = {}
    cursor, has_more, pages = 0, True, 0
    video_id = request.selection.video_id()
    while has_more and pages < config.max_pages and len(items) < config.max_comments:
        body = json.dumps(
            {
                "video_id": int(video_id),
                "max_count": min(100, config.max_comments - len(items)),
                "cursor": cursor,
            }
        ).encode()
        message = urllib.request.Request(
            RESEARCH_URL + "?fields=" + COMMENT_FIELDS,
            data=body,
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
            method="POST",
        )
        with client.open(message, timeout=min(20, config.timeout_seconds)) as response:
            encoded = response.read(2_000_001)
        if len(encoded) > 2_000_000:
            raise VideoResearchDenied("comment_response_too_large")
        envelope = json.loads(encoded)
        if not isinstance(envelope, dict) or envelope.get("error", {}).get("code") != "ok":
            raise VideoResearchDenied("research_request_refused")
        data = envelope.get("data")
        if not isinstance(data, dict) or not isinstance(data.get("comments"), list):
            raise VideoResearchDenied("invalid_comment_response")
        rows = data["comments"]
        if len(rows) > min(100, config.max_comments - len(items)):
            raise VideoResearchDenied("comment_response_exceeds_request")
        if type(data.get("has_more")) is not bool:
            raise VideoResearchDenied("invalid_comment_pagination")
        pages += 1
        for row in rows:
            if not isinstance(row, dict):
                raise VideoResearchDenied("invalid_comment_record")
            _add(items, normalize_comment(row, video_id))
        has_more = data["has_more"]
        next_cursor = _number(data.get("cursor"))
        if has_more and (next_cursor is None or next_cursor <= cursor):
            raise VideoResearchDenied("comment_cursor_did_not_advance")
        if next_cursor is not None:
            cursor = next_cursor
    return CommentSample(
        comments=tuple(items.values()),
        pages_read=pages,
        has_more=has_more,
        coverage="bounded_sample" if has_more else "returned_pages_exhausted",
    ).model_dump(mode="json")


def unofficial_comments(request: VideoRequest, config: ExecutionConfig) -> dict:
    # Deliberately lazy: normal collection and help never import browser libraries.
    from TikTokApi import TikTokApi

    async def collect():
        async with TikTokApi() as api:
            token = os.environ.get(config.unofficial_token_env)
            await api.create_sessions(
                num_sessions=1,
                ms_tokens=[token] if token else None,
                sleep_after=3,
                headless=True,
                browser="chromium",
                enable_session_recovery=False,
                timeout=min(30000, config.timeout_seconds * 1000),
            )
            items: dict[str, Comment] = {}
            video = api.video(url=request.video_url)
            # Count is an SDK record limit, not a claim about HTTP request count.
            async for comment in video.comments(count=config.max_comments):
                value = comment.as_dict
                if not isinstance(value, dict):
                    raise VideoResearchDenied("invalid_comment_record")
                _add(items, normalize_comment(value, request.selection.video_id(), unofficial=True))
                if len(items) >= config.max_comments:
                    break
            return CommentSample(comments=tuple(items.values())).model_dump(mode="json")

    return asyncio.run(collect())


def download_video(request: VideoRequest, config: ExecutionConfig, directory: Path) -> dict:
    import yt_dlp

    target = directory / "video.mp4"
    if target.exists() or target.is_symlink():
        raise VideoResearchDenied("media_target_already_exists")

    def bounded(progress):
        for key in ("downloaded_bytes", "total_bytes", "total_bytes_estimate"):
            if progress.get(key, 0) and progress[key] > config.max_media_bytes:
                raise VideoResearchDenied("media_byte_limit")

    with yt_dlp.YoutubeDL(
        {
            "outtmpl": str(target),
            "format": "best[ext=mp4][height<=720]",
            "noplaylist": True,
            "overwrites": False,
            "continuedl": False,
            "retries": 0,
            "fragment_retries": 0,
            "socket_timeout": min(20, config.timeout_seconds),
            "max_filesize": config.max_media_bytes,
            "quiet": True,
            "no_warnings": True,
            "cachedir": False,
            "progress_hooks": [bounded],
            "writethumbnail": False,
            "writesubtitles": False,
            "writeautomaticsub": False,
            "skip_download": False,
        }
    ) as downloader:
        info = downloader.extract_info(request.video_url, download=False)
        if (
            not isinstance(info, dict)
            or str(info.get("id")) != request.selection.video_id()
            or info.get("_type", "video") != "video"
            or not isinstance(info.get("duration"), int | float)
            or isinstance(info["duration"], bool)
            or not 0 < info["duration"] <= config.max_duration_seconds
        ):
            raise VideoResearchDenied("media_identity_or_duration_unverified")
        downloader.process_info(info)
    return {"video_id": request.selection.video_id(), "duration_seconds": info["duration"]}


def transcribe_media(payload: dict, config: ExecutionConfig) -> dict:
    from faster_whisper import WhisperModel

    from .execution import file_hash

    if config.model_dir is None or config.model_sha256 is None:
        raise VideoResearchDenied("provisioned_large_v3_model_required")
    media_path = Path(payload["media_path"])
    media_before, _ = file_hash(media_path, config.max_media_bytes)
    if media_before != payload["media_sha256"]:
        raise VideoResearchDenied("media_checksum_mismatch")
    before, _ = file_hash(config.model_dir / "model.bin", 8_000_000_000)
    if before != config.model_sha256:
        raise VideoResearchDenied("model_checksum_mismatch")

    model = WhisperModel(
        str(config.model_dir),
        device="cpu",
        compute_type=config.compute_type,
        cpu_threads=config.cpu_threads,
        num_workers=1,
        local_files_only=True,
    )
    segments, info = model.transcribe(
        payload["media_path"],
        language=payload["request"].get("language"),
        beam_size=5,
        vad_filter=False,
        word_timestamps=False,
        condition_on_previous_text=False,
    )
    if not 0 < info.duration <= config.max_duration_seconds:
        raise VideoResearchDenied("audio_duration_limit")
    rows: list[dict[str, Any]] = []
    text = ""
    for segment in segments:
        if len(rows) >= 1000 or len(text) + len(segment.text) + 1 > 16000:
            raise VideoResearchDenied("transcript_text_limit")
        if not 0 <= segment.start <= segment.end <= config.max_duration_seconds:
            raise VideoResearchDenied("invalid_speech_interval")
        text += (" " if text else "") + segment.text.strip()
        rows.append({"start": segment.start, "end": segment.end, "text": segment.text})
    media_after, _ = file_hash(media_path, config.max_media_bytes)
    if media_after != media_before:
        raise VideoResearchDenied("media_changed_during_transcription")
    after, _ = file_hash(config.model_dir / "model.bin", 8_000_000_000)
    if after != before:
        raise VideoResearchDenied("model_changed_during_transcription")
    return SpeechResult.model_validate(
        {
            "text": text,
            "segments": rows,
            "language": info.language,
            "model_sha256": config.model_sha256,
            "media_sha256": payload["media_sha256"],
            "engine_version": version("faster-whisper"),
        }
    ).model_dump(mode="json")
