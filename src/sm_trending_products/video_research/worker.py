"""Private subprocess entry; optional libraries never run in acquisition workers."""

from __future__ import annotations

import json
import os
import resource
import signal
import sys
from pathlib import Path

from .contracts import ExecutionConfig, VideoRequest, VideoResearchDenied
from .execution import re_safe_reason


def main() -> int:
    directory = Path(sys.argv[1])
    try:
        data = sys.stdin.buffer.read(64001)
        if len(data) > 64000:
            raise VideoResearchDenied("worker_request_limit")
        payload = json.loads(data)
        config = ExecutionConfig.model_validate(payload["config"])
        request = VideoRequest.model_validate(payload["request"])
        # The child never changes acquisition resources or downloads a model.
        available = sorted(os.sched_getaffinity(0))
        os.sched_setaffinity(0, available[: config.cpu_threads])
        os.nice(10)
        seconds = (
            config.transcription_timeout_seconds
            if request.operation == "transcribe"
            else config.timeout_seconds
        )
        resource.setrlimit(resource.RLIMIT_CPU, (seconds + 1, seconds + 2))
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        file_limit = max(2_000_000, config.max_media_bytes)
        resource.setrlimit(resource.RLIMIT_FSIZE, (file_limit, file_limit))
        if request.operation == "transcribe":
            amount = config.memory_mb * 1024**2
            resource.setrlimit(resource.RLIMIT_AS, (amount, amount))

        def terminate(_signal, _frame):
            os.killpg(os.getpgrp(), signal.SIGKILL)

        signal.signal(signal.SIGTERM, terminate)
        from .providers import (
            download_video,
            research_comments,
            transcribe_media,
            unofficial_comments,
        )

        if request.operation == "comments":
            result = (
                research_comments(request, config)
                if request.provider == "research"
                else unofficial_comments(request, config)
            )
        elif request.operation == "download":
            result = download_video(request, config, directory)
        else:
            result = transcribe_media(payload, config)
        response = {"ok": True, "result": result}
    except Exception as error:
        reason = str(error) if isinstance(error, VideoResearchDenied) else "optional_backend_failed"
        response = {
            "ok": False,
            "reason": reason if re_safe_reason(reason) else "optional_backend_failed",
        }
    encoded = json.dumps(response, ensure_ascii=False, allow_nan=False).encode()
    if len(encoded) > 2_000_000:
        return 2
    descriptor = os.open(directory / "result.json", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        output.write(encoded)
        output.flush()
        os.fsync(output.fileno())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
