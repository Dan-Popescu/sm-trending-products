"""Signal-aware acquisition and replay loops; task admission remains in Catalog."""

from __future__ import annotations

import signal
import threading
from typing import TYPE_CHECKING

from .catalog import InvalidationPending
from .contracts import canonical

if TYPE_CHECKING:
    from .pipeline import Pipeline


def run_worker(
    pipeline: Pipeline,
    *,
    mode: str,
    batch_size: int,
    synthetic: bool,
    loop: bool,
    poll_seconds: float,
) -> dict:
    if not 0 < poll_seconds <= 60 or not 1 <= batch_size <= 1000:
        raise ValueError("Use a batch of 1..1000 and a poll interval in (0,60] seconds")
    method = {
        "worker": "collect_batch",
        "capture": "capture_batch",
        "publish-captures": "publish_captures",
    }.get(mode)
    if method is None:
        raise ValueError("Unknown worker mode")
    operation = getattr(pipeline, method)
    stop = threading.Event()
    control = pipeline.worker_control
    # Normalization has its own fenced replay leases and never claims source work.
    # Registering it as an acquisition worker would misrepresent drain evidence.
    session = pipeline.start_worker(synthetic) if mode != "publish-captures" else None
    generation = session["generation"] if session else None
    previous = {
        number: signal.signal(number, lambda *_: stop.set())
        for number in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        while not stop.is_set():
            if generation is not None:
                state = control.heartbeat(pipeline.worker_id, generation)["state"]
                if state != "running":
                    control.acknowledge_drain(pipeline.worker_id, generation)
                    if not loop:
                        return {"status": "drained"}
                    stop.wait(min(poll_seconds, 1))
                    continue
            try:
                result = operation(batch_size, synthetic=synthetic, should_stop=stop.is_set)
            except InvalidationPending:
                progress = pipeline.catalog.resume_invalidations(
                    max_batches=1000, max_seconds=0.8, synthetic=synthetic
                )
                result = {"status": "invalidation_pending", "invalidations": progress}
            if generation is not None:
                control.acknowledge_drain(pipeline.worker_id, generation)
            if not loop:
                return result
            print(canonical(result), flush=True)
            if result.get("status") == "invalidation_pending":
                progress = result["invalidations"]
                stop.wait(0.01 if progress["processed"] else min(poll_seconds, 0.25))
            elif (
                result.get("processed", 0) < batch_size
                or result.get("errors")
                or result.get("waiting")
                or result.get("quota_waiting")
            ):
                stop.wait(poll_seconds)
        return {"status": "stopped"}
    finally:
        try:
            if generation is not None:
                control.finish(pipeline.worker_id, generation)
        finally:
            for number, handler in previous.items():
                signal.signal(number, handler)
