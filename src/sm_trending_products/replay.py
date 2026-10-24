"""Offline, immutable source transformations and their stable knowledge time."""

from __future__ import annotations

from .catalog import Catalog
from .collector import CollectionError, Collector
from .contracts import Observation, digest, now, utc
from .history import History


class ReplayService:
    def __init__(self, catalog: Catalog, history: History, collector: Collector):
        self.catalog, self.history, self.collector = catalog, history, collector

    @staticmethod
    def _result(dataset: dict, requested: str) -> dict:
        manifest = dataset["manifest"]
        rejected = manifest.get("quarantine_count", 0)
        return {
            "dataset_id": dataset["dataset_id"],
            "replayed_from": requested,
            "records": manifest["row_count"],
            "quarantine_count": rejected,
            "status": "quarantined" if rejected else "complete",
            "transform_version": manifest["transform_version"],
        }

    def replay(
        self, dataset_id: str, transform_version: str = "1", *, publication_guard=None
    ) -> dict:
        if not isinstance(transform_version, str) or transform_version not in {"1", "2"}:
            raise ValueError("Supported source transformation versions are 1 and 2")
        dataset = self.catalog.one(
            "SELECT * FROM datasets WHERE dataset_id=%s AND state='committed'", (dataset_id,)
        )
        if not dataset:
            raise ValueError("No completed dataset found")
        if dataset["manifest"].get("kind") == "raw_capture_error":
            raise ValueError("A source business error cannot be normalized as source evidence")
        if (
            dataset["manifest"].get("kind") == "raw_capture"
            and dataset["manifest"].get("acquisition_protocol") != "1"
        ):
            raise ValueError("Unsupported raw acquisition protocol")
        raw = self.history.raw(dataset)
        quarantine = raw.get("quarantine", [])
        source_replay = raw.get("kind") not in {"import", "compaction"}
        parent = dataset
        if not source_replay:
            if transform_version != dataset["transform_version"]:
                raise ValueError("Changing a transformation requires an archived source response")
            observations = [Observation.model_validate(item) for item in raw["records"]]
        else:
            if transform_version == "2":
                # Aliases and replay-of-replay use one original delivery/version.
                parent = self.catalog.replay_origin(dataset_id)
                replay_id = digest({"replay_of": parent["dataset_id"], "version": "2"})
                existing = self.catalog.one(
                    "SELECT * FROM datasets WHERE dataset_id=%s", (replay_id,)
                )
                if existing:
                    if (
                        existing["source"] != parent["source"]
                        or existing["synthetic"] != parent["synthetic"]
                        or existing["transform_version"] != "2"
                        or existing["supersedes"] != parent["dataset_id"]
                    ):
                        raise ValueError("Replay identity collision")
                    if existing["state"] == "committed":
                        return self._result(existing, dataset_id)
                    if existing["state"] in {"invalidated", "deleted"}:
                        raise ValueError("Cannot republish invalidated evidence")
                raw = self.history.raw(parent)
            request = {
                "request": raw["request"],
                "raw": raw["response"],
                "observed_at": raw["observed_at"],
            }
            if transform_version == "2":
                request.update(
                    transform_version="2",
                    source_dataset_id=parent["dataset_id"],
                    available_at=max(now(), utc(raw["observed_at"])).isoformat(),
                )
            try:
                result = self.collector.run(request, normalize=True)
            except CollectionError as error:
                if error.category != "schema" or error.artifact is None:
                    raise
                observations = []
                quarantine = [{"reason": error.category}]
            else:
                if result["normalizer_version"] != transform_version:
                    raise ValueError("Collector normalizer version differs from requested replay")
                observations = [Observation.model_validate(item) for item in result["records"]]
                quarantine = result.get("quarantine", [])
            if transform_version == "2" or dataset["manifest"].get("kind") == "raw_capture":
                replay_id = digest(
                    {"replay_of": parent["dataset_id"], "version": transform_version}
                )
                # Reserve only after the decoder actually returned evidence or a
                # schema quarantine. The short SQL transaction never spans Rust/S3.
                available_at = self.catalog.replay_available_at(
                    replay_id,
                    parent["dataset_id"],
                    max(
                        utc(raw["observed_at"]),
                        parent["max_available_at"] or utc(raw["observed_at"]),
                    ),
                )
                observations = [
                    row.model_copy(update={"available_at": available_at}) for row in observations
                ]
            if (
                parent["manifest"].get("kind") == "raw_capture"
                and raw["request"]["source"] == "tiktok_display"
                and raw["request"]["operation"] == "query"
            ):
                requested = set(raw["request"]["payload"].get("filters", {}).get("video_ids", []))
                raw = {
                    **raw,
                    "requested_entity_ids": sorted(requested),
                    "missing_entity_ids": sorted(
                        requested - {row.entity_id for row in observations}
                    ),
                }
        identity = {"replay_of": parent["dataset_id"], "version": transform_version}
        if quarantine and not (source_replay and transform_version == "2"):
            # Keep the established v1 replay keys, including its rejection fix.
            identity["quarantine"] = quarantine
        replay_id = digest(identity)
        self.history.publish(
            observations,
            raw,
            source=parent["source"],
            synthetic=parent["synthetic"],
            dataset_id=replay_id,
            transform_version=transform_version,
            supersedes=parent["dataset_id"],
            quarantine=quarantine,
            **({"publication_guard": publication_guard} if publication_guard else {}),
        )
        published = self.catalog.one("SELECT * FROM datasets WHERE dataset_id=%s", (replay_id,))
        if not published or published["state"] != "committed":
            raise RuntimeError("Replay did not publish a completed dataset")
        return self._result(published, dataset_id)
