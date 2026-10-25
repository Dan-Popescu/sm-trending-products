"""Source-wide expiry authority is separate from individual account access."""

from datetime import timedelta

import psycopg
import pytest
from test_pipeline_integration import ACCOUNT, video_observation

from sm_trending_products.collector import CollectionError
from sm_trending_products.contracts import digest, now
from sm_trending_products.invalidation import SourceInvalidation

pytestmark = pytest.mark.integration


def policy(pipeline, source="tiktok_display"):
    return pipeline.catalog.one("SELECT * FROM source_policies WHERE source=%s", (source,))


def accepted_probe(pipeline, monkeypatch):
    def envelope(request):
        record = video_observation("accepted", now(), 10, synthetic=False)
        record["source"] = request["source"]
        record["observation_id"] = digest(record)
        return {"raw": {"data": {}}, "records": [record], "observed_at": now().isoformat()}

    monkeypatch.setattr(pipeline.collector, "run", envelope)


def verify(pipeline, **kwargs):
    return pipeline.verify_live(
        "tiktok_display", ACCOUNT, "list", {}, evidence="account-access-approval", **kwargs
    )


@pytest.mark.parametrize("unlimited", [False, True])
@pytest.mark.parametrize("failure", ["denied", "missing_token", "empty"])
def test_failed_access_preserves_trusted_source_cap(pipeline, monkeypatch, unlimited, failure):
    accepted_probe(pipeline, monkeypatch)
    assert verify(pipeline, retention_days=7)["ok"]
    trusted = policy(pipeline)

    def failed_probe(request):
        if failure != "empty":
            raise CollectionError(failure)
        return {"raw": {}, "records": [], "observed_at": now().isoformat()}

    monkeypatch.setattr(pipeline.collector, "run", failed_probe)
    requested = (
        {"source_retention_unlimited": True, "source_retention_evidence": "source-contract"}
        if unlimited
        else {"retention_days": 365}
    )
    result = verify(pipeline, **requested)
    assert not result["ok"]
    assert policy(pipeline) == trusted
    latest = pipeline.catalog.one(
        "SELECT state FROM source_capabilities WHERE source='tiktok_display' "
        "AND account_id=%s AND capability='collect' ORDER BY available_at DESC,id DESC LIMIT 1",
        (ACCOUNT,),
    )
    assert latest["state"] == result["state"]


def test_relaxing_source_cap_needs_separate_evidence_but_tightening_preserves_api(
    pipeline, monkeypatch
):
    accepted_probe(pipeline, monkeypatch)
    verify(pipeline, retention_days=7)
    trusted = policy(pipeline)
    with pytest.raises(ValueError, match="separate source-wide retention evidence"):
        verify(pipeline, retention_days=30)
    assert policy(pipeline) == trusted
    verify(pipeline, retention_days=30, source_retention_evidence="source-wide-contract-v2")
    revised = policy(pipeline)
    assert revised["retention_days"] == 30
    assert revised["details"]["source_retention_evidence"] == "source-wide-contract-v2"
    verify(pipeline, retention_days=30)
    assert policy(pipeline) == revised
    verify(pipeline, retention_days=3)
    assert policy(pipeline)["retention_days"] == 3
    with pytest.raises(ValueError, match="separate source-wide retention evidence"):
        verify(pipeline, source_retention_unlimited=True)
    verify(
        pipeline,
        source_retention_unlimited=True,
        source_retention_evidence="source-expiry-unrestricted-v3",
    )
    assert policy(pipeline)["verified"] and policy(pipeline)["retention_days"] is None


def test_unlimited_requires_explicit_choice_verified_policy_and_schema039(pipeline, monkeypatch):
    accepted_probe(pipeline, monkeypatch)
    with pytest.raises(ValueError, match="Choose positive retention days"):
        verify(pipeline)
    with pytest.raises(ValueError, match="Choose positive retention days"):
        verify(pipeline, retention_days=7, source_retention_unlimited=True)
    with pytest.raises(psycopg.errors.CheckViolation):
        pipeline.catalog.execute(
            "INSERT INTO source_policies(source,retention_days,verified) "
            "VALUES('unknown',NULL,false)"
        )
    pipeline.catalog.execute("DELETE FROM schema_migrations WHERE version=39")
    with pytest.raises(ValueError, match="SQL schema 039"):
        verify(
            pipeline, source_retention_unlimited=True, source_retention_evidence="source-contract"
        )
    assert policy(pipeline) is None
    assert verify(pipeline, retention_days=7)["ok"]


def test_unsuccessful_unlimited_probe_does_not_invent_verified_policy(pipeline, monkeypatch):
    monkeypatch.setattr(
        pipeline.collector,
        "run",
        lambda request: {"raw": {}, "records": [], "observed_at": now().isoformat()},
    )
    assert not verify(
        pipeline, source_retention_unlimited=True, source_retention_evidence="source-contract"
    )["ok"]
    assert policy(pipeline) is None


def old_history(pipeline):
    retained = pipeline.import_records(
        [video_observation("retained", now() - timedelta(days=1000), 10)], synthetic=True
    )[0]
    finite_record = video_observation("expired", now() - timedelta(days=1000), 20)
    finite_record["source"] = "finite_fixture"
    expired = pipeline.import_records([finite_record], synthetic=True)[0]
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) "
        "VALUES('fixture_import',NULL,true),('finite_fixture',1,true)"
    )
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '1000 days'"
    )
    return retained, expired


def test_unlimited_old_history_replays_while_finite_expiry_and_withdrawal_still_apply(pipeline):
    retained, expired = old_history(pipeline)
    assert set(pipeline.retention()["dataset_ids"]) == {expired}
    pipeline.retention(apply=True)
    assert (
        pipeline.catalog.one("SELECT state FROM datasets WHERE dataset_id=%s", (retained,))["state"]
        == "committed"
    )
    assert pipeline.replay(retained)["records"] == 1
    assert pipeline.verify()["ok"]
    SourceInvalidation(pipeline.catalog).begin(retained, "source_deleted")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10)
    with pytest.raises(ValueError):
        pipeline.replay(retained)


def test_origin_repair_can_use_old_unlimited_replacement(pipeline):
    first = pipeline.import_records([video_observation("shared", now(), 10)], synthetic=True)[0]
    second = pipeline.import_records([video_observation("shared", now(), 20)], synthetic=True)[0]
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) "
        "VALUES('fixture_import',1,true)"
    )
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '1000 days'"
    )
    pipeline.catalog.execute(
        "UPDATE entities SET origin=jsonb_build_object('dataset_id',%s::text)", (first,)
    )
    work = SourceInvalidation(pipeline.catalog)
    work.begin(first, "retention_expired")
    # An already-authorized retirement finishes after the source cap changes;
    # a surviving old original may now supply unlimited replacement provenance.
    pipeline.catalog.execute("UPDATE source_policies SET retention_days=NULL")
    assert work.resume(max_batches=100, max_seconds=10)["status"] == "complete"
    entity = pipeline.catalog.one(
        "SELECT origin FROM entities WHERE entity_key='fixture_import:video:shared'"
    )
    assert entity["origin"]["dataset_id"] == second
