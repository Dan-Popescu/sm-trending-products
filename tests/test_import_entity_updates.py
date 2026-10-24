"""Repeated historical observations retain every fact without repeated frontier writes."""

from datetime import timedelta

import pytest

from sm_trending_products.contracts import now

pytestmark = pytest.mark.integration


def record(entity, timestamp, title, account="fixture-account"):
    return {
        "source": "entity_import_fixture",
        "entity_type": "video",
        "entity_id": entity,
        "observed_at": timestamp.isoformat(),
        "available_at": timestamp.isoformat(),
        "synthetic": True,
        "account_id": account,
        "attributes": {"title": title},
    }


def test_historical_import_writes_at_most_two_entity_frontiers_and_keeps_all_facts(pipeline):
    with pipeline.catalog.transaction() as connection:
        connection.execute("CREATE TABLE entity_write_audit(entity_key text)")
        connection.execute("""
            CREATE FUNCTION count_entity_write() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                INSERT INTO entity_write_audit VALUES(NEW.entity_key);
                RETURN NULL;
            END $$
        """)
        connection.execute("""
            CREATE TRIGGER count_entity_write AFTER INSERT OR UPDATE ON entities
            FOR EACH ROW EXECUTE FUNCTION count_entity_write()
        """)
    cutoff = now()
    records = [
        record(f"video-{entity}", cutoff - timedelta(hours=119 - point), f"title-{point}")
        for point in range(120)
        for entity in range(20)
    ]
    dataset = pipeline.import_records(records, True)[0]
    writes = pipeline.catalog.one("SELECT count(*) AS n FROM entity_write_audit")["n"]
    assert 20 <= writes <= 40
    assert pipeline.catalog.one("SELECT count(*) AS n FROM observation_deliveries")["n"] == 2400
    observations, _ = pipeline.history.observations(cutoff, True)
    assert len(observations) == 2400
    assert len({row.observation_id for row in observations}) == 2400
    rows = pipeline.catalog.all("SELECT title,last_observed_at,origin FROM entities")
    assert len(rows) == 20
    assert all(row["title"] == "title-119" and row["last_observed_at"] == cutoff for row in rows)
    assert all(row["origin"]["dataset_id"] == dataset for row in rows)
    pipeline.import_records(records, True)
    assert pipeline.catalog.one("SELECT count(*) AS n FROM entity_write_audit")["n"] == writes


@pytest.mark.parametrize("first_is_latest", [False, True])
def test_existing_unknown_frontier_preserves_original_title_comparison(pipeline, first_is_latest):
    cutoff = now()
    initial = pipeline.import_records([record("existing", cutoff, "original")], True)[0]
    pipeline.catalog.execute("UPDATE entities SET last_observed_at=NULL")
    first_time = cutoff if first_is_latest else cutoff - timedelta(hours=2)
    second_time = cutoff - timedelta(hours=1)
    dataset = pipeline.import_records(
        [record("existing", first_time, "first"), record("existing", second_time, "second")],
        True,
    )[0]
    row = pipeline.catalog.one("SELECT title,attributes,last_observed_at,origin FROM entities")
    assert row["title"] == ("original" if first_is_latest else "second")
    assert row["attributes"]["title"] == ("first" if first_is_latest else "second")
    assert row["last_observed_at"] == max(first_time, second_time)
    assert row["origin"]["dataset_id"] == dataset != initial


def test_equal_timestamp_last_input_wins_and_older_input_cannot_replace_frontier(pipeline):
    cutoff = now()
    pipeline.import_records([record("tied", cutoff, "original")], True)
    dataset = pipeline.import_records(
        [
            record("tied", cutoff, "first", "first-account"),
            record("tied", cutoff - timedelta(hours=2), "older"),
            record("tied", cutoff, "last", "last-account"),
        ],
        True,
    )[0]
    row = pipeline.catalog.one("SELECT title,account_id,last_observed_at,origin FROM entities")
    assert row == {
        "title": "last",
        "account_id": "last-account",
        "last_observed_at": cutoff,
        "origin": {"dataset_id": dataset, "source": "entity_import_fixture"},
    }
