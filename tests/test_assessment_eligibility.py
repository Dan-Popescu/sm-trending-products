"""Filtering and projection identity are one captured eligibility contract."""

from datetime import timedelta

from test_evaluation import observation

from sm_trending_products.assessment_eligibility import AssessmentEligibility
from sm_trending_products.contracts import Capability, utc


def test_market_and_capability_changes_select_only_their_compatible_evidence():
    cutoff = utc("2026-01-03T00:00:00Z")
    grant = Capability(
        source="fixture",
        account_id="account",
        capability="collect",
        state="available",
        available_at=cutoff - timedelta(hours=1),
    )
    rows = [
        observation("fr", "2026-01-02T00:00:00Z", 100, market="FR", account_id="account"),
        observation("us", "2026-01-02T00:00:00Z", 100, market="US", account_id="account"),
    ]
    french = AssessmentEligibility("FR", [grant])
    american = AssessmentEligibility("US", [grant])
    denied = AssessmentEligibility("FR", [grant.model_copy(update={"state": "denied"})])

    assert [row.entity_id for row in french.select(rows, cutoff=cutoff, synthetic=True)] == ["fr"]
    assert [row.entity_id for row in american.select(rows, cutoff=cutoff, synthetic=True)] == ["us"]
    assert denied.select(rows, cutoff=cutoff, synthetic=True) == []
    assert (
        len(
            {
                french.cache_identity(cutoff),
                american.cache_identity(cutoff),
                denied.cache_identity(cutoff),
            }
        )
        == 3
    )


def test_a_future_source_grant_changes_selection_and_identity_at_its_availability():
    cutoff = utc("2026-01-03T00:00:00Z")
    grant_time = cutoff + timedelta(hours=1)
    grant = Capability(
        source="fixture",
        account_id="account",
        capability="collect",
        state="available",
        available_at=grant_time,
    )
    rows = [observation("v", "2026-01-02T00:00:00Z", 100, account_id="account")]
    eligibility = AssessmentEligibility(None, [grant])

    assert eligibility.select(rows, cutoff=cutoff, synthetic=True) == []
    assert eligibility.select(rows, cutoff=grant_time, synthetic=True) == rows
    assert eligibility.cache_identity(cutoff) != eligibility.cache_identity(grant_time)


def test_eligibility_captures_capability_inputs_instead_of_retaining_mutable_callers():
    cutoff = utc("2026-01-03T00:00:00Z")
    grant = Capability(
        source="fixture",
        account_id="account",
        capability="collect",
        state="available",
        available_at=cutoff,
        details={"scope": ["original"]},
    )
    capabilities = [grant]
    eligibility = AssessmentEligibility(None, capabilities)
    identity = eligibility.cache_identity(cutoff)
    capabilities.clear()
    grant.details["scope"].append("changed by caller")
    rows = [observation("v", "2026-01-02T00:00:00Z", 100, account_id="account")]

    assert eligibility.cache_identity(cutoff) == identity
    assert eligibility.select(rows, cutoff=cutoff, synthetic=True) == rows
