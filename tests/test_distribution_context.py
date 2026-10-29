"""One missingness vocabulary, without a distribution or business whitelist."""

import pytest

from sm_trending_products.distribution_context import known_distribution_context


@pytest.mark.parametrize("prefix", ["", "declared:", " Declared: "])
@pytest.mark.parametrize(
    "value",
    [
        "",
        " \t",
        "unknown",
        "mixed",
        "unspecified",
        "not_available",
        "unknown_distribution",
        "unavailable",
        "not available",
        " UNKNOWN distribution ",
        "NOT--AVAILABLE",
        "n/a",
        " N/A ",
        "none",
        " NULL ",
        "not_applicable",
        "not applicable",
        "not-applicable",
    ],
)
def test_missing_context_labels_never_establish_comparability(prefix, value):
    assert known_distribution_context(prefix + value) is False


@pytest.mark.parametrize("value", [None, False, 0, 1, [], {}])
def test_malformed_context_is_not_stringified_into_known_context(value):
    assert known_distribution_context(value) is False


@pytest.mark.parametrize("prefix", ["", "declared:"])
@pytest.mark.parametrize(
    "value",
    [
        "public_video",
        "authorized_account",
        "commercial_content",
        "advertisement",
        "custom-source-distribution",
        "Reviewed source context",
    ],
)
def test_nonmissing_source_specific_labels_require_no_whitelist(prefix, value):
    assert known_distribution_context(prefix + value) is True
