"""Malformed evidence must fail at its public ingestion contract."""

import pytest

from sm_trending_products.contracts import AssessmentPolicy, Measurement


def test_boolean_is_not_a_measured_unit_count():
    with pytest.raises(ValueError):
        Measurement(lower=True, upper=True)


@pytest.mark.parametrize("weight", [float("nan"), float("inf")])
def test_ranking_weights_must_be_finite(weight):
    with pytest.raises(ValueError):
        AssessmentPolicy(weights={"attention": weight})
