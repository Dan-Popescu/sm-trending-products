"""Selector labels are private, unique and independent of action identity."""

import pytest

from sm_trending_products.product_display import product_labels


def test_ordinary_labels_and_raw_ids_remain_unchanged_and_runtime_is_not_read(monkeypatch):
    values = ["first", "second"]
    monkeypatch.setenv("RESEARCH_API_KEY", "first")
    assert product_labels(values) == {"first": "first", "second": "second"}
    assert values == ["first", "second"]
    assert product_labels([]) == {}


def test_two_current_secret_ids_have_unique_safe_labels_and_unchanged_keys():
    values = ("private-selector-key-417", "private-selector-key-418", "ordinary")
    labels = product_labels(values, secret_values=values[:2])
    assert list(labels) == list(values)
    assert len(set(labels.values())) == len(values)
    assert labels["ordinary"] == "ordinary"
    assert all(secret not in label for secret in values[:2] for label in labels.values())


def test_collision_ordinals_are_sanitized_as_part_of_the_entire_label():
    labels = product_labels(["first", "second"], secret_values=("first", "second", "1"))
    assert len(set(labels.values())) == 2
    assert all(
        "1" not in label and "first" not in label and "second" not in label
        for label in labels.values()
    )
    with pytest.raises(ValueError, match="unavailable"):
        product_labels(["first", "second"], secret_values=("first", "second", "1", "2"))


def test_source_urls_are_sanitized_before_their_labels_are_used():
    value = "https://user:password@example.test/?q=private%2Bkey%2642"
    label = product_labels([value], secret_values=("private+key&42",))[value]
    assert "user:password" not in label and "private" not in label


@pytest.mark.parametrize("values", [["same", "same"], [1], "identifier", None, ["p"] * 211])
def test_invalid_or_oversized_input_is_refused_without_partial_mapping(values):
    with pytest.raises(ValueError):
        product_labels(values)


def test_total_source_label_bytes_are_bounded():
    with pytest.raises(ValueError):
        product_labels(["x" * 4_000_001])
