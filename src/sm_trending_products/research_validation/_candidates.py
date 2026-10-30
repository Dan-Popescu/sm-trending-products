"""Editable investigation strategies, with no calibration or selection claim."""

from __future__ import annotations

from copy import deepcopy

CRITERIA = {
    "growth_across_days": {
        "available": "growth_per_hour, positive_interval_fraction, available acceleration",
        "meaning": "Existing attention heuristic over distinct-time observations in lookback_days",
        "limit": (
            "7/14/30 days retain history; they do not require growth on each "
            "day or a minimum persistence"
        ),
    },
    "diversity_and_coverage": {
        "available": (
            "known creator spread/concentration, family-deduplicated top-K, selection audit"
        ),
        "limit": (
            "Unknown creators and unsampled sources remain unknown; no "
            "configurable diversity threshold or market census"
        ),
    },
    "demand": {
        "available": "Existing intent proxies and compatible purchase counters when supplied",
        "limit": "Views are not sales; missing weighted demand excludes under strict missingness",
    },
    "competition": {
        "available": (
            "Named observed sellers/advertisers, offers, fingerprints and comparable prices"
        ),
        "limit": (
            "Coverage-dependent proxy, not measured market saturation; "
            "missing competitors stay unknown"
        ),
    },
    "margin": {
        "available": "Existing feasibility scenario from complete currency-consistent stated costs",
        "limit": (
            "Unknown without complete costs; scenario margin is not realized "
            "profit or calibrated sales"
        ),
    },
    "delivery": {
        "available": "Launch delay sensitivity at 12/24/48 hours in the outcome configuration",
        "limit": (
            "Launch delay is not supplier delivery time; fulfillment "
            "reliability/geography are not modeled by AssessmentPolicy"
        ),
    },
}


def example_profile() -> dict:
    return {
        "name": "physical-products-pilot",
        "revision": 1,
        "config": {
            "target": "attention",
            "threshold": 1000,
            "top_k": 3,
            "horizon_days": [3],
            "launch_delay_hours": [12, 24, 48],
            "max_boundary_gap_hours": 24,
        },
        "policies": {"attention": {"version": "attention-v1", "weights": {"attention": 1}}},
        "partitions": {
            name: {"product_ids": [name + "-product"], "decision_times": [date]}
            for name, date in (
                ("development", "2027-01-01T00:00:00Z"),
                ("test", "2027-01-06T00:00:00Z"),
                ("holdout", "2027-01-11T00:00:00Z"),
            )
        },
        "measurement_sample_size": 2,
        "seed": 42,
        "max_attempts": 32,
    }


def candidate_bundle() -> dict:
    from .models import ValidationProtocol

    profile = example_profile()
    profile["name"] = "unvalidated-selection-investigation"
    profile["policies"] = {
        "attention-7d": {
            "version": "unvalidated-attention-7d-v1",
            "weights": {"attention": 1},
            "lookback_days": 7,
            "min_observations": 2,
            "missing_data": "exclude",
        },
        "attention-demand-14d-strict": {
            "version": "unvalidated-attention-demand-14d-strict-v1",
            "weights": {"attention": 0.7, "demand": 0.3},
            "lookback_days": 14,
            "min_observations": 3,
            "missing_data": "exclude",
        },
        "attention-demand-14d-available": {
            "version": "unvalidated-attention-demand-14d-available-v1",
            "weights": {"attention": 0.7, "demand": 0.3},
            "lookback_days": 14,
            "min_observations": 3,
            "missing_data": "renormalize",
        },
        "commercial-30d-strict": {
            "version": "unvalidated-commercial-30d-strict-v1",
            "weights": {"attention": 0.5, "demand": 0.2, "competition": 0.15, "feasibility": 0.15},
            "lookback_days": 30,
            "min_observations": 4,
            "missing_data": "exclude",
        },
    }
    # Validate the exact editable contract without registering or constructing services.
    ValidationProtocol.model_validate(profile)
    return {
        "status": "UNVALIDATED",
        "note": (
            "Illustrative editable investigation strategies. Replace "
            "IDs/dates; preregister all candidates. No winner or calibrated commercial claim."
        ),
        "criterion_dictionary": deepcopy(CRITERIA),
        "protocol": profile,
    }
