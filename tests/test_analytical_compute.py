"""Complete results remain identical to independently captured pre-optimization code."""

import hashlib
import json
import random
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

from sm_trending_products.analytics import assess
from sm_trending_products.contracts import Measurement, Observation, Product, ProductLink, canonical


def test_complete_results_match_immutable_761b779_counter_oracle():
    expected = json.loads(
        (Path(__file__).parent / "fixtures/analytical-compute-oracle.json").read_text()
    )
    r = random.Random(761779)
    at = datetime(2026, 10, 24, tzinfo=UTC)
    metrics = ("view_count", "views", "units_sold", "sold_count")
    for case in range(len(expected["sha256"])):
        observations = []
        for i in range(r.randrange(1, 40)):
            entity = r.randrange(3)
            point = r.randrange(6)
            observed = at - timedelta(hours=point, microseconds=r.choice((0, 1, 999, 1000, 999999)))
            observed = observed.astimezone(timezone(timedelta(hours=r.choice((-4, 0, 3)))))
            lower = r.choice(
                (r.randrange(10000), 2**53 + r.randrange(10), 2**100 + r.randrange(10000))
            )
            precision = r.choice(("exact", "bounded", "estimated"))
            upper = lower if precision == "exact" else lower + r.randrange(1000)
            metric = r.choice(metrics)
            obs = Observation(
                observation_id=f"{case}-{i}",
                source="test",
                entity_type="video" if metric in ("view_count", "views") else "offer",
                entity_id=str(entity),
                observed_at=observed,
                available_at=observed + timedelta(microseconds=r.randrange(2000)),
                synthetic=True,
                metrics={
                    metric: Measurement(
                        lower=lower,
                        upper=upper,
                        precision=precision,
                        semantics=r.choice(("cumulative", "interval", "unknown")),
                        unit=r.choice(("count", "units")),
                        model_version="est" if precision == "estimated" else None,
                    )
                },
                attributes={
                    "counter_scope": r.choice(("lifetime", "period", "unspecified")),
                    "units_sold_period": r.choice(
                        ("lifetime", "rolling_7d", "month", "unknown", None)
                    ),
                },
            )
            observations.append(obs)
        p = Product(product_id="p", name="P", created_at=at - timedelta(days=100), synthetic=True)
        links = [
            ProductLink(product_id="p", entity_key=k, available_at=p.created_at)
            for k in sorted({x.entity_key for x in observations})
        ]
        result = assess(observations, [p], links, at + timedelta(hours=1), True)
        assert hashlib.sha256(canonical(result).encode()).hexdigest() == expected["sha256"][case], (
            case
        )
