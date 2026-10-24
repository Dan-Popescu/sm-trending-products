"""Image publication admits only the exact main revision verified by native CI."""

import pytest

from sm_trending_products.release_artifacts import verified_main_run

REVISION = "a" * 40
REPOSITORY = "Dan-Popescu/sm-trending-products"


def run(**changes):
    return {
        "id": 42,
        "head_sha": REVISION,
        "head_branch": "main",
        "event": "push",
        "path": ".github/workflows/ci.yml",
        "status": "completed",
        "conclusion": "success",
        "head_repository": {"full_name": REPOSITORY},
        **changes,
    }


def test_publication_selects_the_exact_successful_main_push_run():
    result = verified_main_run(
        {"workflow_runs": [run(id=1, head_sha="b" * 40), run()]}, REVISION, REPOSITORY
    )
    assert result == 42


@pytest.mark.parametrize(
    "changes",
    [
        {"head_sha": "b" * 40},
        {"head_branch": "unreviewed"},
        {"event": "pull_request"},
        {"path": ".github/workflows/unrelated.yml"},
        {"status": "in_progress", "conclusion": None},
        {"conclusion": "failure"},
        {"head_repository": {"full_name": "external/fork"}},
    ],
)
def test_untrusted_incomplete_or_unrelated_evidence_cannot_publish(changes):
    with pytest.raises(ValueError, match="verified main push"):
        verified_main_run({"workflow_runs": [run(**changes)]}, REVISION, REPOSITORY)


@pytest.mark.parametrize("revision", ["main", "latest", "a" * 39, "a" * 40 + "\nrun"])
def test_mutable_or_malformed_revisions_are_rejected(revision):
    with pytest.raises(ValueError, match="immutable"):
        verified_main_run({"workflow_runs": [run()]}, revision, REPOSITORY)
