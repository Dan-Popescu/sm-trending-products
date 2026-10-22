# GitHub publication

Published on 2026-10-22 after GitHub connector write access was enabled.

- Repository: https://github.com/Dan-Popescu/sm-trending-products
- Implementation branch: `codex/project-planning`
- Review: https://github.com/Dan-Popescu/sm-trending-products/pull/1
- Global PRD issue: https://github.com/Dan-Popescu/sm-trending-products/issues/2
- Six specification issues and fifteen delivery tickets: [published tracker mapping](../planning/github-issues.json).

The repository was empty before publication. Its `main` branch contains only the
initial README; implementation and specifications are on the PR branch until
review and merge. No production deployment or live source was enabled.

The connector published the six original implementation steps through Git objects.
Every remote tree was verified byte-for-byte identical by its Git tree SHA to the
corresponding local tree, including executable modes. GitHub assigns new commit
metadata, so published commit IDs differ. Each commit records its original local
ID in an `Original-Local-Commit` trailer. Original history remains preserved in
the delivered Git bundle and local `codex/local-implementation-archive` branch.

| Original local commit | Published commit |
| --- | --- |
| `c94ed814f692563b10230f4f3574d234beb45338` | `1a8e494c8800d7dbc9a590b18b5969ad3e9af119` |
| `3c33c89bf83099ec32b961e55c950d4f37ea2e00` | `e2039c456f7580b8eca225cc1e8167f7ef04aba0` |
| `3d8778ef0ecdf95b418bb90eebbf79f0507c3793` | `5dc0c3f74a142c7b9f6d283dfc295d72cae9565a` |
| `6084da66d5d3a8d472bd24cd571644c37719cd67` | `75c8c555ae22874a485410c84346f5eb288afbd1` |
| `2f516e3b14384cd6e7924f074326cd4b6d1991f0` | `0aa2c18889825439447b5b8a62ea85719da21ea4` |
| `7df67b769782c24a55e59fac3142f5977644f808` | `2f7cec064f1b3b90cac9133c0d54888c4307f93d` |

Benchmark and local-test reports refer to the original measured commit IDs.
Use this mapping when locating the identical published code. Subsequent CI fixes
and acceptance reports are separate follow-up commits.

Issue checklists remain open until their corresponding acceptance is established.
Live permissions, actual infrastructure rollout, and representative capacity are
independent from successful source publication. Pending CI results are not passes.

