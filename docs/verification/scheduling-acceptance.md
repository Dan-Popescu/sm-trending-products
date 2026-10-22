# Controlled scheduling acceptance

T07's engineering acceptance follows T04 and T06. It demonstrates decisions over
accessible, sampled candidates; it does not grant a TikTok capability, global
discovery coverage or a production refresh guarantee.

| Criterion | Executed evidence |
| --- | --- |
| Origin, discovery time, context and scope | `tests/test_collection_review.py` collects actual controlled Display-style responses and inspects the recorded request context, source/account, candidate discovery timestamp and origin dataset through the public facade and Streamlit. Synthetic classification remains explicit and source capabilities remain empty. |
| Unavailable discovery stays visible | The same native scenario attempts live Ads discovery with unverified source policy: no task is scheduled and the typed limitation is returned. Existing scheduler and dashboard cases distinguish missing collection/discovery grants and show the scope warning. The live collection-plan view stays empty. |
| Protected exploration | The native delivery gives one low-information candidate a protected request alongside an assessed hot product. Actual Rust request order confirms the exploration execution band. Scheduler cases retain that allocation with larger hot queues, request-cost extremes and concurrent planners. |
| Evidence, cost and measurable intervals | The recorded hot-product score is the actual published assessment; the unlinked candidate retains unknown assessment and explicit uncertainty. Request-cost inputs are retained. The view displays one-hour and twenty-four-hour planned intervals, while published assessment features retain the actual measured collection interval. The accelerated fixture does not pretend it waited for either target interval. Native cost-priority cases check the recorded arithmetic and shared quota cases enforce actual requests. |
| Reproducible policy and reasons | Policy IDs/versions and per-entity inputs remain unchanged after requests complete. The selected detail reads the persisted decision rather than recomputing it from updated evidence. Existing activation tests preserve first availability and reactivation history; owner-resolution and cost cases verify the inputs that determine priority. |
| Bounded orchestration | One in-process Dagster run coordinates the preplanned two-request batch through actual Rust, PostgreSQL and HTTP object storage. The application planner chooses intentions; grouped-source tests verify forty Display IDs use two physical requests and commercial entities share a cycle. No per-video orchestration run is introduced. |

## Read boundary and limits

The Collection plan view reads SQL-projected task summaries and one selected
decision. Migration 016 adds a partial workspace/time index for those summaries.
Selected group members are paged in SQL before JSON transfer, with at most 200
members per call; a 251-member native fixture verifies stable slicing and workspace
isolation. The header excludes the full member/key arrays. Request context and
selected output pass through redaction. One SQL statement snapshots task metadata
and the selected member page.

Frozen `decision`/member `reason` values belong to the recorded plan. The separate
`candidate` object contains current catalog timestamps and origin references; it
is not a historical reconstruction. Shared task/candidate records remain the only
authority. Inspection adds no scheduling mutation, duplicate state, analytical
history scan or remote source request. Deep offset pages still traverse their
preceding index/member positions; no constant-time pagination claim is made.

The delivery PR records its exact-head native, benchmark, image and isolated
recovery results before autonomous merge. Real update resolution, sustained load
and target-provider capacity remain separate acceptance work.
