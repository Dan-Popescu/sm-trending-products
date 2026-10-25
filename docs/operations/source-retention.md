# Explicit source retention

Useful history can be retained indefinitely when the source permits it. There is
no automatic conversion of existing policies and no voluntary age deletion added
by this delivery. Source withdrawals and verified finite expiry requirements
remain effective. A calculation's lookback window does not delete older history.

An absent or unverified source policy is unknown and cannot enable live
acquisition. A verified positive `retention_days` imposes that expiry. On SQL 039,
a verified NULL explicitly states that the source has no expiry cap. NULL is not
an unverified default or a very large number of days.

After the [compatible reader handover](private-release-controller.md#additive-capture-schema-expansions)
and SQL 039, `sm-trends verify-live` accepts `--source-retention-unlimited`
instead of `--retention-days DAYS`. Supply `--source-retention-evidence REF`
with a separate source-wide policy/permission reference, in addition to the
existing account-access `--evidence REF`. These references are not credentials;
never put tokens in them. Existing required source/account/payload and approval
arguments still apply; the operation keeps its source-specific default. Inspect
`sm-trends verify-live --help` for the complete command.

Source policies apply across accounts. A successful account access probe cannot
remove or increase a trusted finite cap without explicit source-wide retention
evidence. A failed probe records its capability result but preserves the trusted
source cap, timestamp and evidence. Tightening a cap remains possible. Do not
claim source-wide permission solely from one account's endpoint response.

Unlimited policies skip age expiry, including capture-spool expiry. Scoped source
withdrawals, invalidations and immutable retirement certificates remain binding.
Changing a policy cannot resurrect evidence already retired. Maintenance must
continue running; disabling it would also stop withdrawal and recovery work.

Native tests cover old retained history, unchanged finite expiry, replacement
origins, rejected false expiry certificates, explicit withdrawals and exact
policy/certificate preservation through `pg_dump`/`pg_restore`. See the
[verification record](../verification/source-retention.md). No real source policy
or source credentials were changed during this engineering delivery.
