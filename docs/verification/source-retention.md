# Explicit source retention verification

Specification: [lossless historical storage](../specs/lossless-history.md).
Scope: issue #109; no source credentials or deployed policy changes.

`source_policies.retention_days` describes a verified source expiry constraint.
An explicitly verified NULL cap permits retained history without age expiry.
Missing or unverified policy never grants live access. Positive finite limits
remain valid. SQL 039 changes no existing policy values or retirement journals.

`verify-live` requires either `--retention-days DAYS` or
`--source-retention-unlimited`. The latter also requires
`--source-retention-evidence REFERENCE` and SQL 039. The source evidence is
separate from `--evidence`, which records account access/intended-use approval.
Increasing or removing a previously trusted source cap requires that separate
source-wide evidence; an account probe cannot supply it implicitly. Existing
finite API calls still work when establishing, confirming or tightening a cap.
A failed or empty probe records its capability result and leaves any existing
trusted policy, evidence and policy clock unchanged. A successful confirmation
of the same trusted cap also preserves the policy snapshot.

Apply SQL 038 and 039 only after all retained application roles support their
formats. Before SQL 039, the new binary rejects unlimited requests and continues
handling finite requests. Old binaries do not support NULL retention arithmetic;
do not expose them to a catalog that can contain unlimited policies.

Native PostgreSQL regression coverage:

- `test_source_retention.py`: independent evidence for relaxed caps; failed,
  missing-token and empty probes preserve trusted caps; the explicit/schema gates;
  1,000-day-old retained observations remain replayable while finite expiry and
  source invalidation still apply; unlimited replacement origins remain usable.
- `test_capture_retirement.py`: NULL expiry certificates are rejected by SQL;
  old unlimited spool receipts survive cleanup; scoped withdrawal still retires
  them; committed certificates remain effective after switching to unlimited.
- `test_source_retention_restore.py`: a native custom dump and restore preserve
  NULL/finite policies and immutable withdrawal certificates. Restored history
  replays, finite expiry executes and withdrawn receipts cannot publish again.

The access-probe tests inject controlled collector responses. They demonstrate
the authorization boundaries, not real source access or verification of any
actual source contract. Native restore checks query source policy rows directly
because the logical catalog fingerprint does not include that table.
