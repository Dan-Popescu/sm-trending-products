# Bounded pure reuse within one decision comparison

C1 / #185 gives each `DecisionChanges.compare` call one private classification
and script-fingerprint memo. Each brief independently reads and hydrates its
eligible text before consulting the memo. Exact text, exact language (including
variants and unknown language), full canonical taxonomy content and both method
identities form the key. Context, attribution, transcript/source identities and
eligibility are reconstructed from each pass; none is memoized. Separate briefs
and progression operations receive no shared memo.

The memo admits at most 256 entries and 4 MiB of accounted retained objects.
Accounting includes the full text, language, taxonomy and method strings, key and
value tuples, immutable serialized classification, script hash and a conservative
one-entry dictionary allocation per entry. Shared strings are charged again for
each entry. These bounds do not describe total comparison memory or exact RSS.
Pressure declines admission while continuing to calculate and return every fact.
The context manager clears all retained entries and accounting on success and
exceptions, including source/read failures. A hit decodes a fresh result so
mutable returned reports cannot modify the stored classification. Serialization
preserves ordinary JSON insertion order as well as canonical values and IDs.

Regression coverage compares complete aggregate and comparison values, canonical
bytes and IDs with reuse disabled; checks ordinary aggregate JSON ordering;
exercises exact entry/byte boundaries, mutable outputs, Unicode offsets, unknown
and unsupported languages, capped mentions, text changes, language variants,
same-version taxonomy changes and method identities; and checks current owner
withdrawal between native brief passes. The existing content-normalization suite
continues to verify full-tail script fingerprints after the mention cap.

Native tests use dedicated PostgreSQL and loopback Moto under
`/tmp/night-native-tests.lock`. Measurements and their synthetic scope will be
recorded separately after the implementation checkpoint; no performance claim
is inferred from classification counts alone.
