# Share documented counter periods while preserving frozen outcome definitions

Status: adopted under the owner's architecture-refactor authorization on 2026-10-28.
Delivery: #136. Preserves the historical compatibility required by ADR0008.

Assessments and new persisted evaluations use one implementation of documented
counter periods, so rolling or contradictory sales declarations cannot become
exact interval sales. New persisted decisions record
`counter_period_contract: "documented-counter-v1"`; configuration parsing and
maturation never add that contract to old decisions. An absent contract keeps
the historical outcome interpretation, and its omission from serialization
preserves existing raw snapshots and hashes required by ADR0008.

The general evaluation version is deliberately not the selector: it has always
accepted arbitrary caller labels, which cannot safely be reinterpreted on old
snapshots. The measurement module owns both the documented rules and the narrow
legacy interpretation. Choosing the contract adds no history scans, storage
access or persistent projection format; the existing prepared entity timeline
continues serving every horizon and threshold query.
