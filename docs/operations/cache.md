# Shared verified-cache boundary

`CACHE_MAX_BYTES` bounds admitted object payload bytes across readers and worker
processes using the same cache root. Accounting counts each cached or pinned
inode once and includes the full reserved size of every in-flight download.
Filesystem metadata/allocation overhead and analytical spill directories are
separate operational disk headroom; this value is not a whole-volume quota.

Workers sharing a root must use the same configured budget. When reducing it,
stop new work in the old group and restart with the new value. The first cache
admission in each application instance reconciles retained payloads under the
existing accounting lock, including a fully warm cache. It keeps the requested
verified warm input when possible and evicts unused payloads. Existing readers
are never discarded: if their pins or existing download reservations exceed the
new limit, admission raises `CacheCapacityError` until they finish and can then
be retried. The successful root/limit check is remembered
only in the application instance, so subsequent warm reads avoid a full directory
scan. Cache payloads and reservations remain the accounting source of truth.

Readers use `pin_cached` for their complete input lifetime. Its hardlink keeps the
verified inode alive even if retention removes the ordinary cache name. A shared
kernel lock protects that inode. A later admission reclaims abandoned pin links
only after it can acquire an exclusive nonblocking lock; hard process termination
releases those locks without requiring a PID timeout or guessing reader liveness.

Downloads reserve the manifest's declared size before contacting storage and
cannot write beyond it. Missing size metadata conservatively reserves the entire
budget. A locked reservation record covers the temporary file until publication;
the next admission removes records and temporary files abandoned by a dead
process. Pin and reservation bookkeeping remains under the short `.cache.lock`.

Remote reads, fsync and SHA-256 verification do not hold that global lock. Warm
reads pin and verify directly. Cold fills use 256 stable lock stripes, avoiding an
unbounded number of permanent coordination files; distinct cold keys can share a
stripe, while warm hits never wait for a cold fill's stripe. Retirement acquires
the same stripe before deleting the remote object and cache name, preventing a
late fill from recreating a retired cache copy.

If live pins/downloads occupy the available capacity, admission raises
`CacheCapacityError` before another download. Retry the bounded analytical work
after its readers finish, or narrow selected inputs. Raising a per-reader limit
does not bypass the global admission boundary. Existing readers keep their pins;
the cache never deletes a live input to make room.

Deploy this pin-locking change with old readers/workers stopped: older software
does not hold the kernel locks that establish pin liveness. Reuse verified warm
object files after restarting the complete cache-sharing group. Do not run old
and new cache implementations against the same directory during the upgrade.

Behavioral coverage in `tests/test_cache.py` uses actual hardlinks, locks, threads
and hard-exit subprocesses. Native exploration, maintenance and benchmark tests
also exercise the cache through PostgreSQL and the controlled HTTP object store.

Analytical operations use `History.read_session()` around their product partitions.
Within one session, a warm file needs one SHA-256 verification while its retained
pin and ordinary cache name still identify the same device, inode, byte size,
nanosecond modification time and nanosecond change time. Every reuse checks that
identity again. Replacing or modifying the file, including restoring its previous
mtime after a same-size write, forces the ordinary checksum/refill path. Removing
the ordinary name through retirement prevents another read through the retained
pin. Existing readers retain their usual protection until they finish. Sessions
do not replace SQL input-publication or retention guards, and verification trust
never survives an operation.

Retained idle pins use an LRU bounded by 64 entries and the smaller of 256 MiB or
one quarter of `CACHE_MAX_BYTES`. They participate in the same global inode-byte
accounting as every other reader. Larger files and working sets use normal
verification instead; a session never pins a complete multi-gigabyte corpus.
Before retrying an admission blocked by capacity, it relinquishes its own idle
pins. Genuine active readers and downloads still enforce the global budget.
Independent operations/processes can therefore cause the existing bounded
capacity error and retry behavior. Exceptions and normal completion release
session pins; dead processes use the same kernel-lock recovery as other readers.

Sessions retain only a checksum with stable inode metadata throughout verification.
Cold downloads verify their incoming stream and keep the ordinary reader lifetime;
their first subsequent warm read establishes the reusable identity. Ordinary
eviction recency refresh happens when a session releases an unchanged pin, so
touching the file on every product partition cannot invalidate its own reuse.
The native regression compares whole assessment, frozen-decision and maturation
results for 20 product partitions: hashing drops from 20 reads of the common
Parquet file to one, with no extra object-store GET. This bounds repeated checksum
work; it does not imply an equivalent reduction in complete pipeline runtime.
