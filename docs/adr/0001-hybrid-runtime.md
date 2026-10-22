# Use Rust for collection and native columnar engines for analysis

Status: accepted by the user.

Collectors and normalization use Rust with bounded asynchronous work to control memory and concurrency. Analysis uses Python interfaces to Polars and DuckDB so most data processing executes in native engines; additional Rust kernels require profiling evidence. This balances operational efficiency with iteration speed without maintaining every analytical transformation twice.
