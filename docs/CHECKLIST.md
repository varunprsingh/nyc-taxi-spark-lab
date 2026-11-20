# Acceptance checklist

- [x] Separate Git repository; original Airflow source and services preserved.
- [x] Digest-pinned local Spark Compose runtime; no cloud dependency or exposed Spark port.
- [x] Reuse existing TLC files with verified checksums; official-download helper for fresh machines.
- [x] DataFrame quality transformations and quarantine; duplicate/refund preservation.
- [x] Immutable monthly versions, file lock and atomic catalog publication.
- [x] Spark fixture verification of repeat and failed replacement preservation.
- [x] Pickup/drop-off zone joins, hourly/route aggregation, approximate percentiles.
- [x] Dense-calendar window baseline, previous week and zone ranking.
- [x] Benchmark configurations, physical plans, event-log metrics, rotated repetitions and exact result checks.
- [x] Small-fixture CI configuration, local lint and HTTP dashboard tests.
- [x] Real January–March ingestion and measured reconciliation: 9,554,598 accepted, 180 quarantined.
- [x] Real January repeat equality and controlled failure preservation: exact multiset, counts, charges and catalog.
- [x] Real reporting build and dashboard browser verification: 9,554,577 trips, $256,686,172.70, 262 observed pickup zones.
- [x] Executed benchmark suite: nine measured runs, exact equality and source-total reconciliation.
- [x] Independent read-back reconciliation of all three persisted reporting tables.
- [x] Final evidence, limitations, runnable demo commands and local commit.

Remote GitHub publishing and remote CI execution are not included without a separate publishing request. No full-year or multi-node processing claim is made.
