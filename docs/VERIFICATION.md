# Executed verification — November 19, 2026

The separate local repository is complete for January–March 2024. Execution used the digest-pinned Apache Spark 3.5.7 image, Python 3.8.10, Java 11.0.27, `local[2]`, a 768 MiB JVM heap and a 2,300 MiB container limit. Docker reported 12 virtual CPUs and 7.604 GiB total engine memory. The existing Airflow platform remained running throughout; its nine services were healthy and its Git working tree unchanged at the final check.

## Actual data processed

| Source month | Source rows | Accepted rows | Quarantined rows | Signed passenger charges |
|---|---:|---:|---:|---:|
| January 2024 | 2,964,624 | 2,964,568 | 56 | $79,454,508.28 |
| February 2024 | 3,007,526 | 3,007,519 | 7 | $80,073,431.28 |
| March 2024 | 3,582,628 | 3,582,511 | 117 | $97,158,773.53 |
| **Total** | **9,554,778** | **9,554,598** | **180** | **$256,686,713.09** |

Every rejected record had `negative_duration`; counts reconcile exactly with the source. Accepted data retains 115,895 negative-charge records and 56 pickup timestamps outside their owning source month. Accepted metered fares sum to $175,086,476.40. Charges include recorded fees/tips and refunds; they are not operator revenue and exclude cash tips.

The three original Parquet artifacts total 160,389,205 bytes (152.96 MiB). The zone lookup contains 265 rows and is 12,331 bytes. Inputs were copied from the original project's data directory and checked with SHA-256; original download times were not invented. [Input provenance](input-manifest.json) records file hashes and official URLs.

The current accepted snapshot has six Parquet files totaling 255,738,098 bytes, excluding quarantine and retained historical versions. This project does not claim a storage-size reduction relative to the original Parquet files.

## Reprocessing and failure recovery

The first full January attempt failed with a JVM heap error while caching a wide projection containing raw-record JSON. No catalog entry was published. Removing that unnecessary cache lets Catalyst prune JSON from counts/accepted writes and serialize it only for quarantine. Subsequent runs succeeded under the same memory limit.

The real January repeat passed exact multiset comparisons in both directions, preserving duplicates, and aggregate equality:

- 2,964,568 accepted records.
- $79,454,508.28 signed passenger charges.
- $53,880,405.30 metered fares.

A controlled replacement wrote and validated new output, then failed before catalog publication. Its failure audit persisted. The catalog and last successful data remained unchanged, and the verifier read the published data again to confirm preservation. See [real-data verification evidence](real-verification.json).

Source partitions publish independently; a multi-month invocation is not one transaction. Old/failed versions remain available for inspection. File locking and atomic catalog rename are local filesystem mechanisms, not an object-store ACID protocol or a tested disaster-recovery backup.

## Persisted reporting and dashboard

Reporting covers January 1 through March 31, 2024: **9,554,577 trips**, **$256,686,172.70** in charges, and **262 observed pickup zones**. Twenty-one accepted records outside that calendar remain in clean storage and are counted separately.

An independent final Spark application read the persisted tables back and reconciled every table's summed trip counts and charges with the report manifest:

| Written table | Table rows | Summed trips | Summed charges |
|---|---:|---:|---:|
| Hourly | 240,900 | 9,554,577 | $256,686,172.70 |
| Routes | 37,241 | 9,554,577 | $256,686,172.70 |
| Dense hourly demand | 572,208 | 9,554,577 | $256,686,172.70 |

The report publication phase took 510.017 seconds. This is an observed duration, not an engine comparison. Initial ingestion and real recovery verification used Windows-mounted shuffle scratch. Subsequent reporting and all benchmark applications used Linux-local temporary scratch under the same memory/CPU limits.

The dashboard was verified through its actual HTTP API and in-app browser at `http://localhost:8502/`. Counts and charges match the persisted report. The demand trend spans all 91 days, with a peak of 140,493 trips/day; Midtown Center is the top pickup zone with 453,822 trips. Route and demand-spike tables rendered correctly. Refresh displayed all three verified benchmark medians, the January workload size, nine measured runs and 506,476 identical result rows. [Preview](dashboard.jpg) · [Full dashboard](dashboard-full.jpg).

## Verified performance experiment

The workload uses **January's 2,964,568 accepted source-partition trips**, enriches both zone names, groups by pickup/drop-off IDs, labels, weekday and hour, sums exact charges and ranks routes. The reference output has **506,476 rows** and reconciles to **2,964,568 trips / $79,454,508.28**. All nine measured Parquet outputs passed distributed `exceptAll` comparison in both directions. Source partition versions stayed equal throughout.

One discarded full workload ran for each case. Measured order rotated: broadcast/tuned/baseline; tuned/baseline/broadcast; baseline/broadcast/tuned. Each application used a fresh JVM. Query timing includes construction, execution and Parquet writing; it excludes startup, lookup validation and a common initialization action. Shuffle counters cover the whole application, including preparation. See [complete benchmark evidence](benchmark-results.json) and [plan notes](PLAN_NOTES.md).

| Case | Repetition 1 (s) | Repetition 2 (s) | Repetition 3 (s) | Median (s) | Median shuffle write (MiB) | Result files |
|---|---:|---:|---:|---:|---:|---:|
| Baseline: merge joins, AQE off, 32 shuffle partitions | 66.863 | 90.462 | 76.997 | **76.997** | 99.388 | 32 |
| Broadcast joins, AQE off, 32 shuffle partitions | 81.840 | 82.153 | 66.752 | **81.840** | 65.953 | 32 |
| Tuned: broadcast joins, AQE on, 8 configured shuffle partitions | 70.312 | 44.370 | 47.717 | **47.717** | 66.601 | 3 |

Tuned's observed local median was **38.0% lower** than the controlled baseline, with **33.0% fewer whole-application shuffle-write bytes**. AQE reduced the final file count to three in every tuned repetition. Broadcasting alone reduced shuffle bytes but had a **6.3% higher median elapsed time** in this experiment. Fewer shuffle bytes did not automatically mean a faster run.

The baseline is a controlled comparison with AQE disabled, not a measurement of Spark's default settings. Tuned changes broadcast strategy, AQE and shuffle count together relative to baseline. Timings vary substantially; workstation activity, filesystem cache and fresh-JVM behavior remain uncontrolled. These results support local observations, not general scalability, default-Spark, pandas, PostgreSQL, cost or multi-node performance claims.

Saved plans show two sort-merge joins in baseline and two broadcast hash joins in the broadcast cases. All cases retain the same source-month partition filter. The tuned executed adaptive plan includes coalesced shuffle reads. Files: [baseline](baseline-plan.txt), [broadcast-only](broadcast_only-plan.txt), [tuned initial](tuned-plan.txt), [tuned adaptive](tuned-adaptive-plan.txt).

## Tests actually executed

- Six Spark fixture tests passed initially in 218.651 seconds, then all six passed on the revised implementation in **262.193 seconds**. They cover schema/lookup rejection, duplicate/refund preservation, multiple rejection reasons, full calendar boundaries, four-week demand history, writer exclusion, skip/strict threshold behavior, repeat processing, controlled failure and reporting reconciliation. The final fixture run overlapped ingestion; its duration is not a benchmark. [Transcript](fixture-test-results.txt).
- The HTTP integration test passed, including aggregate payload and stale-source detection. Its final run took **1.224 seconds**. Windows sandbox socket access was restricted; the same test passed outside that sandbox. [Transcript](dashboard-http-test-results.txt).
- Real January–March Spark ingestion, real January replacement/recovery, persisted-table reconciliation, nine measured benchmark runs and exact result comparisons all completed successfully.
- The live dashboard API check passed against the published count, charges, hourly/daily totals and verified benchmark. Browser inspection confirmed the rendered metrics and tables.
- Ruff 0.14.5 and Python compilation checks passed. CI configuration uses the pinned Spark image and synthetic fixtures; remote GitHub Actions has not been executed for this local repository.

The full orchestration sequence exited zero. [Final evidence](final-evidence.json) contains runtime versions, input/schema lineage, written-table reconciliation and measured dataset statistics. Source artifacts, individual trip records and runtime logs remain in ignored `data/`.

## Honest resume bullets

- Built a Dockerized PySpark batch analytics pipeline processing 9.55 million NYC taxi source records with schema validation, quarantine, immutable monthly versions and atomic local publication.
- Implemented pickup/drop-off dimension joins, approximate route-duration percentiles, dense hourly demand calendars and four-week window baselines; reconciled three persisted analytical tables to 9.55 million in-calendar trips and exact passenger charges.
- Benchmarked three Spark SQL configurations across nine measured runs on 2.96 million input trips; observed a 38% lower local median query-and-write time and 33% lower whole-application shuffle-write volume against a controlled baseline, with exact output equality.

## Remaining scope limits

Only January–March 2024 yellow taxi data and single-machine execution are verified. Full-year expansion, other historical schemas, multi-node/cloud execution, streaming, forecasting, backup recovery and remote CI are not claimed. The official-download helper is implemented; this verification used copied existing artifacts. Route percentiles are approximate and include accepted long-duration outliers. File retention has no automatic garbage collection. The new repository is local; no new GitHub repository, push or PR was performed for this project.
