# How to explain the performance experiment

The baseline is a controlled sort-merge comparison with AQE disabled. It is not a claim about Spark's default performance. All cases use the same source-month filter, rows, analytical grain and output columns.

## Joins and shuffles

The saved baseline warmup plan contains two `SortMergeJoin` operators. Its broadcast-only counterpart contains two `BroadcastHashJoin` operators and no sort-merge joins. The 265-row lookup is small enough to broadcast; pickup and drop-off are enriched separately using zone IDs.

Broadcasting the lookup avoids repartitioning and sorting trip rows for those joins. It does not eliminate the exchanges needed by route aggregation and the ranking window. Read the `Exchange` operators and task shuffle counters to see the remaining work.

The Parquet scans show source-month `PartitionFilters` in every case. Pruning is part of the shared workload, not an advantage granted only to the tuned case. The CSV scan naturally has no source-month partition filter.

## Three different meanings of partition

1. **Storage partition:** the clean files are published under `source_month=YYYY-MM`. Source ownership remains stable even if a trip's pickup date lies outside its file month.
2. **Execution partition:** Spark divides scan and shuffle work into tasks. The configured shuffle counts are 32 or 8, depending on the case. They are not the number of months or necessarily the number of output files.
3. **Output file:** the measured Parquet file count depends on the final execution plan. AQE can coalesce shuffle partitions. The tuned warmup was configured with eight shuffle partitions and actually wrote three files; the baseline and broadcast-only warmups wrote 32.

Inspect `adaptive-plans.txt` for the executed adaptive updates as well as `plan.txt` for the initial query plan. Do not infer executed behavior solely from configuration values.

## Equality and measurement scope

Rows can be ordered and distributed differently across output files without changing the result. File hashes or equal file sizes are not sufficient tests. The suite uses distributed `exceptAll` in both directions, preserving duplicates, against a reference result.

The query timer includes construction, execution and output writing. It excludes JVM startup, zone-lookup validation and the common initialization action. Summed task runtime and shuffle/spill counters cover the whole application; they are labeled accordingly. Summed task runtime is not the same as elapsed wall time, because tasks overlap.

There is one discarded full workload per case, followed by three measured repetitions in rotated order. Every application has a fresh JVM. Filesystem cache state and other workstation activity are uncontrolled. Report the individual runs and medians, and distinguish observed local improvements from general performance claims.

Tuned changes both AQE and shuffle count relative to broadcast-only. That combined comparison cannot isolate the effect of AQE alone. The broadcast-only comparison holds those other settings fixed.

Warmup observations above explain plan behavior; the final verified benchmark summary supplies the reported performance results.
