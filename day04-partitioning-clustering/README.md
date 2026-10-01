# Day 4 — Partitioning and clustering, benchmarked

**Hadoop equivalent:** Hive partitions and bucketing. Partitioning works the
same way (whole partitions are skipped). Clustering is different from
bucketing: rows are sorted and each block records its min/max, so BigQuery
skips blocks at query time.

Source: `bigquery-public-data.stackoverflow.posts_questions`, 2015–2018.
**8,402,679 rows / 604 MB**, copied into four identical tables differing only in physical layout.

| Table | Partitioned by | Clustered by |
|---|---|---|
| `t_plain` | — | — |
| `t_part` | `DATE(creation_date)` (1,461 daily partitions) | — |
| `t_part_clust` | `DATE(creation_date)` | `owner_user_id` |
| `t_clust` | — | `owner_user_id` |

All queries: `SELECT COUNT(*), AVG(score) FROM <table> WHERE <predicate>`, run with `--nouse_cache`.

## Result 1 — partition pruning (predicate: one month of dates)

| Table | MB scanned | Slot ms |
|---|---|---|
| `t_plain` | 128.2 | 803 |
| `t_part` | **3.1** | 179 |
| `t_part_clust` | **3.1** | 167 |

**41× less data scanned.** Partition pruning is the only mechanism that reduces bytes billed.

## Result 2 — clustering (predicate: one `owner_user_id`, no date filter)

| Table | Dry-run est. MB | **Actual** MB | Slot ms |
|---|---|---|---|
| `t_plain` | 126.9 | 126.9 | 342 |
| `t_part` | 126.9 | 126.9 | **33,441** |
| `t_clust` | 126.9 | **5.4** | **86** |

Three findings:
1. **Clustering cut actual bytes 23×** (126.9 → 5.4 MB).
2. **The dry run could not see it** — it reported the full 126.9 MB. Clustering savings are only visible *after* execution. Partition pruning, by contrast, is visible in the dry run.
3. **Partitioning on a column you don't filter by is actively harmful**: 33,441 ms vs 342 ms slot time — ~100× worse — because the query must open 1,461 small partitions instead of scanning contiguous blocks. Same bytes billed, far more compute.

## Result 3 — clustering inside small partitions does nothing

Predicate: full year 2017 + one `owner_user_id`.

| Table | Actual MB |
|---|---|
| `t_part` | 48.1 |
| `t_part_clust` | 47.7 |

Only 0.8% better. Daily partitions here hold ~5,700 rows — too small to contain multiple blocks, so there is nothing for clustering to prune. **Clustering needs large partitions to work.**

## Rules derived

- Partition on the column you filter by **most often**, usually a date. If you don't filter on it, don't partition.
- Match partition granularity to partition size. Daily partitions under ~1 GB are usually too fine; use monthly or yearly.
- Cluster on the high-cardinality column you filter or join on, listed in order of most-used first (max 4 columns).
- Set `require_partition_filter = TRUE` on every large partitioned table.
- Verify pruning with **actual** `totalBytesProcessed` from job statistics, not the dry-run estimate — the dry run cannot predict clustering.
- Hard limit: **4,000 partitions per table**.

SQL: [build.sql](build.sql) · [benchmark.sql](benchmark.sql)
