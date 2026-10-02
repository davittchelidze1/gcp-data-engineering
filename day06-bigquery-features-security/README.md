# Day 6 — MERGE, recovery, security and BigQuery ML

**Hadoop equivalent:** Hive `MERGE INTO` for upserts, HDFS snapshots for recovery,
and Ranger for access control — except all of it is built into the warehouse.

## SCD Type 2 with a single MERGE — [scd2_merge.sql](scd2_merge.sql)
Day 1 load, then a day 2 batch with one unchanged, two changed and one new customer:

| customer | name | city | tier | valid_from | valid_to | current |
|---|---|---|---|---|---|---|
| 1 | Alice | Tbilisi | SILVER | 2026-01-01 | 9999-12-31 | ✅ |
| 2 | Bob | Batumi | GOLD | 2026-01-01 | 2026-01-02 | |
| 2 | Bob | Batumi | PLATINUM | 2026-01-02 | 9999-12-31 | ✅ |
| 3 | Carol | Kutaisi | SILVER | 2026-01-01 | 2026-01-02 | |
| 3 | Carol | Batumi | SILVER | 2026-01-02 | 9999-12-31 | ✅ |
| 5 | Eve | Gori | GOLD | 2026-01-02 | 9999-12-31 | ✅ |

The trick: the source is unioned with itself. Changed rows appear once with a
join key (to expire the current version) and once with a NULL key (so they never
match, and insert the new version). Changes are detected with an MD5 row hash.

## Recovery — [recovery.sql](recovery.sql)
- **Snapshot** (read-only) and **clone** (writable) are metadata-only copies.
- **Time travel** (7 days): deleted a customer, queried `FOR SYSTEM_TIME AS OF`
  the moment before — 7 rows then, 5 now — and restored.
- Two things that bit me: a timestamp string without a zone was read as the
  future, so I switched to `UNIX_MICROS`; and a table can't be read with time
  travel and written in the same statement — restore through a snapshot or a
  staging table.

## Three layers of security — [security.sql](security.sql), [policy_tags.sh](policy_tags.sh)

| Layer | Test | Result |
|---|---|---|
| Row-level policy `city = 'Batumi'` | same query, as me vs as the service account | 7 rows vs 3 |
| Authorized view | service account: view vs base table | ✅ view · ❌ table |
| Column policy tag on `name` | query `name` as the **project owner** | ❌ denied until granted Fine-Grained Reader |

- **Once any row policy exists on a table, everyone is filtered** — including the
  owner, who saw 0 rows until given a `FILTER USING (TRUE)` policy.
- **Policy tags beat project ownership.** Column access is its own permission.
- Revoking access took about a minute to take effect — IAM changes propagate.

## Also
- **Load job vs external table** ([load_and_external.sh](load_and_external.sh)):
  a load copies data into BigQuery; an external table queries it in place — and
  the dry run can't estimate its cost.
- **Materialized view, SQL UDF, stored procedure** ([mv_udf_procedure.sql](mv_udf_procedure.sql)).
- **BigQuery ML** ([bqml.sql](bqml.sql)): a linear regression trained, evaluated
  and used in SQL — R² 0.88, MAE 34.1 on 20k synthetic orders.
- Scheduled queries: `bq mk --transfer_config` needs an interactive OAuth step,
  so these are easier to set up in the console.
