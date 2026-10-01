# Day 5 — BigQuery cost, from my own job history

**Hadoop equivalent:** reading the YARN / Hive job history — except here it's a
SQL table (`INFORMATION_SCHEMA.JOBS_BY_PROJECT`) you can group, join and alert on.

## Two meters, not one

| Meter | Charged under |
|---|---|
| bytes billed (data read) | on-demand pricing |
| slot-milliseconds (compute) | Editions / reserved slots |

They don't move together. From my own history, two table builds of the same
604 MB of data:

| Statement | Slot time per MB billed |
|---|---|
| `CREATE TABLE … PARTITION BY DATE(...)` (1,461 partitions) | 4,364 ms |
| `CREATE TABLE … CLUSTER BY …` | 127 ms |

Writing many small partitions is expensive *work* that on-demand billing never
shows you — and Editions pricing would.

## Findings

**Approximate counting.** `APPROX_COUNT_DISTINCT` vs `COUNT(DISTINCT)` over 8.4M
rows: **4.4× less compute, 0.049% error** (2,118,986 vs 2,120,018). Use it for
dashboards and exploration; not for invoices.

**Storage is billed two ways.** My tables compressed 3.1–3.5×, so physical
(compressed) billing came to $0.034/month vs $0.062 logical — about 45% cheaper
despite the higher per-GB rate. Anything untouched for 90 days drops to
long-term storage at roughly half price, automatically.

**Join order doesn't matter.** `large JOIN small` 995 slot-ms vs
`small JOIN large` 981 — 1.4%. The optimizer reorders; an old Hive habit to drop.

**Skew check:** `computeMsMax ÷ computeMsAvg > 3` on a stage means one worker is
doing most of the work. My synthetic 90%-one-key test didn't reproduce it — the
optimizer collapsed the query into one stage — so I verified the method on a
real 5-stage join instead (ratios 1.4–1.8×, healthy).

**When to trust the dry run:**

| Query | Estimate | Actual |
|---|---|---|
| `COUNT(*)` | 0 | 0 |
| full column scan | 64.1 MB | 64.1 MB |
| partition-pruned | 3.1 MB | 3.1 MB |
| **cluster-pruned** | **126.9 MB** | **5.4 MB** |
| join | 70.5 MB | 70.5 MB |

Exact everywhere except clustering, where it's pessimistic.

**The total:** 65 queries and 9.38 GiB billed across days 0–5 — 0.92% of the
monthly free tier. On-demand, about 4 cents.

Queries: [queries.sql](queries.sql)
