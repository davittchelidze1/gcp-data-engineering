# Day 3 — How BigQuery actually executes a query

**Hadoop equivalent:** Hive — except there is no cluster to size. Storage and
compute are separate: data sits in Google's distributed storage, and compute
(slots) is summoned per query and released.

## You pay for columns read, not rows returned

Dry runs against `stackoverflow.posts_questions` — all free:

| Query | Bytes scanned |
|---|---|
| `SELECT *` | 37.17 GB |
| `SELECT * … LIMIT 10` | 37.17 GB — **identical**; LIMIT is applied after the scan |
| `SELECT body` (one big text column) | 33.51 GB — 90% of the table |
| `SELECT id, title` | 1.39 GB |
| `SELECT id` | **0.17 GB — 217× cheaper than `SELECT *`** |
| `SELECT id WHERE score > 100` | 0.34 GB — **more**: the filter has to read `score` too |
| `SELECT COUNT(*)` | 0 bytes — row counts are metadata |

Each column is stored separately (the Capacitor format), so only partitioning
can make a filter *reduce* the bill (day 4).

## One real join, dissected

Questions joined to users, grouped by name — 0.71 GB scanned.

| Stage | Workers | Rows in → out | Shuffle out |
|---|---|---|---|
| S00: Input (users) | 17 | 18.7M → 18.7M | 446 MB |
| S01: Input (questions) | 152 | 23.0M → 22.6M | 387 MB |
| S02: Join+ | 100 | 41.3M → 4.3M | 165 MB |
| S03: Sort+ | 16 | 4.3M → 320 | — |
| S04: Output | 1 | 320 → 20 | — |

- **152 → 100 → 16 → 1 is the Dremel tree**: leaves scan, mixers combine, one
  root answers. BigQuery sized every stage itself — no executors, no memory
  settings.
- **Wall clock 1.6 s, slot time 55.8 s** → about 36 slots working at once. A
  slot is roughly one CPU thread. Slot time is the number to optimise; wall
  clock depends on how busy the system is.
- **S03 → S04 is a distributed top-N**: 16 workers each kept their top 20, the
  root merged 320 rows down to 20.
- **Skew check:** `computeMsMax ÷ computeMsAvg` stayed under 2× on every stage.
  Above ~3×, one key is dominating.
- Re-running the exact query returns from the result cache — instant, 0 bytes.

Commands: [dry_runs.sh](dry_runs.sh) · [join.sql](join.sql) · [plan.sh](plan.sh)
