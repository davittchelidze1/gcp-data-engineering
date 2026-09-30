# Day 2 — Cloud Storage is not HDFS

**Hadoop equivalent:** HDFS — but it's an object store, and two differences
change how Spark behaves on it.

## Findings

**There are no directories.** `ls` shows `data/year=2024/`, but asking for the
object `data/` returns 404. The whole path is one object *name*; "folders" are
just shared prefixes.

**Rename is copy + delete.** An object's generation number changed on every
`mv`: a new object was written and the old one deleted. On HDFS, rename is a
single NameNode metadata edit.

**That's why Spark commits are slow here.** I simulated the classic
`FileOutputCommitter` — write to `_temporary/`, then move into place. 60 files
took 13.6 s of real work: **0.227 s per file**.

| Output files | Commit time at 0.227 s/file |
|---|---|
| 60 | 14 s |
| 1,000 | ~4 min |
| 10,000 | ~38 min — a fully provisioned cluster doing nothing but copying |

The fixes: write fewer, larger files; use a table format that commits through
metadata (Iceberg); or write straight to BigQuery.

**No data locality, and listing costs money.** Every read is a network call, and
listing is paginated API calls — so deep `year=/month=/day=/hour=` trees and
small files hurt more here than on HDFS.

**Strongly consistent.** A write is visible to the very next read, everywhere.

**Versioning is an undo button.** I overwrote `report.txt`, listed both
generations, and copied the old generation back.

**Lifecycle rules** tier and delete on their own ([lifecycle.json](lifecycle.json)).
Colder classes have minimum storage durations — Nearline 30 days, Coldline 90,
Archive 365 — so don't tier data that gets rewritten often.

Commands: [lab.sh](lab.sh)
