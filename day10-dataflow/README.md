# Day 10 — A batch Dataflow job with a dead-letter path

**Hadoop equivalent:** submitting a Spark job to YARN — except Google runs and
scales the workers. The trap: **a batch job ends itself, a streaming job runs
until you cancel it** — the cloud version of a forgotten long-running YARN app,
billed by the hour.

## The pattern — [pipeline.py](pipeline.py)
Ten raw events ([events.jsonl](events.jsonl)), three of them broken. One `ParDo`
emits valid rows on its main output and routes failures to a tagged
`dead_letter` output, with the error attached:

| Output table | Rows |
|---|---|
| `df_events` | 7 |
| `df_events_dead_letter` | 3 |

The three rejects each failed differently:

| Raw line | Error |
|---|---|
| `{"event_id": 5, "user": "bob", "action": "click"}` | `KeyError: 'value'` — valid JSON, wrong schema |
| `{"broken json": }` | `JSONDecodeError` at char 16 |
| `THIS IS NOT JSON AT ALL` | `JSONDecodeError` at char 0 |

A bad row becomes a queryable record with a reason, not a crashed job or a
silently dropped line.

## Findings
- **Run it on the DirectRunner first**, locally and free; same pipeline, same
  output tables. Then the same code on `--runner DataflowRunner`: job state
  `Done`, type `Batch` — it stopped by itself.
- **Batch writes should use `FILE_LOADS`.** Load jobs are free. The Storage Write
  API is for streaming — and from Python it needs a local Java expansion service,
  which is how I found out.
- **Drain vs cancel** (streaming): drain finishes in-flight data before stopping;
  cancel drops it. Drain in production.

Run: [run.sh](run.sh)
