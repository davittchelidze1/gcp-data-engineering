# Day 8 — Pub/Sub

**Hadoop equivalent:** Kafka — without partitions to size or consumer offsets to
manage. It scales itself; you give up some control in exchange.

## Findings

**Order is not guaranteed by default.** Three messages published as 1, 2, 3
came back 2, 1, 3. **Ordering keys** fix that per key, on a subscription created
with `--enable-message-ordering` — the loose cousin of a Kafka partition key.

**Dead-letter topics work exactly as configured.** A subscription with
`--max-delivery-attempts=5`, fed a message I refused to acknowledge:

| Pull attempt | Result |
|---|---|
| 1–5 | received, not acked |
| 6 | nothing — it had moved on |
| dead-letter subscription | `POISON-cannot-parse-this` |

The Pub/Sub service agent needs *publisher* on the dead-letter topic and
*subscriber* on the source subscription, or nothing gets moved.

**A BigQuery subscription replaces a whole pipeline.** Four messages landed in a
BigQuery table — data, attributes, publish time — with no consumer code and no
Dataflow job. If the transform is trivial, don't build one.

**Replay by timestamp.** Consume and ack three messages, `seek` the subscription
back to a moment before them, pull again: all three redelivered. It needs
`--retain-acked-messages` on the subscription first — the first attempt returned
nothing because retention had only just been switched on.

Commands: [lab.sh](lab.sh)
