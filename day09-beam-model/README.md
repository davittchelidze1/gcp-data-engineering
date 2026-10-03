# Day 9 — The Beam model: windows, watermarks, triggers

**Hadoop equivalent:** Spark Structured Streaming or Flink — the same event-time
ideas. Everything here runs locally on the DirectRunner, for free, exactly as it
would on Dataflow.

```bash
python3.12 -m venv venv && venv/bin/pip install -r requirements.txt   # Beam 2.76 doesn't support Python 3.14
python windows_demo.py
python triggers_demo.py
```

## Windows — [windows_demo.py](windows_demo.py)
Eight page views by alice and bob, stamped with *event time* (seconds):

| Strategy | alice's result |
|---|---|
| Global (batch default) | 5 |
| Fixed, 60 s | `[0,60)` → 3 · `[60,120)` → 2 |
| Sliding, 60 s every 30 s | every event lands in two windows |
| **Session, 30 s gap** | `[0,42)` → 3 · `[100,134)` → 2 |

Session windows are sized by the data itself, per key: bob's sessions came out
`[3,38)` and `[102,132)` — different boundaries from alice's.

## Watermarks and late data — [triggers_demo.py](triggers_demo.py)
Six events, all in window `[0,60)`. `TestStream` advances the watermark to 70
after the fourth, so the fifth and sixth arrive **late**. Trigger: fire early
every 2 elements, on time at the watermark, and once per late element.

| Pane | Accumulating | Discarding | Accumulating, lateness 0 s |
|---|---|---|---|
| EARLY #0 | 2 | 2 | 2 |
| EARLY #1 | 4 | 2 | 4 |
| ON_TIME #2 | 4 | 0 | 4 |
| LATE #3 | 5 | 1 | *dropped* |
| LATE #4 | 6 | 1 | *dropped* |

- **Accumulating** panes repeat the running total; **discarding** panes carry
  only what's new — the consumer has to add them up.
- **Allowed lateness 0 → late events are silently dropped.** No error, no log
  line; the numbers are just smaller.
