"""Day 9 - the Beam model, all on the local DirectRunner ($0)."""
import apache_beam as beam
from apache_beam.transforms import window

# (user, page_view_at_second)
EVENTS = [
    ("alice", 0), ("alice", 5), ("alice", 12),      # burst 1
    ("bob",   3), ("bob",   8),
    ("alice", 100), ("alice", 104),                  # burst 2, long gap before it
    ("bob",   102),
]

class Stamp(beam.PTransform):
    """Create the events, then attach EVENT TIME to each one."""
    def expand(self, pcoll):
        return (pcoll
                | "create" >> beam.Create(EVENTS)
                | "stamp"  >> beam.Map(
                    lambda kv: beam.window.TimestampedValue((kv[0], 1), kv[1])))

def stamped():
    return Stamp()

class Label(beam.DoFn):
    """Print each result together with the window it belongs to."""
    def process(self, kv, win=beam.DoFn.WindowParam):
        user, count = kv
        if isinstance(win, window.GlobalWindow):
            w = "GLOBAL"
        else:
            w = "[%6.0f -> %6.0f)" % (float(win.start), float(win.end))
        yield "   %-18s %-8s %d" % (w, user, count)

def run(title, windowing):
    print("\n=== %s ===" % title)
    with beam.Pipeline() as p:
        (p
         | "src" >> stamped()
         | "win"   >> windowing
         | "count" >> beam.CombinePerKey(sum)
         | "label" >> beam.ParDo(Label())
         | "out"   >> beam.Map(print))

print("EVENTS (user, event-time seconds):")
for u, t in EVENTS:
    print("   %-6s t=%d" % (u, t))

# 1. NO windowing - everything collapses into one global window
run("1. GLOBAL WINDOW (default, batch)", beam.WindowInto(window.GlobalWindows()))

# 2. FIXED - non-overlapping buckets of 60s
run("2. FIXED WINDOWS (60s)", beam.WindowInto(window.FixedWindows(60)))

# 3. SLIDING - 60s wide, emitted every 30s, so events land in 2 windows
run("3. SLIDING WINDOWS (60s wide, every 30s)", beam.WindowInto(window.SlidingWindows(60, 30)))

# 4. SESSION - windows grow to fit activity, close after 30s of silence
run("4. SESSION WINDOWS (30s gap)", beam.WindowInto(window.Sessions(30)))
