"""Day 9 part 2 - watermarks, triggers, late data. DirectRunner, $0."""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms import window, trigger
from apache_beam.transforms.window import TimestampedValue

TIMING = {0: "EARLY", 1: "ON_TIME", 2: "LATE", 3: "UNKNOWN"}

class ShowPane(beam.DoFn):
    def process(self, kv, win=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
        k, v = kv
        yield "   window[%3.0f-%3.0f)  %-8s pane#%d  first=%-5s last=%-5s  ->  %s = %d" % (
            float(win.start), float(win.end),
            TIMING.get(int(pane.timing), "?"), pane.index,
            pane.is_first, pane.is_last, k, v)

def build_stream():
    """Events all land in the fixed window [0,60).
       The watermark is advanced past 60 BEFORE two of them arrive -> those are LATE."""
    return (TestStream()
            .advance_watermark_to(0)
            .add_elements([TimestampedValue(("clicks", 1), 5)])
            .add_elements([TimestampedValue(("clicks", 1), 10)])
            .advance_watermark_to(20)
            .add_elements([TimestampedValue(("clicks", 1), 15)])
            .add_elements([TimestampedValue(("clicks", 1), 25)])
            .advance_watermark_to(70)                       # <-- window [0,60) is now "complete"
            .add_elements([TimestampedValue(("clicks", 1), 30)])   # LATE by 40s
            .advance_watermark_to(90)
            .add_elements([TimestampedValue(("clicks", 1), 40)])   # LATE by 60s
            .advance_watermark_to_infinity())

def run(title, accumulation, allowed_lateness):
    print("\n=== %s ===" % title)
    opts = PipelineOptions()
    opts.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=opts) as p:
        (p
         | build_stream()
         | beam.WindowInto(
             window.FixedWindows(60),
             trigger=trigger.AfterWatermark(
                 early=trigger.AfterCount(2),     # fire early every 2 elements
                 late=trigger.AfterCount(1)),     # fire again on every late element
             accumulation_mode=accumulation,
             allowed_lateness=allowed_lateness)
         | beam.CombinePerKey(sum)
         | beam.ParDo(ShowPane())
         | beam.Map(print))

print("6 events, all with event-time inside window [0,60).")
print("Watermark jumps to 70 after the 4th -> events 5 and 6 arrive LATE.\n")

run("A. ACCUMULATING, allowed_lateness=60s  (each pane repeats the running total)",
    trigger.AccumulationMode.ACCUMULATING, 60)

run("B. DISCARDING, allowed_lateness=60s  (each pane holds only NEW data)",
    trigger.AccumulationMode.DISCARDING, 60)

run("C. ACCUMULATING, allowed_lateness=0s  (late data is DROPPED)",
    trigger.AccumulationMode.ACCUMULATING, 0)
