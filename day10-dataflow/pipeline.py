"""Day 10 - a BATCH Dataflow job with the dead-letter pattern.

Batch jobs end themselves. Streaming jobs run until cancelled - which is
the single fastest way to burn trial credits. This one is batch on purpose.
"""
import argparse, json, logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

GOOD = "good"
BAD = "dead_letter"

GOOD_SCHEMA = "event_id:INTEGER,user:STRING,action:STRING,value:INTEGER"
BAD_SCHEMA = "raw_line:STRING,error:STRING"


class ParseEvent(beam.DoFn):
    """Emit valid rows on the main output, unparseable ones to a dead-letter tag."""

    def process(self, line):
        try:
            rec = json.loads(line)
            # a schema check, not just a JSON check
            out = {
                "event_id": int(rec["event_id"]),
                "user": str(rec["user"]),
                "action": str(rec["action"]),
                "value": int(rec["value"]),
            }
            yield out
        except Exception as e:
            yield beam.pvalue.TaggedOutput(
                BAD, {"raw_line": line[:900], "error": "%s: %s" % (type(e).__name__, e)}
            )


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--dataset", required=True)
    known, pipeline_args = parser.parse_known_args(argv)

    opts = PipelineOptions(pipeline_args)
    opts.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=opts) as p:
        parsed = (p
                  | "read" >> beam.io.ReadFromText(known.input)
                  | "parse" >> beam.ParDo(ParseEvent()).with_outputs(BAD, main=GOOD))

        # ---- good rows -> BigQuery via the Storage Write API ----
        (parsed[GOOD]
         | "write_good" >> beam.io.WriteToBigQuery(
             table="%s.df_events" % known.dataset,
             schema=GOOD_SCHEMA,
             method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

        # ---- bad rows -> a quarantine table you can actually inspect ----
        (parsed[BAD]
         | "write_bad" >> beam.io.WriteToBigQuery(
             table="%s.df_events_dead_letter" % known.dataset,
             schema=BAD_SCHEMA,
             method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
