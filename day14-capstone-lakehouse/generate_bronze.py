"""Land synthetic raw orders in bronze, one partition per ingest day.

    gs://<project>-bronze/orders/ingest_date=YYYY-MM-DD/part-*.json

The rows are produced by BigQuery (sql/generator.sql) and written straight to
GCS with EXPORT DATA - nothing is generated or uploaded from this machine, and
because the generator reads no tables, every export bills 0 bytes.

    python generate_bronze.py --from 2026-08-01 --to 2026-08-30
    python generate_bronze.py --from 2026-08-31 --to 2026-08-31
"""
import argparse
import datetime as dt
import os
import time

from google.cloud import bigquery

HERE = os.path.dirname(os.path.abspath(__file__))

EXPORT = """
EXPORT DATA OPTIONS (
  uri = 'gs://{bucket}/orders/ingest_date={d}/part-*.json',
  format = 'JSON',
  overwrite = true
) AS
SELECT * FROM platform.gen_orders(DATE '{d}')
"""


def days(a, b):
    d = dt.date.fromisoformat(a)
    end = dt.date.fromisoformat(b)
    while d <= end:
        yield d.isoformat()
        d += dt.timedelta(days=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="d_from", required=True)
    ap.add_argument("--to", dest="d_to", required=True)
    ap.add_argument("--project", default="gcp-learning-507108")
    ap.add_argument("--location", default="us-central1")
    a = ap.parse_args()

    client = bigquery.Client(project=a.project, location=a.location)
    bucket = "%s-bronze" % a.project

    # (re)create the generator functions
    client.query(open(os.path.join(HERE, "sql", "generator.sql"), encoding="utf-8").read()).result()

    t0 = time.time()
    jobs = {d: client.query(EXPORT.format(bucket=bucket, d=d)) for d in days(a.d_from, a.d_to)}
    print("submitted %d export jobs in parallel" % len(jobs))

    total_rows = total_files = 0
    for d, job in jobs.items():
        job.result()
        exp = job._properties["statistics"]["query"].get("exportDataStatistics", {})
        rows, files = int(exp.get("rowCount", 0)), int(exp.get("fileCount", 0))
        total_rows += rows
        total_files += files
        print("  ingest_date=%s  rows=%10s  files=%3d  billed=%s B"
              % (d, "{:,}".format(rows), files, job.total_bytes_billed))

    print("done: %s rows, %d files, %.1f s wall clock"
          % ("{:,}".format(total_rows), total_files, time.time() - t0))


if __name__ == "__main__":
    main()
