"""Run a .sql file (single statement or multi-statement script) on BigQuery
and report what it actually cost: bytes billed and slot time, per statement.

    python tools/run_sql.py sql/silver_to_gold.sql --param ingest_from=2026-08-31 --param ingest_to=2026-08-31
"""
import argparse
import json
import time

from google.cloud import bigquery


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sql_file")
    ap.add_argument("--param", action="append", default=[],
                    help="named DATE parameter, e.g. ingest_from=2026-08-31")
    ap.add_argument("--project", default="gcp-learning-507108")
    ap.add_argument("--location", default="us-central1")
    ap.add_argument("--max-rows", type=int, default=40)
    ap.add_argument("--label", default=None, help="job label value for chargeback")
    a = ap.parse_args()

    sql = open(a.sql_file, encoding="utf-8").read()
    params = [bigquery.ScalarQueryParameter(k, "DATE", v)
              for k, v in (p.split("=", 1) for p in a.param)]
    cfg = bigquery.QueryJobConfig(query_parameters=params)
    if a.label:
        cfg.labels = {"pipeline": a.label}

    client = bigquery.Client(project=a.project, location=a.location)
    t0 = time.time()
    job = client.query(sql, job_config=cfg)
    rows = list(job.result())
    wall = time.time() - t0

    if rows:
        cols = list(rows[0].keys())
        print("\t".join(cols))
        for r in rows[: a.max_rows]:
            print("\t".join("" if r[c] is None else str(r[c]) for c in cols))
        if len(rows) > a.max_rows:
            print("... %d more rows" % (len(rows) - a.max_rows))

    stmts = []
    if job.statement_type == "SCRIPT":
        for child in sorted(client.list_jobs(parent_job=job.job_id), key=lambda j: j.created):
            stmts.append({
                "statement": child.statement_type,
                "mb_billed": round((child.total_bytes_billed or 0) / 2**20, 1),
                "slot_s": round((child.slot_millis or 0) / 1000, 1),
                "dml_rows": getattr(child, "num_dml_affected_rows", None),
            })

    stats = {
        "job_id": job.job_id,
        "wall_s": round(wall, 1),
        "mb_processed": round((job.total_bytes_processed or 0) / 2**20, 1),
        "mb_billed": round((job.total_bytes_billed or 0) / 2**20, 1),
        "slot_s": round((job.slot_millis or 0) / 1000, 1),
        "statements": stmts,
    }
    print("STATS " + json.dumps(stats))


if __name__ == "__main__":
    main()
