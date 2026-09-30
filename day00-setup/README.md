# Day 0 — Setup and cost guardrails

**Hadoop equivalent:** provisioning a cluster — except nothing runs until you
ask for it, and anything you forget to stop keeps billing.

## What I set up
- A project on the free trial ($300 credits / 90 days), region `us-central1`
- Budget alerts at $1 / $5 / $10. Alerts only: a budget does not stop spending
- Billing export to BigQuery on day one — it is not retroactive, so every later
  day can be costed from it
- `gcloud`, `bq` and Terraform, with two logins: one for me (`gcloud auth login`)
  and one for code — Terraform and Python clients (`application-default login`)

## Rules I kept for all 14 days
- Never upgrade the trial account: in trial mode, running out of credits pauses
  resources instead of charging the card
- Never create a Cloud Composer environment (~$300/month, bills while idle)
- Never leave a streaming Dataflow job running
- Always give a Dataproc cluster an auto-delete TTL

## The one habit
`bq query --dry_run` reports exactly what a query would scan, for free. The first
one — a full aggregation over the Shakespeare sample — would scan 2.65 MB.

The full study plan is in [plan.md](plan.md); `setup.sh` has the commands.
