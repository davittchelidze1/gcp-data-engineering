#!/usr/bin/env bash
# Day 8: ordering, dead-lettering, BigQuery subscriptions, replay.
set -euo pipefail
PROJECT=gcp-learning-507108
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT" --format="value(projectNumber)")
AGENT="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

# ---- basics: publish 3, pull back (arrived 2, 1, 3) ----
gcloud pubsub topics create day8-orders
gcloud pubsub subscriptions create day8-sub --topic=day8-orders --ack-deadline=10
for i in 1 2 3; do
  gcloud pubsub topics publish day8-orders --message="{\"order_id\":$i}" --attribute="seq=$i"
done
gcloud pubsub subscriptions pull day8-sub --auto-ack --limit=5 \
  --format="table(message.data.decode(base64),message.attributes.seq)"

# ---- ordering keys ----
gcloud pubsub subscriptions create day8-sub-ordered --topic=day8-orders --enable-message-ordering
for i in 1 2 3 4 5 6; do
  gcloud pubsub topics publish day8-orders --message="event-$i" --ordering-key=customer-A
done

# ---- dead-letter topic ----
gcloud pubsub topics create day8-dlq
gcloud pubsub subscriptions create day8-dlq-sub --topic=day8-dlq
gcloud pubsub subscriptions create day8-sub-poison --topic=day8-orders --ack-deadline=10 \
  --dead-letter-topic=day8-dlq --max-delivery-attempts=5
gcloud pubsub topics add-iam-policy-binding day8-dlq --member="$AGENT" --role=roles/pubsub.publisher
gcloud pubsub subscriptions add-iam-policy-binding day8-sub-poison --member="$AGENT" --role=roles/pubsub.subscriber

gcloud pubsub topics publish day8-orders --message="POISON-cannot-parse-this"
for attempt in 1 2 3 4 5 6; do
  gcloud pubsub subscriptions pull day8-sub-poison --limit=5 \
    --format="value(message.data.decode(base64))"            # pulled but never acked
  sleep 12                                                    # > ack deadline: redelivered
done
gcloud pubsub subscriptions pull day8-dlq-sub --auto-ack --format="value(message.data.decode(base64))"

# ---- BigQuery subscription: no consumer code at all ----
bq query --use_legacy_sql=false "CREATE OR REPLACE TABLE day6.pubsub_landing (
  subscription_name STRING, message_id STRING, publish_time TIMESTAMP,
  data STRING, attributes STRING)"
gcloud projects add-iam-policy-binding "$PROJECT" --member="$AGENT" --role=roles/bigquery.dataEditor --condition=None
gcloud pubsub subscriptions create day8-bq-sub --topic=day8-orders \
  --bigquery-table="$PROJECT:day6.pubsub_landing" --write-metadata
for i in 1 2 3 4; do gcloud pubsub topics publish day8-orders --message="{\"evt\":$i}" --attribute=kind=bqtest; done
sleep 20
bq query --use_legacy_sql=false "SELECT data, attributes, publish_time FROM day6.pubsub_landing ORDER BY publish_time"

# ---- replay: seek back to a timestamp ----
gcloud pubsub subscriptions update day8-sub --retain-acked-messages --message-retention-duration=1d
MARK=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 3
for i in 1 2 3; do gcloud pubsub topics publish day8-orders --message="replay-$i"; done
gcloud pubsub subscriptions pull day8-sub --auto-ack --limit=10     # consumed and acked
gcloud pubsub subscriptions seek day8-sub --time="$MARK"
gcloud pubsub subscriptions pull day8-sub --auto-ack --limit=10     # all three again
