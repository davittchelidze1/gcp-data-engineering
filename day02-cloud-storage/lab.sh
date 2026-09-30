#!/usr/bin/env bash
# Day 2: object-store semantics, measured.
set -euo pipefail
PROJECT=gcp-learning-507108
B="gs://$PROJECT-day2"

gcloud storage buckets create "$B" --location=us-central1 \
  --default-storage-class=STANDARD --uniform-bucket-level-access

# ---- a "partitioned directory tree" ----
for p in year=2024/month=01 year=2024/month=02 year=2025/month=01; do
  printf 'id,val\n1,a\n' | gcloud storage cp - "$B/data/$p/part-0.csv"
done
gcloud storage ls "$B/"                              # shows data/ ... looks like a folder
gcloud storage ls -r "$B/**"                         # the truth: three objects, no folders
gcloud storage objects describe "$B/data/" || true   # 404: there is no object "data/"

# ---- rename = copy + delete: the generation number changes ----
head -c 33554432 /dev/urandom > big.bin
gcloud storage cp big.bin "$B/rename-test/big.bin"
gcloud storage objects describe "$B/rename-test/big.bin" --format="value(generation)"
gcloud storage mv "$B/rename-test/big.bin" "$B/rename-test/big-renamed.bin"
gcloud storage objects describe "$B/rename-test/big-renamed.bin" --format="value(generation)"

# ---- simulate a Spark commit: 60 part files into _temporary, then "rename" ----
mkdir -p parts
for i in $(seq -f "%05g" 1 60); do
  printf 'id,val\n%s,row%s\n' "$i" "$i" > "parts/part-$i.csv"
done
gcloud storage cp parts/* "$B/job/_temporary/"
time gcloud storage mv "$B/job/_temporary/*" "$B/job/output/"    # ~0.23 s per object

# ---- versioning: overwrite, then restore the old generation ----
gcloud storage buckets update "$B" --versioning
echo "VERSION 1 - the good data" | gcloud storage cp - "$B/report.txt"
echo "VERSION 2 - oops"          | gcloud storage cp - "$B/report.txt"
gcloud storage ls -a "$B/report.txt"                 # two generations
OLD=$(gcloud storage ls -a "$B/report.txt" | head -1)
gcloud storage cp "$OLD" "$B/report.txt"             # restore
gcloud storage cat "$B/report.txt"

# ---- lifecycle ----
gcloud storage buckets update "$B" --lifecycle-file=lifecycle.json
