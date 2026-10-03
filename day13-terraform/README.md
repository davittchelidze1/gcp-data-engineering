# Day 13 — Terraform

**Hadoop equivalent:** Ambari blueprints or Ansible playbooks for a cluster —
but covering everything around the data, not just the servers: buckets,
datasets, tables, service accounts and IAM, all in code.

## The module
Seven resources for one environment:

| Resource | Why |
|---|---|
| landing bucket | lifecycle rule → Nearline after 30 days |
| warehouse dataset | named per environment (`dev_warehouse`) |
| `orders` table | partitioned by `order_ts`, clustered by `customer_id`, `require_partition_filter` |
| pipeline service account | the identity the pipeline runs as |
| 3 IAM bindings | least privilege: run jobs, read the bucket, write only its own dataset |

State lives in a **GCS bucket** (with versioning), not on my laptop.

## The real test
```bash
terraform destroy -auto-approve    # Resources: 7 destroyed
terraform apply   -auto-approve    # Resources: 7 added
```
Everything came back identical: same partitioning, same clustering, same
partition-filter requirement. If a platform can't be rebuilt from its code, the
code isn't the source of truth.

**Environments are one variable — but not one state.** `terraform plan -var env=prod`
showed *7 to add, 7 to destroy*: with a single shared state, switching the
variable would replace dev with prod. Each environment needs its own state
(and, in real life, its own project).
