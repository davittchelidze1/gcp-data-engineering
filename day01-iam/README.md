# Day 1 — IAM and service accounts

**Hadoop equivalent:** Ranger/Sentry policies plus Kerberos service principals.
The difference: there is no central policy server. Every resource carries its
own policy, permissions inherit down the hierarchy (org → folder → project →
resource), and they only ever add — a lower level cannot take away what a higher
level granted.

## Lab
One service account allowed to read one dataset and one bucket, and nothing
else. Then I impersonated it and tried both doors.

| Test, running as the service account | Result |
|---|---|
| query `sales_data.orders` | ✅ rows returned |
| query `hr_data.salaries` | ❌ Access Denied |
| read an object in the granted bucket | ✅ |
| list every bucket in the project | ❌ 403 |

## What I took from it
- **Deny by default.** Nothing says "deny hr_data" — it was simply never granted.
- **A query needs two grants.** `roles/bigquery.jobUser` on the project (may run
  jobs — the project running the query pays) and `dataViewer` on the dataset
  (may see the data). Missing either one fails.
- **A service account is both an identity and a resource.** *What can it do?*
  lives in the project and dataset policies. *Who can become it?* lives in the
  policy on the service account itself.
- **No key files.** Impersonation issues a token that lives in memory for about
  an hour; `keys list --managed-by=user` stays empty.
- **BigQuery datasets still carry a legacy access list** next to IAM, so the
  dataset grant is an edit of that list (`bq show` → edit → `bq update`).

Commands: [lab.sh](lab.sh)
