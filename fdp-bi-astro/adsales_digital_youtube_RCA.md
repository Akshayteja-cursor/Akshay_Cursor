# BIADS-21813 / BIADS-22012 — Why Prod vs QA YouTube tables still differ

## Short answer

Same source files are **not enough**. QA silver only matches Prod when **(1)** Airflow writes unzipped CSVs to the same S3 prefix the QA dbt stage reads, **(2)** the email-bucket file set is identical (no QA backlog), and **(3)** tables are re-synced from Prod immediately before that shared run.

## What was already fixed

- Astronomer Dev DAG `adsales_digital_youtube` silver task is green again (`digital_youtube_silver_task` → `qa_adsales_silver_youtube`).
- Gold was always able to trigger because it only calls dbt; silver also unzips S3 email attachments first.

## Root causes of row-count drift

### 1. Landing path mismatch (primary)

| Env | Email bucket | Data landing key |
| --- | --- | --- |
| Dev/QA (broken development zip) | `fdp-dmo-emails-test` / `adsales/dev/youtube` | `fox-fdp-bi-dev` / **`adsales/dev/youtube`** |
| Dev/QA (main / stage tables) | same | `fox-fdp-bi-dev` / **`adsales/qa/youtube`** |
| Prod | `fdp-dmo-emails-prod` / `adsales/prod/youtube` | `fox-fdp-bi-prod` / `adsales/prod/youtube` |

JIRA Step 1 (recreate stage tables onto Prod path) was skipped. Stage tables stay on the **QA path**, and Support is supposed to drop files there.

The development branch changed `data_s3_key` from `adsales/qa/youtube` → `adsales/dev/youtube`. Airflow then wrote unzipped files to a prefix dbt was not reading, while `qa_adsales_silver_youtube` still succeeded against whatever remained under `adsales/qa/youtube` → Prod/QA diverge after “same files”.

**Fix in this PR:** restore `data_s3_key` to `adsales/qa/youtube`.

### 2. Partial silver processing on empty prefixes

`process_emails()` used `if response['Contents']:`. When a file-type prefix has no objects, S3 omits `Contents` → `KeyError` → silver aborts mid-loop. Remaining file types never load. QA often has a different subset of the 20 prefixes than Prod.

**Fix in this PR:** use `response.get('Contents')` and log processed vs skipped file types.

### 3. “Same files” ≠ same bucket inventory

Silver walks **all** emails under each prefix and deletes them after load. If `fdp-dmo-emails-test` still held backlog MIME files that Prod’s bucket did not, QA applies extra loads on top of the Prod→QA sync.

### 4. dbt `step_name` is not a model selector

`DBTCloudUtils.run(..., step_name=...)` only sets the dbt Cloud run **cause**. Every file triggers the **full** `qa_adsales_silver_youtube` job against current unzipped folders. Wrong/stale unzipped content is therefore reloaded repeatedly (matches the 65 successful dbt runs in UAT).

## Correct re-validation procedure

1. Confirm QA external stage / dbt source points at `s3://fox-fdp-bi-dev/adsales/qa/youtube/`.
2. Deploy the path + Contents fixes to Astronomer Dev.
3. Re-clone Prod → QA for the four silver tables (and gold if comparing gold):
   - `fox_bi_qa.silver_ad_sales.ft_youtube_views_daily`
   - `fox_bi_qa.silver_ad_sales.ft_youtube_views_monthly`
   - `fox_bi_qa.silver_ad_sales.ft_youtube_revenue_daily`
   - `fox_bi_qa.silver_ad_sales.ft_youtube_revenue_monthly`
4. Empty leftover MIME objects under `s3://fdp-dmo-emails-test/adsales/dev/youtube/` (or ensure only the same day’s files exist as Prod).
5. Drop the identical attachments into Prod + QA email prefixes.
6. Run both DAGs; compare row counts / metrics by date.

```sql
-- Example: views daily
SELECT 'prod' AS env, COUNT(*) AS cnt, MIN(date) AS min_dt, MAX(date) AS max_dt
FROM fox_bi_prod.silver_ad_sales.ft_youtube_views_daily
UNION ALL
SELECT 'qa', COUNT(*), MIN(date), MAX(date)
FROM fox_bi_qa.silver_ad_sales.ft_youtube_views_daily;
```
