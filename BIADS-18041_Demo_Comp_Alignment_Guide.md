# BIADS-18041 — Align Demo Comp logic across DROP reports

## Target repo (apply changes here)

**Primary repo:** `fdp-di-adtech-databricks`  
**Path:** `src/prod/data_reporting_and_operational_processing/` (mirror to `src/dev/...`)

This Cursor PR holds the patched files extracted from that repo. Copy/port into the real `fdp-di-adtech-databricks` branch (e.g. under Kranthi’s DROP work).

Do **not** treat `Akshay_Cursor` as the production DROP repo — it is the working sandbox with zips + patched sources.

---

## What was wrong

| Report | Old Demo Comp behavior | Result |
|---|---|---|
| **BAR** | 1 monthly comp × month overall delivery | Correct / agency billable |
| **AOS** (daily Delivery Pull) | Day-varying / blended comps on daily delivery | Under/over vs BAR |
| **Pacing** | 1 overall comp across whole flight | Differs from sum of monthly BAR |
| **Executive Summary** | Used lifetime Demo Comp on period delivery | Could disagree with BAR |

Ticket example (Deal `240150`, SLI `526610`, August):
- AOS: 11,116,031 imps / $297,464.99
- BAR: 11,869,288 imps / $317,622.15  
  (`3P Imps 15,984,796 × Demo Comp 74.2536%`)

---

## Fix (after)

**Rule (Rea / Kranthi agreed):**  
`Billable Demo Imps = (1 monthly Demo Comp) × (overall delivery for that month)`

BAR unchanged. Other reports aligned to that rule.

### Files changed

1. `core.py`
   - `read_fw_demo_by_month()` — one demo numerator/denominator per Placement + Event Month
   - `attach_monthly_demo_to_daily()` — join monthly demo onto daily rows by month
   - `delivery_weighted_monthly_demo_comp()` — roll monthly comps to placement for pacing

2. `ad_operations_daily_delivery_data_load.py` (AOS feed)
   - Uses `read_fw_demo_by_month`
   - Joins demo on Placement + month(`Report End Date`)

3. `ad_operations_lifetime_delivery_data_load.py` (Pacing input)
   - Builds delivery-weighted monthly demo comps from QTD monthly delivery + monthly demo

4. `executive_summary_report.py`
   - Recomputes period Demo Comp from monthly rates before `calculate_imps`

5. `billing_adjustment_report.py`
   - **No change** (already `monthly=True`)

---

## Before vs After (code)

### AOS daily delivery — BEFORE
```python
fw_demo_agg = read_fw_demo(..., report_date)  # blended / not month-keyed
op_fw_adj = fw_adj_agg.join(fw_demo_agg)      # join on Placement ID only
```

### AOS daily delivery — AFTER
```python
fw_demo_by_month = read_fw_demo_by_month(..., report_date)
op_fw_adj = attach_monthly_demo_to_daily(op_fw_adj, fw_demo_by_month, date_col='Report End Date')
# every day in August gets August's single monthly Demo Comp
```

### BAR — BEFORE & AFTER (unchanged)
```python
fw_demo_agg_bill = read_fw_demo(..., monthly=True)
```

---

## How to test

### Unit tests (sandbox)
```bash
cd fdp-di-adtech-databricks-main/src/dev/data_reporting_and_operational_processing
python3 -m unittest test_monthly_demo_comp_alignment.py -v
```

### DROP / Databricks UAT (real validation)

1. Deploy patched files to **dev** in `fdp-di-adtech-databricks`
2. Run DROP daily delivery job for a **closed prior month**
3. Compare for Deal `240150` / SLI `526610` (or a current closed-month line from Rea):

| Check | Expected |
|---|---|
| AOS Finance Daily Delivery CoV / Demo Imps (month sum) | ≈ BAR Billable Impressions |
| AOS Net / billable revenue for that month | ≈ BAR Billable Revenue |
| Demo Comp on all days in that month (NEW/pull) | **Same %** for the month |
| BAR output | **Unchanged** |

4. Run Executive Summary for same report date → Month tab Demo Comp / billable should align with BAR month
5. Run Pacing → earned/demo metrics should move toward sum of monthly BAR (QTD monthly file scope)

### Quick SQL / Excel check
- BAR: Billable Imps ≈ `3P Imps × Demo Comp` for the billing month
- AOS pull: sum of daily demo/CoV imps for that month ≈ BAR Billable Imps  
  (allow tiny int-rounding drift, not ~753K gap)

### Do not
- Run `operative_ingestion_upload` against prod Operative during test
- Change BAR `monthly=True` path

---

## Jira comment (paste)

```
Update — BIADS-18041 Demo Comp alignment

Root cause confirmed:
- BAR uses 1 monthly Demo Comp × month overall delivery (correct / agency billable)
- AOS daily Delivery Pull was applying day-varying or blended comps
- Pacing used one overall flight-level comp
- Executive Summary reused lifetime Demo Comp on period delivery

Fix implemented (BAR left unchanged):
1) core.py — monthly demo helpers (by Placement + Event Month)
2) ad_operations_daily_delivery_data_load.py — AOS daily feed uses one monthly comp per event month
3) ad_operations_lifetime_delivery_data_load.py — pacing uses delivery-weighted monthly comps
4) executive_summary_report.py — period Demo Comp recomputed from monthly rates

Target repo to merge into: fdp-di-adtech-databricks
Path: src/prod (and src/dev) / data_reporting_and_operational_processing/

UAT check: Deal 240150 / SLI 526610 (or newer closed-month sample from RAE)
Expected: AOS month sum CoV/Demo Imps and revenue ≈ BAR billable for that month;
all days in the month share the same Demo Comp %.
```
