# Investigation Proof — BAR Net Invoice Amount vs AOS Finance

**Issue:** [Drop] BAR Net Invoice Amount not matching AOS Finance Module  
**Evidence file:** `June '26_BAR Net Check.xlsx`  
**Code:** `fdp-di-adtech-databricks-*/src/*/data_reporting_and_operational_processing/core.py` → `calculate_billable()`  
**PR:** https://github.com/Akshayteja-cursor/Akshay_Cursor/pull/2

---

## 1. Reported discrepancy (ticket example)

| Field | Value |
|---|---|
| SO ID | 258950 |
| Parent Line ID | 581030 |
| BAR Net Invoice Amount | $91,836.72 |
| AOS Finance Export | $183,673.4694 |
| Ratio | **0.500000** (exactly half) |

---

## 2. Population check (June BAR Net Check — Comparison sheet)

| Category | Count | Meaning |
|---|---|---|
| BAR ≈ 50% of AOS | **140** | Systematic under-reporting |
| BAR ≈ AOS (match) | 38 | Already aligned |
| Other / partial | 23 | Mixed / incomplete flight coverage |
| Both zero | 9 | N/A |

This is not a one-off data issue; ~70% of nonzero SO comparisons are halved.

---

## 3. Parent-level proof for SO 258950

Every parent under this SO shows the same ~0.5 ratio, and BAR child Net Cost sums equal AOS parent Net Cost:

| Parent Line | BAR Net Invoice | AOS Net Invoice | Ratio | BAR children | BAR Net Cost | AOS Net Cost |
|---|---:|---:|---:|---:|---:|---:|
| 581023 | 6,860.83 | 13,619.5250 | 0.503750 | 1 | 25,000.00 | 25,000.00 |
| 581025 | 47,687.09 | 95,374.1700 | 0.500000 | 1 | 275,000.00 | 275,000.00 |
| 581027 | 91,251.37 | 182,502.7500 | 0.500000 | 2 | 275,000.01 | 275,000.01 |
| **581030** | **91,836.72** | **183,673.4694** | **0.500000** | **3** | **300,000.00** | **300,000.00** |
| 582669 | 37,497.34 | 74,994.6748 | 0.500000 | 2 | 114,033.02 | 114,033.02 |

### Parent 581030 child detail (Cash BAR)

| Child SLI | Net Invoice (BAR) | Net Cost | Product |
|---|---:|---:|---|
| 581031 | 30,612.24 | 100,000 | FB/Instagram |
| 581033 | 30,612.24 | 100,000 | YouTube |
| 581034 | 30,612.24 | 100,000 | TikTok |
| **Sum** | **91,836.72** | **300,000** | |

Math check:
- AOS parent invoice / 2 = `183,673.4694 / 2 = 91,836.7347` ≈ BAR parent sum
- Per child with package doubling: `183,673.4694 × (100,000 / 600,000) = 30,612.2449` ≈ BAR child amount

---

## 4. Root cause

BAR allocates AOS finance **Net Invoice Amount** (joined on Parent Sales Line Item ID) to children using:

```text
Scaled Active Net Invoice Amount = Net Invoice Amount × (Net Cost / Total Active Net Cost)
```

`Total Active Net Cost` was computed over **all** rows under the parent, including `Operative Product Type = Package`.

For packaged orders:
- Package row Net Cost = sum of child Net Costs (e.g. 300,000)
- Children Net Cost sum = 300,000
- Denominator becomes **600,000** (2×)
- Children receive only **50%** of AOS invoice
- Package row is later removed in `format_billing` (`~Operative Product Type.isin({'Package'})`), so the other 50% never appears on BAR

Code location: `calculate_billable()` in `core.py` (called from Billing Adjustment Report / BAR).

---

## 5. Fix

Exclude Package rows from:
1. Total Active / Total Net Cost aggregations
2. Rounding reconciliation groupbys

After fix, children ratios sum to 1.0 and BAR Net Invoice aligns to AOS.

---

## 6. Regression test proof

Scenario: SO 258950 / Parent 581030 (package + 3 children)

```text
test_buggy_behavior_is_half_of_aos ... ok          # pre-fix = 50% of AOS, ~30612.24/child
test_fix_aligns_children_sum_to_aos_net_invoice ... ok   # post-fix sum = AOS
test_single_child_package_no_longer_halved ... ok  # parent 581025 pattern

Ran 3 tests — OK
```

Command:
```bash
python3 -m unittest test_bar_net_invoice_scaling.py -v
```

---

## 7. Conclusion

| Question | Answer |
|---|---|
| Are BAR numbers pulled from AOS finance? | Yes — finance Net Invoice is joined by parent line, then scaled to children by Net Cost |
| Why different from AOS export? | Package rows inflated the scaling denominator; BAR showed ~half |
| Is ticket example explained? | Yes — exact 50% with reproducible child-level math |
| Fix verified? | Yes — unit tests + parent-level reconciliation logic |
