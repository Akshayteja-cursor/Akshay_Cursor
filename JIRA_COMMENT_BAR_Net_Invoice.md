## Investigation Update — BAR Net Invoice Amount vs AOS Finance

Investigated using the attached **June '26_BAR Net Check.xlsx** and BAR pipeline code (`calculate_billable` in DROP billing adjustment report / `core.py`).

### Ticket example confirmed
- **SO:** 258950  
- **Parent Line:** 581030  
- **BAR:** $91,836.72  
- **AOS Finance:** $183,673.4694  
- **Ratio:** exactly **0.50**

Parent 581030 has 3 BAR children (FB/IG, YouTube, TikTok), each showing **$30,612.24**.  
`3 × 30,612.24 = 91,836.72`, and `183,673.47 / 2 = 91,836.73`.

### Broader impact (June file)
From the Comparison sheet:
- **140** SOs where BAR ≈ **50%** of AOS  
- **38** SOs already matching  
- Remaining mixed/partial

So this is a systematic issue, not a single-order data problem.

### Root cause
BAR does pull Net Invoice from AOS Finance (joined on Parent Line Item ID), then allocates it to child lines by Net Cost ratio.

**Bug:** Package parent rows were included in the Net Cost denominator used for that allocation. Package Net Cost equals the sum of child Net Costs, so the denominator was doubled. Children only received ~50% of the AOS amount. Package rows are dropped later from BAR output, so the missing half never appears on the report.

Formula (broken):
`child invoice = AOS Net Invoice × (child Net Cost / (package Net Cost + children Net Cost))`  
→ with package Net Cost = children Net Cost, this becomes **50%**.

### Fix
Exclude `Operative Product Type = Package` from Net Cost ratio / invoice scaling in `calculate_billable` (same filter already used when formatting BAR output).

### Validation
Regression tests reproduce SO 258950 / 581030:
- Pre-fix: BAR child sum = 50% of AOS  
- Post-fix: BAR child sum aligns to AOS Net Invoice  

PR: https://github.com/Akshayteja-cursor/Akshay_Cursor/pull/2

**AC status:** Fix aligns BAR Net Invoice to AOS Finance for packaged lines; ready for DROP BAR rerun / UAT on June period (SO 258950 / parent 581030 as primary check).
