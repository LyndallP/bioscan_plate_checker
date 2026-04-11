# bioscan_plate_checker

Track BIOSCAN plates through every stage of the sequencing and QC pipeline to identify plates that have been lost or stalled at any point.

## What it does

For every BIOSCAN plate that has ever been processed, checks:

| Stage | Source | What we check |
|---|---|---|
| **Submitted** | ToL Portal | Plate registered, partner code, BOLD upload status |
| **mBRAVE** | `mbrave_batch_data/` | Plate present in consensusseq_network files |
| **QC** | `qc_reports_rerun_Feb2026/` | Plate present in qc_portal files, best QC result |
| **BOLD** | ToL Portal `bold_nuc` field | Any well has sequence uploaded to BOLD |

Output is a master CSV/Excel with one row per plate, plus a text report of plates that dropped out at each stage.

## Installation

Run on the farm cluster where the lustre paths are accessible and the `tol` library is available.

```bash
git clone https://github.com/LyndallP/bioscan_plate_checker.git
cd bioscan_plate_checker
# no additional dependencies beyond pandas (already available in tol environment)
```

## Usage

```bash
# Check all BIOSCAN plates
python plate_status_report.py --partner ALL

# Check one partner
python plate_status_report.py --partner BGEP

# Skip portal query (faster, offline mode — no BOLD or partner info)
python plate_status_report.py --partner ALL --skip-portal

# Show only plates that dropped out somewhere
python plate_status_report.py --partner ALL --missing-only

# Repeat analysis
python repeat_analysis.py --partner ALL
python repeat_analysis.py --partner BGEP
```

## Batch deduplication rules

When both a base batch folder and split/merged variants exist, the following rules apply:

1. `batch50_merged` exists → **use merged**, ignore `batch50` and `batch50_0/1/2/3`
2. `batch27_0`, `batch27_1` etc exist alongside `batch27` → **use splits**, ignore plain `batch27` (plain = likely incomplete download)
3. Only `batch30` exists → **use it**

These rules apply independently to `mbrave_batch_data/` and `qc_reports_rerun_Feb2026/`.

## MOZZ plates

Early BIOSCAN batches used `MOZZ-XXX` plate codes shared across multiple partners. Partner disambiguation for MOZZ plates is done via the `sts_gal_abbreviation` field from the ToL Portal. The `--skip-portal` flag will leave partner as `None` for these plates.

## Output files

Written to `results/`:

- `bioscan_plate_status_ALL_YYYYMMDD.csv` — full master table
- `bioscan_plate_status_ALL_YYYYMMDD.xlsx` — same, Excel format
- `missing_plates_ALL_YYYYMMDD.txt` — text report of plates dropped at each stage

## Master table columns

| Column | Description |
|---|---|
| `plate_id` | Plate identifier |
| `partner` | 4-letter partner code (from portal) |
| `mbrave_status` | FOUND / MISSING |
| `mbrave_batches` | Comma-separated batch list |
| `n_sequencings` | Number of times plate was sequenced (repeats > 1) |
| `qc_status` | FOUND / MISSING |
| `qc_batches` | Comma-separated QC batch list |
| `best_qc_result` | PASS / ON_HOLD / FAIL / UNKNOWN / MISSING |
| `portal_status` | FOUND / MISSING / SKIPPED |
| `bold_status` | HAS_DATA / NO_DATA / UNKNOWN |
| `pipeline_stage` | Furthest stage reached |
| `missing_at` | Stage where plate dropped out (blank if complete) |

## Scripts

| Script | Purpose |
|---|---|
| `plate_status_report.py` | Main entry point — builds master table |
| `mbrave_checker.py` | Scans mBRAVE data for plate presence |
| `qc_checker.py` | Scans QC reports for plate presence and results |
| `portal_query.py` | Queries ToL Portal for submission and BOLD status |
| `repeat_analysis.py` | Analysis of plates sequenced more than once |
| `config.py` | All paths and constants |
| `utils.py` | Batch dedup, plate ID parsing, safe CSV reading |
