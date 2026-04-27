# bioscan_plate_checker

A pipeline tracking and quality assessment toolkit for BIOSCAN DNA barcoding data. Traces every plate from submission through sequencing, QC, and BOLD upload to identify plates lost at any stage, assess repeat sequencing outcomes, verify BOLD upload integrity, and flag sequences with quality issues.

---

## Output file reference

All outputs are written to the results directory:
`/lustre/scratch126/tol/teams/lawniczak/users/lp20/bioscan_plate_checker_results/`

Files are dated `YYYYMMDD`. Below is a complete reference of every file produced.

---

### From `plate_status_report.py` — master pipeline tracker

| File | Type | Level | Description |
|---|---|---|---|
| `bioscan_plate_status_ALL_YYYYMMDD.csv` | CSV | Plate | **Master table.** One row per plate. Tracks portal submission, mBRAVE sequencing, QC result, and BOLD upload status. The primary file for understanding where every plate is in the pipeline. |
| `bioscan_plate_status_ALL_YYYYMMDD.xlsx` | Excel | Plate | Same as above, Excel format for sharing with colleagues. |
| `missing_plates_ALL_YYYYMMDD.txt` | Text | Plate | Summary of plates that dropped out at each stage (not sequenced / not QC'd / not on BOLD), with submission dates. The key actionable output for chasing missing plates. |

---

### From `generate_pipeline_report.py` — human-readable summary

| File | Type | Level | Description |
|---|---|---|---|
| `pipeline_report_YYYYMMDD.txt` | Text | Summary | Human-readable report of the full pipeline status. Shows overall pass-through rates at each stage, per-partner breakdown, plates flagged as old submissions not yet sequenced (configurable threshold, default 180 days), and BOLD upload gaps by partner. The best file to share with project managers or partners. |

---

### From `bold_summary_from_portal.py` — BOLD upload status

| File | Type | Level | Description |
|---|---|---|---|
| `bold_summary_report_YYYYMMDD.txt` | Text | Summary | Report of BOLD upload status. Shows how many specimens are uploaded per partner, which have no BIN URI (needing BOLD follow-up), and the distribution of upload dates for un-BINned specimens. |
| `bold_missing_bin_YYYYMMDD.csv` | CSV | Specimen | Detail file. Every specimen that has a sequence on BOLD but no BIN URI assigned. Includes upload date and submission date. Use this to identify which specimens need chasing with the BOLD team. |
| `bold_plate_summary_YYYYMMDD.csv` | CSV | Plate | Plate-level summary of BOLD upload counts, BIN coverage, and earliest/latest upload dates. |

---

### From `repeat_analysis.py` — repeat sequencing

| File | Type | Level | Description |
|---|---|---|---|
| `repeat_analysis_YYYYMMDD.csv` | CSV | Plate | One row per plate sequenced more than once. Shows all batches, pass rate in first vs best vs last sequencing, improvement achieved, and whether the plate has been uploaded to BOLD. The key file for understanding whether repeat sequencing was worthwhile. |
| `repeat_analysis_YYYYMMDD.xlsx` | Excel | Plate | Same as above, Excel format. |

---

### From `missing_specimen_analysis.py` — consensus assembly failures

| File | Type | Level | Description |
|---|---|---|---|
| `missing_specimens_categorised_YYYYMMDD.csv` | CSV | Specimen | One row per specimen that is absent from the consensusseq_network table. Categorised as: Cat1 (zero reads — well failed), Cat2_low (few reads, no consensus — below assembly threshold), or Cat2_high (reads present but no consensus — unexpected, investigate). |
| `missing_specimens_batch_summary_YYYYMMDD.csv` | CSV | Batch | Batch-level counts of expected vs present specimens, with counts in each category and percentage missing. Use this to identify batches with systematic assembly failures. |

---

### From `bold_workbench_analysis.py` — BOLD quality flags and sequence concordance

| File | Type | Level | Description |
|---|---|---|---|
| `bold_workbench_combined.csv` | CSV | Specimen | **Cache file** — combined workbench records from all annual files. Auto-generated on first run. Do not edit manually; delete and rerun with `--rebuild-cache` if source files change. |
| `bold_workbench_report_YYYYMMDD.txt` | Text | Summary | Report of quality flags (stop codon, contamination, flagged record, BIN compliance) by partner. Includes sequence comparison results for flagged specimens. |
| `bold_workbench_plates_YYYYMMDD.csv` | CSV | Plate | Plate-level counts of quality flags. Shows how many specimens per plate are flagged for each issue. |
| `bold_flagged_comparison_YYYYMMDD.csv` | CSV | Specimen | **Routine output.** For every flagged specimen (stop codon / contamination / flagged record), shows whether the BOLD sequence is IDENTICAL or DIFFERENT to the QC-passed FASTA sequence. DIFFERENT = QC has found a better sequence and BOLD should be updated. |
| `bold_full_concordance_YYYYMMDD.csv` | CSV | Specimen | **Ad hoc output** (`--full-concordance` only). Full sense check — every sequence on BOLD compared against the QC FASTA. Confirms 100% concordance or flags any drift. |

---

### From `bold_check.R` — quarterly BOLD sanity check (job output)

| File | Type | Level | Description |
|---|---|---|---|
| `bold_check_specimens_YYYYMMDD.csv` | CSV | Specimen | Full specimen-level comparison of portal vs BOLD records. 198MB. Use as raw cache — do not delete between runs. |
| `bold_check_plates_YYYYMMDD.csv` | CSV | Plate | Plate-level BOLD coverage summary including BIN counts. |
| `bold_check_JOBID.log` | Text | Log | Job log showing download progress and final summary. |
| `bold_check_JOBID.err` | Text | Log | Job error log (should be empty if successful). |

---

### Internal/cache files (not for routine use)

| File | Description |
|---|---|
| `portal_plates_from_dump.csv` | Cached plate-level portal summary built by `read_portal_dump.py`. Rebuilt when portal dump is updated. |
| `bold_workbench_combined.csv` | Cached combined workbench file. Rebuilt with `--rebuild-cache`. |

---

## Setup

### 1. Clone the repository

```bash
git clone git@github.com:LyndallP/bioscan_plate_checker.git
cd bioscan_plate_checker
```

### 2. Activate the conda environment

```bash
conda activate bioscan-ops
```

### 3. Build the portal plate summary (run once, or when portal dump is updated)

```bash
python3 read_portal_dump.py
```

### 4. API key for BOLD (only needed for the quarterly BOLD sanity check)

```bash
cp .env.example .env
# Edit .env and add your BOLD API key
```

---

## Routine run order

```bash
# 1. Build portal plate summary from dump (~30 seconds)
python3 read_portal_dump.py

# 2. Build master plate status table: portal → mBRAVE → QC → BOLD
python3 plate_status_report.py --partner ALL

# 3. Human-readable pipeline report with flagged plates
python3 generate_pipeline_report.py

# 4. BOLD upload and BIN URI summary (from portal dump, no API needed)
python3 bold_summary_from_portal.py --partner ALL

# 5. Repeat sequencing analysis
python3 repeat_analysis.py --partner ALL

# 6. Missing specimen analysis (scans all mBRAVE batches — takes ~10 mins)
python3 missing_specimen_analysis.py --partner ALL
```

### When new BOLD workbench exports are available

Place files named `bold_workbench_YYYY.csv` (or `bold_workbench_YYYYa.csv`, `bold_workbench_YYYYb.csv` for split downloads) in the results directory, then:

```bash
# Routine: quality flag report + flagged sequence comparison vs QC
python3 bold_workbench_analysis.py --partner ALL

# Ad hoc: full concordance of ALL sequences on BOLD vs QC FASTA
python3 bold_workbench_analysis.py --partner ALL --full-concordance

# If new year files added, rebuild the combined cache
python3 bold_workbench_analysis.py --rebuild-cache
```

### Quarterly BOLD sanity check

```bash
bsub < run_bold_check.sh
```

---

## Key data paths

| Path | Contents |
|---|---|
| `/lustre/.../mbrave_batch_data/` | mBRAVE output by batch |
| `/lustre/.../qc_reports_rerun_Feb2026/` | QC reports by batch |
| `/lustre/.../100k_paper/output/sts_manifests_20260408.tsv` | Portal dump — all specimens with BOLD fields |
| `/lustre/.../bioscan_plate_checker_results/` | All outputs and workbench input files |

---

## Script reference

---

### `config.py`
Central configuration. Edit this if lustre paths or the portal dump filename change.

---

### `utils.py`
Shared utilities: batch folder resolution, plate ID parsing, cross-directory batch mapping, safe CSV reading.

**Batch deduplication rules (applied independently per directory):**
1. Splits (`batchN_0`, `batchN_1`...) always preferred over plain (`batchN`) or merged (`batchN_merged`)
2. Plain used only if no splits exist
3. Merged used only as last resort

```bash
python3 utils.py   # run the batch structure audit
```

---

### `read_portal_dump.py`
Reads the pre-exported portal manifest TSV and builds a plate-level summary CSV.
Avoids live portal queries which take ~2 hours for 350k+ specimens.

```bash
python3 read_portal_dump.py
python3 read_portal_dump.py --input /path/to/sts_manifests_20260408.tsv
```

---

### `plate_status_report.py`
**Main entry point.** Joins portal, mBRAVE, and QC data into a master plate status table.
Controls (`CONTROL_NEG_*`, `CONTROL_POS_*`) are excluded throughout.

```bash
python3 plate_status_report.py --partner ALL
python3 plate_status_report.py --partner BGEP
python3 plate_status_report.py --partner ALL --skip-portal
python3 plate_status_report.py --partner ALL --missing-only
```

---

### `generate_pipeline_report.py`
Human-readable pipeline status report. Flags plates submitted more than N days ago that have not yet been sequenced.

```bash
python3 generate_pipeline_report.py
python3 generate_pipeline_report.py --old-threshold-days 180
```

---

### `bold_summary_from_portal.py`
BOLD upload and BIN URI summaries from the portal dump. No API, no R required.

```bash
python3 bold_summary_from_portal.py --partner ALL
python3 bold_summary_from_portal.py --partner FACE
```

---

### `bold_workbench_analysis.py`
Analyses BOLD workbench exports to assess quality flags and compare flagged sequences against QC output.

**Background:** BOLD flags sequences for Stop Codon (internal stop codons suggesting pseudogenes or NUMTs), Contamination (foreign DNA), and Flagged Record (general quality concern). Flagged sequences do not receive BIN assignments. The BIOSCAN QC pipeline includes a stop codon detection step that searches for alternative sequences — this script checks whether a cleaner sequence now exists in QC that should replace the flagged BOLD record.

**Input files** (place in results directory, named by year):
```
bold_workbench_2021.csv
bold_workbench_2022.csv
bold_workbench_2023.csv
bold_workbench_2024a.csv   ← split downloads use a/b suffix
bold_workbench_2024b.csv
bold_workbench_2025a.csv
bold_workbench_2025b.csv
bold_workbench_2026.csv
```
Each file: downloaded from BOLD workbench filtered by year (max 99,999 records per download), skip 2 header rows, columns include Sample ID, BIN, Stop Codon, Contamination, Flagged Record, Barcode Compliant.

**Sequence comparison results:**
- `IDENTICAL` — same sequence in BOLD and QC FASTA → flag is genuine, no update needed
- `DIFFERENT` → QC has a better sequence → **BOLD record should be updated**
- `QC_ONLY` — passed QC but not on BOLD
- `BOLD_ONLY` — on BOLD but not in QC FASTA

```bash
python3 bold_workbench_analysis.py --partner ALL          # routine
python3 bold_workbench_analysis.py --full-concordance     # ad hoc sense check
python3 bold_workbench_analysis.py --rebuild-cache        # after adding new year files
python3 bold_workbench_analysis.py --skip-sequence-comparison  # flags only
```

---

### `repeat_analysis.py`
Plates sequenced more than once — pass rate comparison between batches.

```bash
python3 repeat_analysis.py --partner ALL
python3 repeat_analysis.py --min-sequencings 2
```

---

### `missing_specimen_analysis.py`
Categorises specimens absent from consensusseq by read count category.

| Category | Definition | Interpretation |
|---|---|---|
| **Cat1** | Count = 0 | Well failed to sequence. Expected. |
| **Cat2_low** | 0 < Count < threshold | Below assembly minimum. |
| **Cat2_high** | Count ≥ threshold, no consensus | Assembly failed despite reads. Investigate. |
| **Cat3** | Absent from UMI stats | Barcode assignment error. Zero cases detected. |

```bash
python3 missing_specimen_analysis.py --partner ALL
python3 missing_specimen_analysis.py --batch batch41_1
python3 missing_specimen_analysis.py --low-read-threshold 50
```

---

### `bold_check.R` + `run_bold_check.sh`
Quarterly BOLDconnectR sanity check. Confirms portal and BOLD are in sync.
Run time ~40 minutes. Requires R module and API key.

```bash
bsub < run_bold_check.sh
```

---

## BOLD API key security

- Store in `~/bioscan_plate_checker/.env` (in `.gitignore`, never committed)
- Load with `export $(cat .env | xargs)` before running R scripts
- See `.env.example` for required format

---

## Notes on plate ID formats

| Format | Example | Notes |
|---|---|---|
| `XXXX_NNN` | `HIRW_001` | Standard 4-letter partner code |
| `XXXX-NNN` | `BGEP-161` | Dash-separated |
| `TOL-XXXX-NNN` | `TOL-BGEP-001` | TOL-prefixed BGE plates |
| `MOZZ00000609A` | `MOZZ00000609A` | Early MOZZ format, shared across partners |
| `BSN_NNN` | `BSN_052` | Early R&D plates, not formally submitted |

---

## Known findings (April 2026)

- **324 plates** submitted but not yet in mBRAVE — 42 from 2023 (FACE, FRBX) warrant investigation
- **702 plates** through QC but not yet on BOLD
- **6,261 specimens** on BOLD with no BIN — stop codon and contamination flags confirmed as root cause; FACE (803), RRNW (386), CAMP (332) most affected
- **57 specimens** absent from BOLD workbench entirely — all BGE partners (BGKU, BGPT, BGEG); edge well positions suggest quality-based removal by BOLD team
- **Portal/BOLD concordance: 100%** — all 272,005 specimens confirmed on BOLD
- **248 plates** sequenced more than once; average improvement 6.1%; TOL-BGEP-010 most improved (6.5% → 98.9%)
- **6,208 specimens** with reads but no consensus — concentrated in BGKU and WALW (aquatic invertebrates; taxon-specific assembly failure)
- **Cat3 = 0** — no demultiplexing failures detected across any batch
