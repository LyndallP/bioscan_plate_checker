# bioscan_plate_checker

A pipeline tracking and quality assessment toolkit for BIOSCAN DNA barcoding data. Traces every plate from submission through sequencing, QC, and BOLD upload to identify plates lost at any stage, assess repeat sequencing outcomes, verify BOLD upload integrity, and flag sequences with quality issues.

---

## Input files

All input data is read directly from lustre — nothing is copied or modified.

### Portal manifest dump
**Path:** `/lustre/scratch126/tol/teams/lawniczak/projects/bioscan/100k_paper/output/sts_manifests_YYYYMMDD.tsv`
**Source:** Exported from the Tree of Life (ToL) portal by the data team. Updated periodically — check for a newer dated file before running.
**Contains:** One row per specimen (well) for all BIOSCAN samples ever submitted. Includes plate ID (`sts_specimen.id`), submission date (`sts_submit_date`), partner code (`sts_gal_abbreviation`), and all BOLD fields: `bold_nuc` (sequence), `bold_bin_uri` (BIN assignment), `bold_sequence_upload_date`, `bold_bin_created_date`.
**Why we use it:** This is the ground truth for what has been submitted to the project. It also contains the BOLD upload status and sequence for each specimen, allowing BOLD summaries without live API queries. Querying the portal live takes ~2 hours for 350k+ specimens; the dump is read in ~30 seconds.

### mBRAVE batch data
**Path:** `/lustre/scratch126/tol/teams/lawniczak/projects/bioscan/bioscan_qc/mbrave_batch_data/`
**Source:** Output from the mBRAVE pipeline run at the Wellcome Sanger Institute. Organised by batch folder (e.g. `batch30/`, `batch51_0/`).
**Contains:** Per batch: `*consensusseq_network.tsv` (one row per specimen per sequence produced, with taxonomy), `umi.*_sample_stats.txt` (one row per specimen showing read counts from demultiplexing).
**Why we use it:** Tells us which plates and specimens made it through sequencing and produced a consensus sequence. The consensusseq file is upstream of QC — it contains all sequences before any quality filtering. The UMI stats file tells us the expected specimen list and read counts for each run.

### QC reports
**Path:** `/lustre/scratch126/tol/teams/lawniczak/projects/bioscan/bioscan_qc/qc_reports_rerun_Feb2026/`
**Source:** Output from the BIOSCAN QC pipeline. Organised by batch folder matching the mBRAVE structure.
**Contains:** Per batch: `filtered_metadata_batchN.csv` (all specimens with sequences, QC category 1-12, decision YES/NO/ON_HOLD), `qc_portal_batchN.csv` (all specimens including FAILED, used for repeat analysis), `BOLD_filtered_sequences_batchN.fasta` (exactly the sequences submitted to BOLD).
**Why we use it:** The primary source for QC decisions per specimen. `filtered_metadata` is used for pass rate calculations. `qc_portal` is used for the specimen-level repeat analysis as it includes FAILED specimens. The FASTA files are used for sequence concordance checks against BOLD.

### BOLD workbench exports
**Path:** `/lustre/scratch126/tol/teams/lawniczak/users/lp20/bioscan_plate_checker_results/bold_workbench_YYYY.xlsx`
**Source:** Downloaded manually from the BOLD workbench (https://bench.boldsystems.org) filtered by upload year. Maximum 99,999 records per download — split into `bold_workbench_YYYYa.xlsx` / `bold_workbench_YYYYb.xlsx` for high-volume years. Use the `Lab Sheet` tab only.
**Contains:** One row per specimen on BOLD. Includes `Sample ID` (specimen ID), `BIN`, `Stop Codon`, `Contamination`, `Flagged Record`, `Barcode Compliant` flags. Does not contain the actual sequence.
**Why we use it:** The only source for BOLD quality flags. Sequences flagged for stop codons or contamination do not receive BIN assignments — this file tells us which specimens are affected and why. Combined with the portal dump (which has the actual sequences), we can identify whether the QC pipeline has since produced a better sequence that should replace the flagged BOLD record.

---

## Output file reference

All outputs are written to:
`/lustre/scratch126/tol/teams/lawniczak/users/lp20/bioscan_plate_checker_results/`

Files are dated `YYYYMMDD`.

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
| `pipeline_report_YYYYMMDD.txt` | Text | Summary | Human-readable report of the full pipeline status. Shows overall pass-through rates at each stage, per-partner breakdown, plates flagged as old submissions not yet sequenced (configurable threshold, default 180 days), and BOLD upload gaps by partner. Best file to share with project managers or partners. |

---

### From `bold_summary_from_portal.py` — BOLD upload status

| File | Type | Level | Description |
|---|---|---|---|
| `bold_summary_report_YYYYMMDD.txt` | Text | Summary | Report of BOLD upload status by partner: specimens uploaded, BIN coverage, missing BINs, and upload date distribution for un-BINned specimens. |
| `bold_missing_bin_YYYYMMDD.csv` | CSV | Specimen | Every specimen with a sequence on BOLD but no BIN URI. Includes upload date and submission date. Use to identify which specimens need chasing with the BOLD team. |
| `bold_plate_summary_YYYYMMDD.csv` | CSV | Plate | Plate-level summary of BOLD upload counts, BIN coverage, and earliest/latest upload dates. |

---

### From `plate_summary_all.py` — comprehensive plate-level QC summary

| File | Type | Level | Description |
|---|---|---|---|
| `plate_summary_all_ALL_YYYYMMDD.csv` | CSV | Plate | **Primary plate QC table.** One row per plate across all partners. Uses best QC result per specimen across all sequencing batches — plain and split batch QC files are both used, so if a specimen passed in any batch it is counted as PASS. Columns: partner, submit date, n_batches_sequenced, n_specimens, n_controls, pass/on_hold/fail counts and rates, combined rate, positive control well and reads, lysate negative well and reads, random negative SQPP ID and well and reads. Plates never sequenced appear with null sequencing columns. |
| `plate_summary_categories_ALL_YYYYMMDD.csv` | CSV | Plate | Same as above but with individual category 1–12 counts instead of PASS/ON_HOLD/FAIL buckets. Categories sourced from `filtered_metadata`; decisions from `qc_portal` (includes FAILED specimens). |

### From `repeat_analysis.py` — repeat sequencing (plate level)

| File | Type | Level | Description |
|---|---|---|---|
| `repeat_analysis_YYYYMMDD.csv` | CSV | Plate | One row per plate sequenced more than once. Shows all batches, pass rate in first vs best vs last sequencing, and improvement achieved. |
| `repeat_analysis_YYYYMMDD.xlsx` | Excel | Plate | Same as above, Excel format. |

---

### From `repeat_analysis_specimens.py` — repeat sequencing (specimen level)

| File | Type | Level | Description |
|---|---|---|---|
| `repeat_specimens_summary_YYYYMMDD.csv` | CSV | Specimen | One row per repeated specimen. Shows first/last/best QC decision, whether outcome improved or declined, and all batches as a comma-separated list. Key file for understanding repeat sequencing outcomes at specimen level. |
| `repeat_specimens_transitions_YYYYMMDD.csv` | CSV | Summary | Transition matrix of first → last QC decision (PASS/ON_HOLD/FAILED) with counts and percentages. Shows how many specimens improved, declined, or stayed the same across repeat sequencings. |
| `repeat_specimens_long_YYYYMMDD.csv` | CSV | Specimen | One row per specimen per batch, with QC decision and full description text. Good for detailed investigation of specific specimens or batches. |
| `repeat_specimens_wide_YYYYMMDD.csv` | CSV | Specimen | One row per specimen, with one column per batch showing the decision. Easiest to read in Excel for viewing the trajectory at a glance. |

---

### From `missing_specimen_analysis.py` — consensus assembly failures

| File | Type | Level | Description |
|---|---|---|---|
| `missing_specimens_categorised_YYYYMMDD.csv` | CSV | Specimen | One row per specimen absent from the consensusseq table. Categorised as Cat1 (zero reads), Cat2_low (few reads, no consensus), or Cat2_high (reads present but no consensus — investigate). |
| `missing_specimens_batch_summary_YYYYMMDD.csv` | CSV | Batch | Batch-level counts of expected vs present specimens in each category. Use to identify batches with systematic assembly failures. |

---

### From `bold_workbench_analysis.py` — BOLD quality flags and sequence concordance

| File | Type | Level | Description |
|---|---|---|---|
| `bold_workbench_combined.csv` | CSV | Cache | Combined workbench records from all annual files. Auto-generated on first run. Delete and rerun with `--rebuild-cache` if source files change. |
| `bold_workbench_report_YYYYMMDD.txt` | Text | Summary | Quality flag counts (stop codon, contamination, flagged record, BIN compliance) by partner, plus sequence comparison results. |
| `bold_workbench_plates_YYYYMMDD.csv` | CSV | Plate | Plate-level counts of each quality flag type. |
| `bold_flagged_comparison_YYYYMMDD.csv` | CSV | Specimen | For every flagged specimen, shows whether the BOLD sequence is IDENTICAL or DIFFERENT to the QC-passed FASTA. |
| `bold_needs_resubmission_YYYYMMDD.csv` | CSV | Specimen | **Key actionable output.** Specimens with no BIN, a quality flag, and a DIFFERENT (better) sequence in QC. These should be resubmitted to BOLD to obtain a BIN. Includes partner, flag type, and upload date. |
| `bold_flagged_no_alternative_YYYYMMDD.csv` | CSV | Specimen | Flagged specimens where the QC sequence is IDENTICAL to BOLD — flag is genuine, no better sequence available. Requires manual expert assessment. |
| `bold_full_concordance_YYYYMMDD.csv` | CSV | Specimen | **Ad hoc only** (`--full-concordance`). Every sequence on BOLD compared against QC FASTA. Confirms 100% concordance or flags drift. |

---

### From `bold_check.R` — quarterly BOLD sanity check

| File | Type | Level | Description |
|---|---|---|---|
| `bold_check_specimens_YYYYMMDD.csv` | CSV | Specimen | Full portal vs BOLD comparison. Large file (~200MB) — keep as raw cache. |
| `bold_check_plates_YYYYMMDD.csv` | CSV | Plate | Plate-level BOLD coverage summary including BIN counts. |
| `bold_check_JOBID.log` | Text | Log | Job log with download progress and final summary. |

---

### Internal/cache files

| File | Description |
|---|---|
| `portal_plates_from_dump.csv` | Cached plate-level portal summary. Rebuilt by `read_portal_dump.py` when dump is updated. |
| `bold_workbench_combined.csv` | Cached combined workbench. Rebuilt with `--rebuild-cache`. |

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

### 3. Update config.py when a new portal dump is available

Edit `PORTAL_DUMP_TSV` in `config.py` to point to the latest `sts_manifests_YYYYMMDD.tsv`.

### 4. Build the portal plate summary

```bash
python3 read_portal_dump.py
```

### 5. API key for BOLD (quarterly sanity check only)

```bash
cp .env.example .env
# Edit .env and add your BOLD API key
```

---

## Routine run order

```bash
# 1. Build portal plate summary from dump (~30 seconds)
python3 read_portal_dump.py

# 2. Master plate status table: portal → mBRAVE → QC → BOLD
python3 plate_status_report.py --partner ALL

# 3. Human-readable pipeline report
python3 generate_pipeline_report.py

# 4. Comprehensive plate-level QC summary (best result per specimen, all controls)
python3 plate_summary_all.py --partner ALL

# 5. BOLD upload and BIN URI summary
python3 bold_summary_from_portal.py --partner ALL

# 6. Repeat analysis — plate level
python3 repeat_analysis.py --partner ALL

# 7. Repeat analysis — specimen level with QC decisions per batch
python3 repeat_analysis_specimens.py --partner ALL

# 8. Missing specimen analysis (~10 mins)
python3 missing_specimen_analysis.py --partner ALL

# 9. BOLD workbench analysis (when new workbench files downloaded)
python3 bold_workbench_analysis.py --partner ALL --rebuild-cache
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
| `/lustre/.../100k_paper/output/sts_manifests_YYYYMMDD.tsv` | Portal dump |
| `/lustre/.../bioscan_plate_checker_results/` | All outputs and workbench input files |

---

## Script reference

---

### `config.py`
Central configuration. Update `PORTAL_DUMP_TSV` when a new portal dump is available.

---

### `utils.py`
Shared utilities: batch folder resolution, plate ID parsing, cross-directory batch mapping, safe CSV reading.

**Batch deduplication rules:**
1. Splits (`batchN_0`, `batchN_1`...) always preferred over plain (`batchN`) or merged (`batchN_merged`)
2. Plain used only if no splits exist
3. Merged used only as last resort

```bash
python3 utils.py   # run the batch structure audit
```

---

### `read_portal_dump.py`
Reads the portal manifest TSV and builds a plate-level summary CSV. Run whenever a new dump is available.

```bash
python3 read_portal_dump.py
python3 read_portal_dump.py --input /path/to/sts_manifests_20260427.tsv
```

---

### `plate_status_report.py`
Main entry point. Joins portal, mBRAVE, and QC into a master plate status table. Controls excluded throughout.

```bash
python3 plate_status_report.py --partner ALL
python3 plate_status_report.py --partner BGEP
python3 plate_status_report.py --partner ALL --skip-portal
python3 plate_status_report.py --partner ALL --missing-only
```

---

### `generate_pipeline_report.py`
Human-readable pipeline report. Flags plates submitted more than N days ago not yet sequenced.

```bash
python3 generate_pipeline_report.py
python3 generate_pipeline_report.py --old-threshold-days 180
```

---

### `bold_summary_from_portal.py`
BOLD upload and BIN URI summaries from the portal dump. No API or R required.

```bash
python3 bold_summary_from_portal.py --partner ALL
python3 bold_summary_from_portal.py --partner FACE
```

---

### `plate_summary_all.py`
**Comprehensive plate-level QC summary across all partners and batches.**

One row per plate showing the best QC result per specimen across all repeat sequencings (PASS > ON_HOLD > FAILED). This means if a specimen passed in batch 1 but failed in batch 2, it counts as PASS — giving the true achievable success rate per plate rather than the result of any single sequencing run.

**n_controls** = 96 − n_specimens. For full plates this should be 3 (one positive control, one lysate negative, one random negative). Higher values indicate partial plates where empty wells were assigned control barcodes.

**Control columns reported (from last sequencing batch):**
- `pos_control_well` / `pos_control_reads` — positive control well position and read count
- `neg_lysate_well` / `neg_lysate_reads` — lysate negative control (fixed well: G12 for BGEP, H12 for others)
- `random_neg_sqpp_id` / `random_neg_well` / `random_neg_reads` — random negative control (SQPP specimen ID and random well position; not in portal — only identifiable from UMI stats files)

**Category source:** categories 1–12 come from `filtered_metadata` (reliable number-prefixed descriptions in all batch formats). Decisions come from `qc_portal` which includes FAILED specimens absent from `filtered_metadata`.

Produces two output files: PASS/ON_HOLD/FAIL summary, and categories 1–12 breakdown.

```bash
python3 plate_summary_all.py --partner ALL
python3 plate_summary_all.py --partner BGEP
python3 plate_summary_all.py --verbose
```

### `repeat_analysis.py`
Plate-level repeat analysis. Pass rate comparison between first and best sequencing batch.

```bash
python3 repeat_analysis.py --partner ALL
python3 repeat_analysis.py --min-sequencings 2
```

---

### `repeat_analysis_specimens.py`
Specimen-level repeat analysis. For every specimen appearing in more than one batch, shows the QC decision (PASS/ON_HOLD/FAILED) in each batch. Source is `qc_portal_batchN.csv` which includes all three decision types including FAILED specimens.

Key outputs include the transition matrix (e.g. FAILED → PASS: 6,005 specimens) showing the effectiveness of repeat sequencing across the whole dataset.

```bash
python3 repeat_analysis_specimens.py --partner ALL
python3 repeat_analysis_specimens.py --decision-filter FAILED
python3 repeat_analysis_specimens.py --min-appearances 2
```

---

### `missing_specimen_analysis.py`
Categorises specimens absent from consensusseq by read count.

| Category | Definition | Interpretation |
|---|---|---|
| **Cat1** | Count = 0 | Well failed to sequence. Expected. |
| **Cat2_low** | 0 < Count < threshold | Below assembly minimum. |
| **Cat2_high** | Count ≥ threshold, no consensus | Assembly failed despite reads. Investigate. |
| **Cat3** | Absent from UMI stats | Barcode error. Zero cases detected. |

```bash
python3 missing_specimen_analysis.py --partner ALL
python3 missing_specimen_analysis.py --batch batch41_1
python3 missing_specimen_analysis.py --low-read-threshold 50
```

---

### `bold_workbench_analysis.py`
Analyses BOLD workbench exports to assess quality flags and compare sequences.

**Input files** (place in results directory):
```
bold_workbench_2021.xlsx
bold_workbench_2022.xlsx
bold_workbench_2023.xlsx
bold_workbench_2024a.xlsx   ← split if >99,999 records
bold_workbench_2024b.xlsx
bold_workbench_2025a.xlsx
bold_workbench_2025b.xlsx
bold_workbench_2026.xlsx
```

**Sequence comparison results:**
- `IDENTICAL` — same sequence in BOLD and QC → flag is genuine
- `DIFFERENT` → QC has better sequence → **resubmit to BOLD**
- `QC_ONLY` — passed QC, not on BOLD
- `BOLD_ONLY` — on BOLD, not in QC FASTA
- `NEITHER` — flagged specimen with no sequence in either system

```bash
python3 bold_workbench_analysis.py --partner ALL          # routine
python3 bold_workbench_analysis.py --full-concordance     # ad hoc
python3 bold_workbench_analysis.py --rebuild-cache        # after new files
python3 bold_workbench_analysis.py --skip-sequence-comparison
```

---

### `bold_check.R` + `run_bold_check.sh`
Quarterly BOLDconnectR sanity check. Confirms portal and BOLD are in sync.
Portal/BOLD concordance confirmed 100% as of April 2026.

```bash
bsub < run_bold_check.sh
```

---

## BOLD API key security

- Store in `~/bioscan_plate_checker/.env` (in `.gitignore`, never committed)
- Load with `export $(cat .env | xargs)` before R scripts
- See `.env.example` for format

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
- **2,952 specimens** where QC has a better sequence than the flagged BOLD record → candidates for resubmission
- **1,912 specimens** with genuine flags — same sequence in QC and BOLD, need manual expert assessment
- **57 specimens** absent from BOLD workbench entirely — all BGE partners; edge well positions suggest quality-based removal
- **Portal/BOLD concordance: 100%** — all 272,005 specimens confirmed on BOLD
- **21,780 specimens** sequenced more than once; 6,005 improved from FAILED → PASS; 4,351 persistently FAILED
- **252 plates** sequenced more than once; average pass rate improvement 6.1% (first→last batch); best-batch selection recommended over most-recent-batch
- **6,208 specimens** with reads but no consensus — concentrated in BGKU and WALW (aquatic invertebrates, taxon-specific assembly failure)
- **Cat3 = 0** — no demultiplexing failures detected across any batch
