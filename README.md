# bioscan_plate_checker

A pipeline tracking and quality assessment toolkit for BIOSCAN DNA barcoding data. Traces every plate from submission through sequencing, QC, and BOLD upload to identify plates lost at any stage, assess repeat sequencing outcomes, verify BOLD upload integrity, and flag sequences with quality issues.

---

## Overview

BIOSCAN generates hundreds of sequencing plates per year across dozens of UK partner institutions. This toolkit answers the following questions routinely:

- **Which plates have been submitted but never sequenced?** And how long have they been waiting?
- **Which plates made it through sequencing but failed QC, or passed QC but haven't reached BOLD?**
- **Are all sequences on BOLD exactly what our QC pipeline produced?**
- **For plates sequenced more than once, did repeat sequencing improve the pass rate?**
- **Which specimens got reads but failed to produce a consensus sequence, and why?**
- **Which BOLD records are flagged for stop codons or contamination, and does our QC pipeline now have a better sequence that should replace them?**

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

```bash
# Place bold_workbench_YYYY.csv files in the results directory, then:

# Routine: quality flag report + flagged sequence comparison vs QC
python3 bold_workbench_analysis.py --partner ALL

# Ad hoc: full concordance check of ALL sequences on BOLD vs QC FASTA
python3 bold_workbench_analysis.py --partner ALL --full-concordance
```

### Quarterly BOLD sanity check (requires R and API key)

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

**Run the batch structure audit:**
```bash
python3 utils.py
```

---

### `read_portal_dump.py`
Reads the pre-exported portal manifest TSV and builds a plate-level summary CSV.
Avoids live portal queries which take ~2 hours for 350k+ specimens.

**Input:** `sts_manifests_YYYYMMDD.tsv`
**Output:** `portal_plates_from_dump.csv`

```bash
python3 read_portal_dump.py
python3 read_portal_dump.py --input /path/to/sts_manifests_20260408.tsv
```

---

### `plate_status_report.py`
**Main entry point.** Joins portal, mBRAVE, and QC data into a master plate status table with one row per plate. Controls (`CONTROL_NEG_*`, `CONTROL_POS_*`) are excluded throughout.

**Output columns:**

| Column | Description |
|---|---|
| `plate_id` | Plate identifier |
| `partner` | 4-letter partner code (from portal) |
| `submit_date` | Date plate was submitted to portal |
| `portal_status` | FOUND / MISSING |
| `portal_n_wells` | Number of wells registered in portal |
| `mbrave_status` | FOUND / MISSING |
| `mbrave_batches` | Comma-separated list of mBRAVE batches |
| `n_sequencings` | Number of times plate was sequenced |
| `qc_status` | FOUND / MISSING |
| `qc_batches` | Comma-separated list of QC batches |
| `best_qc_result` | PASS / ON_HOLD / FAIL / MISSING |
| `bold_status` | HAS_DATA / NO_DATA (from portal dump) |
| `pipeline_stage` | Furthest stage reached |
| `missing_at` | Stage where plate dropped out |

```bash
python3 plate_status_report.py --partner ALL
python3 plate_status_report.py --partner BGEP
python3 plate_status_report.py --partner ALL --skip-portal
python3 plate_status_report.py --partner ALL --missing-only
```

---

### `generate_pipeline_report.py`
Generates a human-readable text report from the master plate status CSV. Flags plates submitted more than N days ago that have not yet been sequenced — useful for identifying plates that may have been lost in the pipeline.

```bash
python3 generate_pipeline_report.py
python3 generate_pipeline_report.py --old-threshold-days 180
```

---

### `bold_summary_from_portal.py`
Generates BOLD upload and BIN URI summaries directly from the portal dump.
**No API call, no R, no BOLDconnectR required.**

Uses portal dump fields: `bold_nuc`, `bold_sequence_upload_date`, `bold_bin_uri`, `bold_bin_created_date`.

**Key finding this script addresses:** Specimens uploaded to BOLD but with no `bold_bin_uri` have sequences that have not been assigned to a BIN cluster. If the upload date is more than a few weeks ago, this requires follow-up with the BOLD team.

**Outputs:**
- `bold_summary_report_YYYYMMDD.txt` — partner breakdown, upload date distribution, missing BINs
- `bold_missing_bin_YYYYMMDD.csv` — specimens with sequence but no BIN URI
- `bold_plate_summary_YYYYMMDD.csv` — plate-level BOLD status

```bash
python3 bold_summary_from_portal.py --partner ALL
python3 bold_summary_from_portal.py --partner FACE
```

---

### `bold_workbench_analysis.py`
Analyses BOLD workbench exports to assess sequence quality flags and check whether the QC pipeline has produced better sequences than what is currently on BOLD.

**Background:** BOLD assigns quality flags to uploaded sequences including Stop Codon (internal stop codons in the COI region, suggesting pseudogenes or NUMTs), Contamination (foreign DNA), and Flagged Record (general quality concern). Sequences with these flags do not receive BIN assignments. This script identifies whether our QC pipeline — which includes a stop codon detection step that searches for alternative sequences — has since produced a clean sequence for these specimens that could replace the flagged BOLD record.

**Input files** (place in results directory):
```
bold_workbench_2021.csv   ┐
bold_workbench_2022.csv   │  Downloaded from BOLD workbench, filtered by year
bold_workbench_2023.csv   │  (max 99,999 records per download)
bold_workbench_2024.csv   │  Skip 2 header rows; columns include:
bold_workbench_2025.csv   │  Sample ID, BIN, Stop Codon, Contamination,
bold_workbench_2026.csv   ┘  Flagged Record, Barcode Compliant
```

**Two modes:**

**Routine** — run whenever new workbench files are downloaded:
- Identifies all flagged specimens (stop codon / contamination / flagged record)
- For each flagged specimen, compares the sequence currently on BOLD (from portal `bold_nuc`) against the sequence that passed QC (from `BOLD_filtered_sequences_batchN.fasta`)
- `IDENTICAL` → flag is genuine, same sequence in both — no BOLD update needed
- `DIFFERENT` → QC has found a better sequence → **BOLD record should be updated**
- `QC_ONLY` → specimen passed QC but no BOLD sequence found
- `BOLD_ONLY` → on BOLD but not in QC FASTA

**Ad hoc** (`--full-concordance`) — run occasionally as a sense check:
- Compares ALL sequences on BOLD against QC FASTA output
- Confirms 100% concordance between what the pipeline produced and what is on BOLD
- Catches any drift between QC output and BOLD records after repeat sequencing

**Outputs:**
- `bold_workbench_report_YYYYMMDD.txt` — flag summary by partner + sequence comparison results
- `bold_workbench_plates_YYYYMMDD.csv` — plate-level flag counts
- `bold_flagged_comparison_YYYYMMDD.csv` — per-specimen routine comparison results
- `bold_full_concordance_YYYYMMDD.csv` — per-specimen full concordance results (ad hoc only)
- `bold_workbench_combined.csv` — cached combined workbench file (auto-generated)

```bash
# Routine — flagged specimens only
python3 bold_workbench_analysis.py --partner ALL
python3 bold_workbench_analysis.py --partner FACE

# Ad hoc — full concordance of all sequences
python3 bold_workbench_analysis.py --partner ALL --full-concordance

# Rebuild cache after adding new year files
python3 bold_workbench_analysis.py --rebuild-cache

# Flag summary only, no sequence loading
python3 bold_workbench_analysis.py --skip-sequence-comparison
```

---

### `repeat_analysis.py`
Identifies plates sequenced more than once and compares pass rates between batches. Quantifies whether repeat sequencing improved outcomes.

**Output columns:**

| Column | Description |
|---|---|
| `n_sequencings` | Number of QC batches |
| `first_pct_pass` | Pass rate in first batch |
| `last_pct_pass` | Pass rate in most recent batch |
| `best_pct_pass` | Highest pass rate achieved |
| `improvement` | `last_pct_pass - first_pct_pass` |
| `bold_uploaded` | Whether plate is on BOLD |

```bash
python3 repeat_analysis.py --partner ALL
python3 repeat_analysis.py --partner BGEP
python3 repeat_analysis.py --min-sequencings 2
```

---

### `missing_specimen_analysis.py`
Categorises specimens from UMI sample_stats files by their presence in the consensusseq_network table. Distinguishes between specimens that genuinely failed to sequence versus those that got reads but failed consensus assembly.

**Three categories:**

| Category | Definition | Interpretation |
|---|---|---|
| **Cat1** — zero reads | Count = 0 in UMI stats | Well failed to sequence. Expected absence. Not a pipeline failure. |
| **Cat2_low** — few reads, no seq | 0 < Count < threshold, absent from consensusseq | Likely below minimum read count for assembly. |
| **Cat2_high** — reads but no seq | Count ≥ threshold, absent from consensusseq | Got reads but consensus assembly failed. **Investigate** — may be taxon-specific (e.g. BGKU aquatic invertebrates). |
| **Cat3** — absent from UMI | Not in UMI stats at all | Barcode assignment error. Zero cases detected across all batches. |

Controls (H12, G12 wells, CONTROL_NEG/POS labels) are excluded.

```bash
python3 missing_specimen_analysis.py --partner ALL
python3 missing_specimen_analysis.py --batch batch41_1
python3 missing_specimen_analysis.py --low-read-threshold 50
```

---

### `bold_check.R` + `run_bold_check.sh`
Quarterly sanity check that queries BOLD directly via BOLDconnectR and compares against the portal dump. Confirms portal and BOLD are fully in sync.

Portal/BOLD concordance has been confirmed as 100% (272,005/272,005 specimens matched as of April 2026).

**Requires:**
```bash
module load HGI/softpack/users/aw43/BOLDconnectR_bioscan/2
export BOLD_API_KEY="your-key-here"
```

**Submit as farm job (~40 minutes):**
```bash
bsub < run_bold_check.sh
```

---

## BOLD API key security

- Store in `~/bioscan_plate_checker/.env` (in `.gitignore`, never committed)
- Load with `export $(cat .env | xargs)` before running R scripts
- See `.env.example` for the required format

---

## Notes on plate ID formats

| Format | Example | Notes |
|---|---|---|
| `XXXX_NNN` | `HIRW_001` | Standard 4-letter partner code |
| `XXXX-NNN` | `BGEP-161` | Dash-separated |
| `TOL-XXXX-NNN` | `TOL-BGEP-001` | TOL-prefixed BGE plates |
| `MOZZ00000609A` | `MOZZ00000609A` | Early MOZZ format, shared across partners |
| `BSN_NNN` | `BSN_052` | Early UK BIOSCAN R&D plates (not formally submitted) |

MOZZ plates were shared across multiple partners in early batches — partner disambiguation requires the portal `sts_gal_abbreviation` field.

---

## Known findings (April 2026)

- **324 plates** submitted but not yet in mBRAVE — 42 submitted in 2023 (FACE, FRBX) warrant investigation
- **702 plates** through QC but not yet on BOLD
- **6,261 specimens** on BOLD with no BIN URI — predominantly FACE (803), RRNW (386), CAMP (332); all uploaded >2 weeks ago; stop codon and contamination flags confirmed as root cause
- **57 specimens** in portal missing-BIN list absent from BOLD workbench entirely — all BGE partners (BGKU, BGPT, BGEG); edge well positions suggest quality-based removal
- **Portal/BOLD concordance: 100%** — all 272,005 specimens confirmed on BOLD
- **248 plates** sequenced more than once; average pass rate improvement 6.1%; TOL-BGEP-010 most improved (6.5% → 98.9%)
- **6,208 specimens** with reads but no consensus sequence — concentrated in BGKU and WALW (aquatic invertebrates, taxon-specific assembly failure due to mucus/shell material)
- **Cat3 = 0** — no demultiplexing failures detected across any batch
