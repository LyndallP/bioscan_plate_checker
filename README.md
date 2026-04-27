# bioscan_plate_checker

Tracks BIOSCAN plates through every stage of the sequencing and QC pipeline to identify missing plates, assess repeat sequencing, summarise BOLD upload status, and find specimens lost during consensus assembly.

---

## Setup

### 1. Clone and configure

```bash
git clone git@github.com:LyndallP/bioscan_plate_checker.git
cd bioscan_plate_checker
```

Create a `.env` file for your BOLD API key (only needed for the occasional `bold_check.R` sanity check — not for routine use):
```bash
cp .env.example .env
# Edit .env and add your BOLD API key
```

### 2. Activate the conda environment
```bash
conda activate bioscan-ops
```

### 3. Build the portal plate summary (run once, or when portal dump is updated)
```bash
python3 read_portal_dump.py
```

---

## Routine run order

```bash
# 1. Build portal plate summary from dump (fast, ~30 seconds)
python3 read_portal_dump.py

# 2. Build master plate status table (portal → mBRAVE → QC → BOLD)
python3 plate_status_report.py --partner ALL

# 3. Generate human-readable pipeline report
python3 generate_pipeline_report.py

# 4. BOLD upload and BIN URI summary (from portal dump — no API needed)
python3 bold_summary_from_portal.py --partner ALL

# 5. Repeat analysis
python3 repeat_analysis.py --partner ALL

# 6. Missing specimen analysis (slow — scans all mBRAVE batches)
python3 missing_specimen_analysis.py --partner ALL
```

### Occasional BOLD sanity check (run ~quarterly)
```bash
# Verifies portal and BOLD are in sync — requires R and API key
bsub < run_bold_check.sh
```

---

## Key data paths

| Path | Contents |
|---|---|
| `/lustre/.../mbrave_batch_data/` | mBRAVE output by batch |
| `/lustre/.../qc_reports_rerun_Feb2026/` | QC reports by batch |
| `/lustre/.../100k_paper/output/sts_manifests_20260408.tsv` | Portal dump (specimen-level, all BOLD fields) |
| `/lustre/.../bioscan_plate_checker_results/` | All outputs |

---

## Script reference

---

### `config.py`
Central configuration. Edit this if lustre paths or the portal dump filename change.

**Key settings:**
- `MBRAVE_DIR` — path to mBRAVE batch data
- `QC_DIR` — path to QC reports
- `RESULTS_DIR` — where outputs are written
- `PORTAL_DUMP_TSV` — path to portal manifest dump
- `PORTAL_PLATES_CSV` — cached plate-level portal summary

---

### `utils.py`
Shared utilities used by all scripts.

**Key functions:**
- `resolve_batches(data_dir)` — returns list of batch folders to use, applying dedup rules
- `build_batch_cross_map(mbrave_dir, qc_dir)` — maps mBRAVE batch folders to QC batch folders
- `extract_plate_from_pid(pid)` — strips well coordinate from specimen ID
- `audit_batch_structure(mbrave_dir, qc_dir)` — prints full batch resolution audit

**Batch deduplication rules (applied independently per directory):**
1. Splits (`batchN_0`, `batchN_1`...) always preferred over plain (`batchN`) or merged (`batchN_merged`)
2. Plain used only if no splits exist
3. Merged used only as last resort

**Run the audit:**
```bash
python3 utils.py
```

---

### `read_portal_dump.py`
Reads the pre-exported portal manifest TSV and builds a plate-level summary CSV.
Run this instead of querying the portal live (live queries take ~2 hours for 350k+ specimens).

**Input:** `sts_manifests_YYYYMMDD.tsv` (portal dump)
**Output:** `portal_plates_from_dump.csv`

**Usage:**
```bash
python3 read_portal_dump.py
python3 read_portal_dump.py --input /path/to/sts_manifests_20260408.tsv
```

---

### `plate_status_report.py`
Main entry point. Joins portal, mBRAVE, and QC data into a master plate status table.
Controls (`CONTROL_NEG_*`, `CONTROL_POS_*`) are excluded throughout.

**Output columns:**

| Column | Description |
|---|---|
| `plate_id` | Plate identifier |
| `partner` | 4-letter partner code (from portal) |
| `submit_date` | Date plate was submitted to portal |
| `portal_status` | FOUND / MISSING |
| `portal_n_wells` | Number of wells in portal |
| `mbrave_status` | FOUND / MISSING |
| `mbrave_batches` | Comma-separated batch list |
| `n_sequencings` | Times plate was sequenced |
| `qc_status` | FOUND / MISSING |
| `qc_batches` | Comma-separated QC batch list |
| `best_qc_result` | PASS / ON_HOLD / FAIL / MISSING |
| `bold_status` | HAS_DATA / NO_DATA (from portal dump) |
| `pipeline_stage` | Furthest stage reached |
| `missing_at` | Stage where plate dropped out |

**Usage:**
```bash
python3 plate_status_report.py --partner ALL
python3 plate_status_report.py --partner BGEP
python3 plate_status_report.py --partner ALL --skip-portal
python3 plate_status_report.py --partner ALL --missing-only
```

---

### `generate_pipeline_report.py`
Generates a human-readable text report from the master plate status CSV.
Flags plates submitted more than N days ago that have not yet been sequenced.

**Usage:**
```bash
python3 generate_pipeline_report.py
python3 generate_pipeline_report.py --old-threshold-days 180
python3 generate_pipeline_report.py --input /path/to/bioscan_plate_status_ALL_YYYYMMDD.csv
```

---

### `bold_summary_from_portal.py`
Generates BOLD upload and BIN URI summaries directly from the portal dump.
**No API call, no R, no BOLDconnectR required.**

Uses these fields from the portal dump:
- `bold_nuc` — sequence present = uploaded to BOLD
- `bold_sequence_upload_date` — when uploaded
- `bold_bin_uri` — BIN assignment (null = missing BIN, needs follow-up)
- `bold_bin_created_date` — when BIN was assigned

**Produces:**
- `bold_summary_report_YYYYMMDD.txt` — full report with partner breakdown, upload dates, missing BINs
- `bold_missing_bin_YYYYMMDD.csv` — specimens with sequence but no BIN URI
- `bold_plate_summary_YYYYMMDD.csv` — plate-level BOLD status

**Usage:**
```bash
python3 bold_summary_from_portal.py --partner ALL
python3 bold_summary_from_portal.py --partner FACE
```

**Note on missing BIN URIs:** Specimens uploaded to BOLD but with no `bold_bin_uri` have sequences
but have not been assigned to a BIN cluster. If the upload date is more than a few weeks ago this
needs follow-up with the BOLD team. Recent uploads (< 2 weeks) may simply not have been processed yet.

---

### `repeat_analysis.py`
Identifies plates sequenced more than once and compares pass rates between batches.

**Output columns:**

| Column | Description |
|---|---|
| `plate_id` | Plate identifier |
| `n_sequencings` | Number of QC batches |
| `batches` | All QC batches (comma-separated) |
| `first_pct_pass` | Pass rate in first batch |
| `last_pct_pass` | Pass rate in most recent batch |
| `best_pct_pass` | Highest pass rate achieved |
| `improvement` | `last_pct_pass - first_pct_pass` |
| `bold_uploaded` | Whether plate is on BOLD |

**Usage:**
```bash
python3 repeat_analysis.py --partner ALL
python3 repeat_analysis.py --partner BGEP
python3 repeat_analysis.py --min-sequencings 2
```

---

### `missing_specimen_analysis.py`
Categorises specimens from UMI sample_stats files by their presence in the
consensusseq_network table. Three categories:

| Category | Definition | Interpretation |
|---|---|---|
| **Cat1** — zero reads | Count = 0 in UMI stats | Well failed to sequence. Expected absence from consensusseq. Not a pipeline failure. |
| **Cat2_low** — few reads, no seq | Count > 0 but < threshold, absent from consensusseq | Likely below assembly minimum read count. |
| **Cat2_high** — reads but no seq | Count ≥ threshold, absent from consensusseq | Got reads but consensus assembly failed. Investigate — may be taxon-specific (e.g. BGKU aquatic invertebrates with mucus/shell). |
| **Cat3** — absent from UMI | Not in UMI stats at all | Would indicate a manifest/barcode assignment error. Has been zero across all batches checked. |

Controls (H12, G12 wells, CONTROL_NEG/POS labels) are excluded.

**UMI file pattern:** `umi.*_sample_stats.txt` (excludes `_control_neg_stats.txt` and `_control_pos_stats.txt`)

**Usage:**
```bash
python3 missing_specimen_analysis.py --partner ALL
python3 missing_specimen_analysis.py --batch batch41_1
python3 missing_specimen_analysis.py --low-read-threshold 50
python3 missing_specimen_analysis.py --verbose
```

---

### `bold_check.R` + `run_bold_check.sh`
Occasional sanity check that queries BOLD directly via BOLDconnectR and compares
against the portal dump. **Only needed ~quarterly** — routine BOLD summaries use
`bold_summary_from_portal.py` instead.

Portal/BOLD concordance has been confirmed as perfect (272,005/272,005 specimens matched).

**Requires:**
```bash
module load HGI/softpack/users/aw43/BOLDconnectR_bioscan/2
export BOLD_API_KEY="your-key-here"  # or load from .env
```

**Submit as farm job:**
```bash
bsub < run_bold_check.sh
```

---

### `mbrave_checker.py` / `qc_checker.py`
Internal modules used by `plate_status_report.py` and `repeat_analysis.py`.
Can also be run standalone for debugging:

```bash
python3 mbrave_checker.py --partner ALL --verbose
python3 qc_checker.py --partner ALL --verbose
```

---

## BOLD API key security

- Store in `~/bioscan_plate_checker/.env` (listed in `.gitignore`, never committed)
- Load with `export $(cat .env | xargs)` before running R scripts
- See `.env.example` for the required format

---

## Notes on plate ID formats

| Format | Example | Partner |
|---|---|---|
| `XXXX_NNN` | `HIRW_001` | Standard 4-letter code |
| `XXXX-NNN` | `BGEP-161` | Dash-separated |
| `TOL-XXXX-NNN` | `TOL-BGEP-001` | TOL-prefixed |
| `MOZZ00000609A` | `MOZZ00000609A` | Early MOZZ format |
| `BSN_NNN` | `BSN_052` | Early UK BIOSCAN R&D plates |

MOZZ plates were shared across multiple partners in early batches — partner disambiguation
requires the portal `sts_gal_abbreviation` field.

Early non-standard plates (`BSN_052_A`, `BSN_052_A_100nL` etc.) are R&D/test runs
not formally submitted to the portal — they appear in mBRAVE but have no portal record
and are expected to be absent from the master plate table.

---

## Known findings (as of April 2026)

- **324 plates** submitted to portal but not yet in mBRAVE — 42 submitted in 2023 (FACE, FRBX) warrant investigation
- **702 plates** through QC but not yet on BOLD
- **6,261 specimens** on BOLD with no BIN URI — FACE (803), RRNW (386), CAMP (332) most affected; all uploaded >2 weeks ago, needs BOLD team follow-up
- **Portal/BOLD concordance: 100%** — all 272,005 specimens marked as uploaded were confirmed on BOLD
- **248 plates** sequenced more than once; average improvement 6.1%; TOL-BGEP-010 most improved (6.5% → 98.9%)
- **6,208 specimens** with reads but no consensus sequence (Cat2_high) — concentrated in BGKU and WALW plates (aquatic invertebrates, taxon-specific assembly failure)
- **Cat3 = 0** across all batches — no demultiplexing failures detected
