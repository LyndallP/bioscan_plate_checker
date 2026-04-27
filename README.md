# bioscan_plate_checker

Tracks BIOSCAN plates through every stage of the sequencing and QC pipeline to identify missing plates, assess repeat sequencing, verify BOLD uploads, and find specimens lost during demultiplexing.

---

## Setup

### 1. Clone and configure

```bash
git clone git@github.com:LyndallP/bioscan_plate_checker.git
cd bioscan_plate_checker
```

Create a `.env` file for your BOLD API key (never commit this):
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

# 2. Build master plate status table
python3 plate_status_report.py --partner ALL

# 3. Generate human-readable pipeline report
python3 generate_pipeline_report.py

# 4. Repeat analysis
python3 repeat_analysis.py --partner ALL

# 5. Missing specimen analysis (slow — scans all mBRAVE batches)
python3 missing_specimen_analysis.py --partner ALL

# 6. BOLD verification (submit as farm job — queries BOLD API)
bsub < run_bold_check.sh
```

---

## Key data paths

| Path | Contents |
|---|---|
| `/lustre/.../mbrave_batch_data/` | mBRAVE output by batch |
| `/lustre/.../qc_reports_rerun_Feb2026/` | QC reports by batch |
| `/lustre/.../100k_paper/output/sts_manifests_20260408.tsv` | Portal dump (specimen-level) |
| `/lustre/.../bioscan_plate_checker_results/` | All outputs |

---

## Script reference

---

### `config.py`
Central configuration. Edit this if lustre paths change.

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
- `build_batch_cross_map(mbrave_dir, qc_dir)` — maps mBRAVE batch folders to QC batch folders (handles auto-merged QC)
- `extract_plate_from_pid(pid)` — strips well coordinate from specimen ID
- `audit_batch_structure(mbrave_dir, qc_dir)` — prints full batch resolution audit

**Batch deduplication rules:**
1. Splits (`batchN_0`, `batchN_1`...) always preferred over plain (`batchN`) or merged (`batchN_merged`)
2. Plain used only if no splits exist
3. Merged used only as last resort

**Run the audit:**
```bash
python3 utils.py
```

---

### `read_portal_dump.py`
Reads the pre-exported portal manifest TSV and builds a plate-level summary CSV. Run this instead of querying the portal live (live queries take ~2 hours for 350k+ specimens).

**Input:** `sts_manifests_YYYYMMDD.tsv` (portal dump)
**Output:** `portal_plates_from_dump.csv`

**Columns used from dump:**
- `sts_specimen.id` → plate ID (strip well coordinate)
- `bold_nuc` → BOLD upload status
- `sts_submit_date` → submission date

**Usage:**
```bash
python3 read_portal_dump.py
python3 read_portal_dump.py --input /path/to/sts_manifests_20260408.tsv
```

---

### `plate_status_report.py`
Main entry point. Joins portal, mBRAVE, and QC data into a master plate status table.

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
| `bold_status` | HAS_DATA / NO_DATA |
| `pipeline_stage` | Furthest stage reached |
| `missing_at` | Stage where plate dropped out |

**Controls** (`CONTROL_NEG_*`, `CONTROL_POS_*`) are excluded throughout.

**Usage:**
```bash
python3 plate_status_report.py --partner ALL
python3 plate_status_report.py --partner BGEP
python3 plate_status_report.py --partner ALL --skip-portal
python3 plate_status_report.py --partner ALL --missing-only
```

---

### `generate_pipeline_report.py`
Generates a human-readable text report from the master plate status CSV. Flags plates submitted more than N days ago that have not yet been sequenced.

**Usage:**
```bash
python3 generate_pipeline_report.py
python3 generate_pipeline_report.py --old-threshold-days 180
python3 generate_pipeline_report.py --input /path/to/bioscan_plate_status_ALL_YYYYMMDD.csv
```

---

### `repeat_analysis.py`
Identifies plates sequenced more than once and compares pass rates between batches. Shows improvement/decline between first and best sequencing.

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
Finds specimens that are expected in a sequencing run (present in the UMI `_sample_stats.txt` file) but entirely absent from the `consensusseq_network.tsv` — meaning they were never demultiplexed from the FASTQ.

These are distinct from specimens with zero reads (which appear in the consensusseq with low counts). Completely absent specimens suggest a demultiplexing failure.

**UMI file pattern:** `umi.*_sample_stats.txt` (excludes `_control_neg_stats.txt` and `_control_pos_stats.txt`)

**Usage:**
```bash
python3 missing_specimen_analysis.py --partner ALL
python3 missing_specimen_analysis.py --batch batch30
python3 missing_specimen_analysis.py --verbose
```

---

### `bold_check.R`
Queries BOLD via BOLDconnectR using specimen IDs from the portal dump. Compares BOLD records against the portal to find:
- Specimens marked as uploaded in portal but not found on BOLD
- Specimens on BOLD but not in portal dump
- Specimens with sequences but no BIN URI (`bin_uri` is NA) — may need follow-up
- Plate-level BIN coverage summary

**Requires:**
```bash
module load HGI/softpack/users/aw43/BOLDconnectR_bioscan/2
export BOLD_API_KEY="your-key-here"  # or load from .env
```

**Submit as farm job (recommended — can take 1-2 hours for full dataset):**
```bash
bsub < run_bold_check.sh
# or with partner filter:
bsub < run_bold_check.sh --partner BGEP
```

**Output:**
- `bold_check_specimens_YYYYMMDD.csv` — specimen-level comparison
- `bold_check_plates_YYYYMMDD.csv` — plate-level BIN summary

**Note on BIN URIs:** Specimens with sequences but no BIN may be:
- Recently uploaded (BINs assigned with a delay) — check `sequence_upload_date`
- Genuinely missing BINs — flag for follow-up with BOLD team

---

### `mbrave_checker.py`
Scans mBRAVE batch data and builds plate → batch mapping. Used internally by `plate_status_report.py`.

```bash
python3 mbrave_checker.py --partner ALL --verbose
```

---

### `qc_checker.py`
Scans QC `filtered_metadata` files and builds per-plate QC summary. Used internally by `plate_status_report.py` and `repeat_analysis.py`.

```bash
python3 qc_checker.py --partner ALL --verbose
```

---

## BOLD API key security

The BOLD API key is sensitive and must never be committed to GitHub.

- Store it in `~/bioscan_plate_checker/.env` (listed in `.gitignore`)
- Load it with `export $(cat .env | xargs)` before running R scripts
- The `.env.example` file shows the required format without the real key
- **Do not paste the key into any script file**

---

## Notes on plate ID formats

| Format | Example | Partner |
|---|---|---|
| `XXXX_NNN` | `HIRW_001` | Standard 4-letter code |
| `XXXX-NNN` | `BGEP-161` | Dash-separated |
| `TOL-XXXX-NNN` | `TOL-BGEP-001` | TOL-prefixed |
| `MOZZ00000609A` | `MOZZ00000609A` | Early MOZZ format |
| `BSN_NNN` | `BSN_052` | Early UK BIOSCAN |

MOZZ plates were shared across multiple partners in early batches — partner disambiguation requires the portal `sts_gal_abbreviation` field.
