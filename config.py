"""
Central configuration for bioscan_plate_checker.
"""

import os

# ── Lustre data paths ─────────────────────────────────────────────────────────
MBRAVE_DIR = "/lustre/scratch126/tol/teams/lawniczak/projects/bioscan/bioscan_qc/mbrave_batch_data"
QC_DIR     = "/lustre/scratch126/tol/teams/lawniczak/projects/bioscan/bioscan_qc/qc_reports_rerun_Feb2026"
RESULTS_DIR = "/lustre/scratch126/tol/teams/lawniczak/users/lp20/bioscan_plate_checker_results"

# ── Batch deduplication rule (see utils.resolve_batches) ─────────────────────
# Priority per base_num group: splits > plain > merged
# Plain folder is IGNORED when splits exist (plain = likely incomplete download)
# Merged folder is IGNORED when splits exist (merged = failed post-processing)
# Special folders (batchRnD*, PCR1_volume_test_*, batch35_repeat_*, batch39_rep_*)
# are excluded from production analysis by default.

# ── mBRAVE file patterns ──────────────────────────────────────────────────────
CONSENSUSSEQ_NETWORK_PATTERN     = "*consensusseq_network.tsv"
CONSENSUSSEQ_NETWORK_CSV_PATTERN = "*consensusseq_network.csv"  # fallback
MBRAVE_PID_COL = "pid"   # column containing specimen+well ID e.g. "HIRW_001_A01"

# ── QC report file patterns ───────────────────────────────────────────────────
QC_PORTAL_PATTERN     = "qc_portal_batch*.csv"
FILTERED_META_PATTERN = "filtered_metadata_batch*.csv"

# Column names — CONFIRMED consistent across batch3, batch10, batch20, batch30
QC_PID_COL      = "pid"               # specimen+well ID
QC_DECISION_COL = "category_decision" # YES / NO / ON_HOLD
# filtered_metadata additionally has:
QC_PLATE_COL         = "Sample.Plate.ID"
QC_PARTNER_PLATE_COL = "partner_plate"

# Canonical QC decision values
QC_PASS_VALUES    = {"YES"}
QC_ONHOLD_VALUES  = {"ON_HOLD"}
QC_FAIL_VALUES    = {"NO"}

# ── Portal ────────────────────────────────────────────────────────────────────
PORTAL_BATCH_SIZE       = 50
PORTAL_RATE_LIMIT_SLEEP = 1.5  # seconds between batches

PORTAL_PLATE_FIELD   = "sts_rackid"
PORTAL_WELL_FIELD    = "sts_tubeid"
PORTAL_PARTNER_FIELD = "sts_gal_abbreviation"  # 4-letter; disambiguates MOZZ plates
PORTAL_BOLD_FIELD    = "bold_nuc"              # non-empty -> uploaded to BOLD

# ── Portal dump (pre-exported TSV — avoids live portal queries) ───────────────
PORTAL_DUMP_TSV = ("/lustre/scratch126/tol/teams/lawniczak/projects/bioscan"
                   "/100k_paper/output/sts_manifests_20260408.tsv")
PORTAL_PLATES_CSV = os.path.join(
    "/lustre/scratch126/tol/teams/lawniczak/users/lp20/bioscan_plate_checker_results",
    "portal_plates_from_dump.csv"
)
