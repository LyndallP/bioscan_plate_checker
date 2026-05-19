#!/bin/bash
# run_pipeline.sh
#
# Runs the full BIOSCAN pipeline, placing all outputs into a single
# timestamped folder so one run = one folder.
#
# Usage:
#   bash run_pipeline.sh                   # standard run, BGE excluded
#   bash run_pipeline.sh --include-bge     # include BGE partners
#   bash run_pipeline.sh --skip-workbench  # skip bold_workbench_analysis
#   bash run_pipeline.sh --skip-concordance # skip bold_sequence_concordance

set -e

RESULTS_DIR="/lustre/scratch126/tol/teams/lawniczak/users/lp20/bioscan_plate_checker_results"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── Parse args ────────────────────────────────────────────────────────────────
BGE_FLAG="--exclude-bge"
SKIP_WORKBENCH=0
SKIP_CONCORDANCE=0
SKIP_BATCH_FAMILY=0

for arg in "$@"; do
    case $arg in
        --include-bge)        BGE_FLAG="" ;;
        --skip-workbench)     SKIP_WORKBENCH=1 ;;
        --skip-concordance)   SKIP_CONCORDANCE=1 ;;
        --skip-batch-family)  SKIP_BATCH_FAMILY=1 ;;
    esac
done

# ── Create single shared run folder ──────────────────────────────────────────
RUN_TS=$(date +%Y%m%d_%H%M%S)
RUN_DIR="${RESULTS_DIR}/${RUN_TS}"
mkdir -p "$RUN_DIR"

echo "============================================================"
echo "BIOSCAN Pipeline Run"
echo "============================================================"
echo "Run folder : $RUN_DIR"
echo "BGE flag   : ${BGE_FLAG:-(BGE included)}"
echo "Started    : $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"
echo ""

cd "$SCRIPT_DIR"

run_step() {
    local script=$1
    shift
    echo ">>> $script $@"
    python3 "$script" --run-dir "$RUN_DIR" "$@"
    echo ""
}

# ── Steps ─────────────────────────────────────────────────────────────────────
run_step plate_status_report.py        --partner ALL $BGE_FLAG
run_step generate_pipeline_report.py   $BGE_FLAG
run_step bold_summary_from_portal.py   --partner ALL $BGE_FLAG
run_step plate_summary_all.py          --partner ALL $BGE_FLAG --verbose
run_step qc_bold_mismatch_portal.py    $BGE_FLAG
run_step repeat_analysis.py            --partner ALL $BGE_FLAG
run_step repeat_analysis_specimens.py  --partner ALL $BGE_FLAG
run_step missing_specimen_analysis.py  --partner ALL $BGE_FLAG

if [ "$SKIP_WORKBENCH" -eq 0 ]; then
    run_step bold_workbench_analysis.py --partner ALL --rebuild-cache $BGE_FLAG
else
    echo ">>> Skipping bold_workbench_analysis.py"
    echo ""
fi

# Batch family sequence comparison (once in a while — run by default, skip with flag)
if [ "$SKIP_BATCH_FAMILY" -eq 0 ]; then
    run_step batch_family_sequence_comparison.py --verbose
else
    echo ">>> Skipping batch_family_sequence_comparison.py"
    echo ""
fi

if [ "$SKIP_CONCORDANCE" -eq 0 ]; then
    run_step bold_sequence_concordance.py $BGE_FLAG
else
    echo ">>> Skipping bold_sequence_concordance.py"
    echo ""
fi

run_step generate_html_report.py   $BGE_FLAG
run_step generate_summary_report.py $BGE_FLAG

echo "============================================================"
echo "Pipeline complete : $(date '+%Y-%m-%d %H:%M:%S')"
echo "All outputs in   : $RUN_DIR"
echo "============================================================"
