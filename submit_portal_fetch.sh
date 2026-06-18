#!/bin/bash
#BSUB -J portal_fetch
#BSUB -n 1
#BSUB -M 16000
#BSUB -R "select[mem>16000] rusage[mem=16000] span[hosts=1]"
#BSUB -G team222
#BSUB -W 360
#BSUB -o /lustre/scratch126/tol/teams/lawniczak/users/lp20/bioscan_plate_checker_results/portal_fetch_%J.out
#BSUB -e /lustre/scratch126/tol/teams/lawniczak/users/lp20/bioscan_plate_checker_results/portal_fetch_%J.err
#
# submit_portal_fetch.sh
#
# Submits the BIOSCAN portal dump fetch as a true LSF batch job rather than
# an interactive session, so it survives disconnects and does not depend on
# keeping a terminal or tmux session alive.
#
# IMPORTANT: This script is submitted to bsub as a FILE (bsub < script.sh),
# never as an inline command. The original problem was that bsub mangles
# shell quoting when given the tol CLI's JSON filter argument directly on
# the command line. Submitting a script file avoids this entirely, because
# bsub only ever sees "python3 read_portal_dump.py --fetch" — the JSON
# filter is built and passed to subprocess.run() as a Python list, inside
# the script, after LSF has already started the job. No shell re-parsing
# of the filter ever happens.
#
# Usage:
#   cd ~/bioscan_plate_checker
#   bsub < submit_portal_fetch.sh
#
# This returns immediately with a job ID. The job runs in the background
# on a compute node and does not require tmux, an interactive session, or
# your terminal to stay open.
#
# Check status:
#   bjobs                      # see if it's still running
#   bjobs -l <job_id>          # detailed status
#
# Check progress / output while running or after completion:
#   tail -f /lustre/scratch126/tol/teams/lawniczak/users/lp20/bioscan_plate_checker_results/portal_fetch_<job_id>.out
#
# A completion marker file is written on success or failure so you can
# check unambiguously without reading the whole log:
#   cat /lustre/scratch126/tol/teams/lawniczak/users/lp20/bioscan_plate_checker_results/portal_fetch_status.txt

set -e

RESULTS_DIR="/lustre/scratch126/tol/teams/lawniczak/users/lp20/bioscan_plate_checker_results"
SCRIPT_DIR="/nfs/users/nfs_l/lp20/bioscan_plate_checker"
STATUS_FILE="${RESULTS_DIR}/portal_fetch_status.txt"

echo "============================================================"
echo "Portal dump fetch — LSF batch job"
echo "Job ID:    ${LSB_JOBID:-unknown}"
echo "Host:      $(hostname)"
echo "Started:   $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"
echo ""

echo "RUNNING (job ${LSB_JOBID:-unknown}, started $(date '+%Y-%m-%d %H:%M:%S'))" > "$STATUS_FILE"

# Activate conda environment
source /software/treeoflife/conda/etc/profile.d/conda.sh 2>/dev/null || \
    source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate bioscan-ops

cd "$SCRIPT_DIR"

echo "Running: python3 read_portal_dump.py --fetch"
echo ""

if python3 read_portal_dump.py --fetch; then
    echo ""
    echo "============================================================"
    echo "SUCCESS — completed at $(date '+%Y-%m-%d %H:%M:%S')"
    echo "============================================================"
    echo "SUCCESS (job ${LSB_JOBID:-unknown}, completed $(date '+%Y-%m-%d %H:%M:%S'))" > "$STATUS_FILE"
else
    EXIT_CODE=$?
    echo ""
    echo "============================================================"
    echo "FAILED — exit code ${EXIT_CODE} at $(date '+%Y-%m-%d %H:%M:%S')"
    echo "============================================================"
    echo "FAILED (job ${LSB_JOBID:-unknown}, exit code ${EXIT_CODE}, failed $(date '+%Y-%m-%d %H:%M:%S'))" > "$STATUS_FILE"
    exit $EXIT_CODE
fi
