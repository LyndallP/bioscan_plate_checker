"""
new_run.py — create a timestamped run folder and print the shell export command.

Usage:
    eval $(python3 new_run.py)

This sets BIOSCAN_RUN_DIR in your current shell. All scripts that write output
will then place their files in that folder instead of creating separate folders.

To stop sharing the folder (back to auto-generate mode):
    unset BIOSCAN_RUN_DIR
"""

import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

run_ts  = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
run_dir = os.path.join(config.RESULTS_DIR, run_ts)
os.makedirs(run_dir, exist_ok=True)

print(f"export BIOSCAN_RUN_DIR={run_dir}")
