"""
qc_checker.py

Scans qc_reports and returns per-plate QC results.

Key complexity: mBRAVE may have splits (batch51_0..3) while QC has only
a plain folder (batch51) — the auto-merged QC case. We use
build_batch_cross_map() to resolve this, so QC results from batch51 are
correctly attributed to mBRAVE batches batch51_0/1/2/3.

category_decision values: YES (PASS), NO (FAIL), ON_HOLD
"""

import os
import pandas as pd
from collections import defaultdict

import config
from utils import (build_batch_cross_map, resolve_batches, find_file_in_batch,
                   extract_plate_from_pid, normalise_plate_id, safe_read_csv,
                   batch_sort_key, matches_partner)

_DECISION_MAP = {'YES': 'PASS', 'NO': 'FAIL', 'ON_HOLD': 'ON_HOLD'}
_STATUS_RANK  = {'PASS': 3, 'ON_HOLD': 2, 'FAIL': 1, 'UNKNOWN': 0}


def get_qc_from_batch(batch_folder, batch_path, verbose=False):
    """
    Read qc_portal file from batch_path.
    Returns DataFrame [plate_id, pid, status, qc_batch] or empty DF.
    """
    qc_file = find_file_in_batch(batch_path, config.QC_PORTAL_PATTERN)
    if qc_file is None:
        if verbose:
            print(f"  {batch_folder}: no qc_portal file")
        return pd.DataFrame()

    try:
        df = safe_read_csv(qc_file, dtype=str)
    except Exception as e:
        print(f"  WARNING: failed reading {qc_file}: {e}")
        return pd.DataFrame()

    # Validate columns
    for col in (config.QC_PID_COL, config.QC_DECISION_COL):
        if col not in df.columns:
            print(f"  WARNING: column '{col}' missing in {qc_file}")
            print(f"           Found: {list(df.columns)}")
            return pd.DataFrame()

    # Plate ID: prefer Sample.Plate.ID if present, else extract from pid
    if 'Sample.Plate.ID' in df.columns:
        df['plate_id'] = df['Sample.Plate.ID'].apply(normalise_plate_id)
    else:
        df['plate_id'] = df[config.QC_PID_COL].apply(extract_plate_from_pid)

    df['status']   = df[config.QC_DECISION_COL].map(_DECISION_MAP).fillna('UNKNOWN')
    df['qc_batch'] = batch_folder

    if verbose:
        print(f"  {batch_folder}: {df['plate_id'].nunique()} plates, "
              f"{len(df)} wells")

    return df[['plate_id', config.QC_PID_COL, 'status', 'qc_batch']].copy()


def build_qc_plate_index(mbrave_dir=None, qc_dir=None, partner=None, verbose=False):
    """
    Scan all resolved QC batches and build per-plate QC summary.

    Uses build_batch_cross_map() to handle auto-merged QC batches (where
    mBRAVE splits map to a single QC plain folder).

    Returns:
        plate_qc_summary: dict
            plate_id -> {
                'qc_batches':    [qc_folder, ...],
                'batch_results': {qc_folder: {PASS:n, FAIL:n, ON_HOLD:n}},
                'best_status':   'PASS'|'ON_HOLD'|'FAIL'|'UNKNOWN',
                'total_wells':   int,
            }
        mbrave_to_qc:     cross-map dict
        qc_to_mbrave:     reverse cross-map dict
        issues:           list of mapping warning strings
        batches_no_qc:    list of QC folders with no qc_portal file
    """
    if mbrave_dir is None:
        mbrave_dir = config.MBRAVE_DIR
    if qc_dir is None:
        qc_dir = config.QC_DIR

    (mbrave_to_qc, qc_to_mbrave, issues,
     mbrave_resolved, qc_resolved,
     mbrave_skipped, qc_skipped) = build_batch_cross_map(mbrave_dir, qc_dir)

    if verbose:
        print(f"\nQC: using {len(qc_resolved)} batches, "
              f"skipped {len(qc_skipped)}: {qc_skipped}")
        if issues:
            for issue in issues:
                print(f"  WARNING: {issue}")

    all_records    = []
    batches_no_qc  = []

    for qc_folder in qc_resolved:
        batch_path = os.path.join(qc_dir, qc_folder)
        df = get_qc_from_batch(qc_folder, batch_path, verbose=verbose)
        if df.empty:
            batches_no_qc.append(qc_folder)
        else:
            all_records.append(df)

    if not all_records:
        return {}, mbrave_to_qc, qc_to_mbrave, issues, batches_no_qc

    combined = pd.concat(all_records, ignore_index=True)

    if partner and partner.upper() != 'ALL':
        mask     = combined['plate_id'].apply(
            lambda p: matches_partner(str(p), partner) if p else False)
        combined = combined[mask]

    plate_qc_summary = {}
    for plate_id, grp in combined.groupby('plate_id'):
        if not plate_id:
            continue
        qc_batches = sorted(grp['qc_batch'].unique().tolist(), key=batch_sort_key)
        batch_results = {}
        for qc_batch, bgrp in grp.groupby('qc_batch'):
            counts = bgrp['status'].value_counts().to_dict()
            batch_results[qc_batch] = {
                'PASS':    counts.get('PASS', 0),
                'ON_HOLD': counts.get('ON_HOLD', 0),
                'FAIL':    counts.get('FAIL', 0),
                'UNKNOWN': counts.get('UNKNOWN', 0),
            }
        best = max(grp['status'].unique(),
                   key=lambda s: _STATUS_RANK.get(s, 0))
        plate_qc_summary[plate_id] = {
            'qc_batches':    qc_batches,
            'batch_results': batch_results,
            'best_status':   best,
            'total_wells':   len(grp),
        }

    return plate_qc_summary, mbrave_to_qc, qc_to_mbrave, issues, batches_no_qc


def summarise_qc(plate_qc_summary, batches_no_qc):
    n        = len(plate_qc_summary)
    n_pass   = sum(1 for v in plate_qc_summary.values() if v['best_status'] == 'PASS')
    n_onhold = sum(1 for v in plate_qc_summary.values() if v['best_status'] == 'ON_HOLD')
    n_fail   = sum(1 for v in plate_qc_summary.values() if v['best_status'] == 'FAIL')
    n_repeat = sum(1 for v in plate_qc_summary.values() if len(v['qc_batches']) > 1)
    print(f"\nQC: {n} plates  |  PASS={n_pass}  ON_HOLD={n_onhold}  "
          f"FAIL={n_fail}  repeated={n_repeat}")
    if batches_no_qc:
        print(f"  Batches with no qc_portal file: {batches_no_qc}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--partner', default='ALL')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    summary, mbrave_to_qc, qc_to_mbrave, issues, no_qc = build_qc_plate_index(
        partner=args.partner, verbose=args.verbose)
    summarise_qc(summary, no_qc)
    if issues:
        print(f"\nMapping issues ({len(issues)}):")
        for i in issues:
            print(f"  {i}")
