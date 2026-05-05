"""
qc_checker.py

Scans qc_reports using filtered_metadata_batchN.csv files.

filtered_metadata contains ALL specimens with QC decisions:
  - Sample.Plate.ID  : plate identifier (e.g. "CAMP_211")
  - pid              : specimen+well (e.g. "CAMP_211_A1")
  - category_decision: YES / NO / ON_HOLD
  - run_primary      : batch identifier

This is the correct file to use — qc_portal files are flat upload files
for the ToL portal and do not contain the full QC record.

Plate vs specimen distinction:
  - Plate level  : did this plate appear in QC at all? (Sample.Plate.ID)
  - Specimen level: per-well pass/fail counts within each plate (pid + category_decision)
Both are captured in the summary.
"""

import os
import re
import pandas as pd
from collections import defaultdict

import config
from utils import (build_batch_cross_map, find_file_in_batch,
                   normalise_plate_id, safe_read_csv,
                   batch_sort_key, matches_partner, is_bge_plate)

_DECISION_MAP = {'YES': 'PASS', 'NO': 'FAIL', 'ON_HOLD': 'ON_HOLD'}
_STATUS_RANK  = {'PASS': 3, 'ON_HOLD': 2, 'FAIL': 1, 'UNKNOWN': 0}

# Columns we need from filtered_metadata
_PLATE_COL    = 'Sample.Plate.ID'
_PID_COL      = 'pid'
_DECISION_COL = 'category_decision'


def get_qc_from_batch(batch_folder, batch_path, verbose=False):
    """
    Read filtered_metadata file from batch_path.
    Returns DataFrame [plate_id, pid, status, qc_batch] or empty DF.
    """
    meta_file = find_file_in_batch(batch_path, config.FILTERED_META_PATTERN)
    if meta_file is None:
        if verbose:
            print(f"  {batch_folder}: no filtered_metadata file")
        return pd.DataFrame()

    try:
        df = safe_read_csv(meta_file, dtype=str,
                           usecols=[_PLATE_COL, _PID_COL, _DECISION_COL])
    except Exception as e:
        # If usecols fails (column name mismatch), read all and check
        try:
            df = safe_read_csv(meta_file, dtype=str)
            missing = [c for c in [_PLATE_COL, _PID_COL, _DECISION_COL]
                       if c not in df.columns]
            if missing:
                print(f"  WARNING: {batch_folder} filtered_metadata missing "
                      f"columns: {missing}")
                print(f"           Found: {list(df.columns[:10])}")
                return pd.DataFrame()
            df = df[[_PLATE_COL, _PID_COL, _DECISION_COL]]
        except Exception as e2:
            print(f"  WARNING: failed reading {meta_file}: {e2}")
            return pd.DataFrame()

    df['plate_id'] = df[_PLATE_COL].apply(normalise_plate_id)
    df['status']   = df[_DECISION_COL].str.strip().map(_DECISION_MAP).fillna('UNKNOWN')
    df['qc_batch'] = batch_folder

    if verbose:
        n_plates = df['plate_id'].nunique()
        n_pass   = (df['status'] == 'PASS').sum()
        n_fail   = (df['status'] == 'FAIL').sum()
        n_hold   = (df['status'] == 'ON_HOLD').sum()
        print(f"  {batch_folder}: {n_plates} plates, {len(df)} specimens "
              f"(PASS={n_pass} FAIL={n_fail} ON_HOLD={n_hold})")

    return df[['plate_id', _PID_COL, 'status', 'qc_batch']].copy()


def build_qc_plate_index(mbrave_dir=None, qc_dir=None, partner=None, verbose=False):
    """
    Scan all resolved QC batches and build per-plate QC summary.

    Returns:
        plate_qc_summary: dict  plate_id -> {
            'qc_batches':    [qc_folder, ...],
            'batch_results': {qc_folder: {PASS:n, FAIL:n, ON_HOLD:n, total:n}},
            'best_status':   'PASS'|'ON_HOLD'|'FAIL'|'UNKNOWN',
            'total_specimens': int,
            'pass_specimens':  int,
        }
        mbrave_to_qc:     cross-map dict
        qc_to_mbrave:     reverse cross-map dict
        issues:           list of mapping warning strings
        batches_no_qc:    list of QC folders with no filtered_metadata file
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

    all_records   = []
    batches_no_qc = []

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

    # Exclude BGE partner plates (BGEP, BGEG, BGKU, BGPT) including TOL- variants
    bge_mask = combined['plate_id'].apply(lambda p: is_bge_plate(str(p)) if p else False)
    n_bge = bge_mask.sum()
    if n_bge > 0:
        print(f"  Excluded {n_bge} QC rows from BGE partners (BGEP/BGEG/BGKU/BGPT)")
    combined = combined[~bge_mask]

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
                'total':   len(bgrp),
            }

        # Best status across all batches (PASS beats ON_HOLD beats FAIL)
        best = max(grp['status'].unique(),
                   key=lambda s: _STATUS_RANK.get(s, 0))

        plate_qc_summary[plate_id] = {
            'qc_batches':      qc_batches,
            'batch_results':   batch_results,
            'best_status':     best,
            'total_specimens': len(grp),
            'pass_specimens':  (grp['status'] == 'PASS').sum(),
        }

    return plate_qc_summary, mbrave_to_qc, qc_to_mbrave, issues, batches_no_qc


def summarise_qc(plate_qc_summary, batches_no_qc):
    n        = len(plate_qc_summary)
    n_pass   = sum(1 for v in plate_qc_summary.values() if v['best_status'] == 'PASS')
    n_onhold = sum(1 for v in plate_qc_summary.values() if v['best_status'] == 'ON_HOLD')
    n_fail   = sum(1 for v in plate_qc_summary.values() if v['best_status'] == 'FAIL')
    n_repeat = sum(1 for v in plate_qc_summary.values() if len(v['qc_batches']) > 1)
    total_specimens = sum(v['total_specimens'] for v in plate_qc_summary.values())
    pass_specimens  = sum(v['pass_specimens']  for v in plate_qc_summary.values())
    print(f"\nQC: {n} plates  |  best_status: PASS={n_pass}  "
          f"ON_HOLD={n_onhold}  FAIL={n_fail}  repeated={n_repeat}")
    print(f"    {total_specimens} total specimen decisions, "
          f"{pass_specimens} PASS ({100*pass_specimens//total_specimens if total_specimens else 0}%)")
    if batches_no_qc:
        print(f"  Batches with no filtered_metadata: {batches_no_qc}")


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
