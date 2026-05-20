"""
positive_control_analysis.py

Analyses positive control read counts across all sequencing batches to flag:
  1. Plates with low positive control reads relative to their batch median.
     Threshold: pos_control_reads < 50% of batch median → investigate /
     candidate for re-sequencing.
  2. Batches that are overall poor-performing: bottom 20% of all batches
     ranked by mean plate pass rate.

For each flagged plate the report also shows:
  - Specimen count from UMI sample_stats.txt (actual rows sequenced)
  - Specimen count from portal dump (expected plate size)
  - Pass rate for that specific batch run (not the best-across-batches figure)

Outputs:
  positive_control_plates_YYYYMMDD.csv       — one row per plate per batch
  positive_control_batch_summary_YYYYMMDD.csv — one row per batch

Usage:
    conda activate bioscan-ops
    python3 positive_control_analysis.py
    python3 positive_control_analysis.py --exclude-bge
    python3 positive_control_analysis.py --threshold 0.4   # 40% of batch median
"""

import argparse
import datetime
import glob
import os
import re
import numpy as np
import pandas as pd
from collections import defaultdict

import config
from utils import resolve_batches, is_bge_plate, matches_partner, resolve_run_dir

TODAY = datetime.datetime.now().strftime('%Y%m%d')

# ── Thresholds ────────────────────────────────────────────────────────────────

POS_CTRL_THRESHOLD = 0.50   # flag if < this fraction of batch median
POOR_BATCH_PERCENTILE = 20  # flag batches in the bottom N% by mean pass rate

# ── File patterns ─────────────────────────────────────────────────────────────

_POS_PATTERN    = "umi.*_control_pos_stats.txt"
_SAMPLE_PATTERN = "umi.*_sample_stats.txt"
_QC_PORTAL_PAT  = "qc_portal_batch*.csv"

_CONTROL_WELLS = {'H12', 'G12', 'h12', 'g12'}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalise_plate(pid):
    if not pid:
        return None
    s = str(pid).strip()
    if s.upper().startswith('TOL-'):
        s = s[4:]
    return s or None


def _is_control_label(label):
    s = str(label).upper()
    return s.startswith('CONTROL_NEG') or s.startswith('CONTROL_POS')


def _extract_well(label):
    m = re.search(r'_([A-H]\d{1,2})$', str(label))
    return m.group(1) if m else None


def _is_control_specimen(label):
    if _is_control_label(label):
        return True
    well = _extract_well(label)
    return well is not None and well.upper() in _CONTROL_WELLS


# ── Per-batch data loading ────────────────────────────────────────────────────

def load_pos_controls(batch_path):
    """
    Read umi.*_control_pos_stats.txt for one batch folder.
    Returns list of {plate_id, count} dicts.
    """
    rows = []
    for f in glob.glob(os.path.join(batch_path, _POS_PATTERN)):
        try:
            df = pd.read_csv(f, sep='\t', dtype=str)
            if 'Label' not in df.columns:
                continue
            for _, row in df.iterrows():
                plate = _normalise_plate(row.get('Sample Plate ID', ''))
                count_raw = str(row.get('Count', '0')).strip()
                try:
                    count = int(float(count_raw))
                except ValueError:
                    count = 0
                if plate and plate not in ('nan', ''):
                    rows.append({'plate_id': plate, 'pos_control_reads': count})
        except Exception:
            pass
    return rows


def load_umi_specimen_counts(batch_path):
    """
    Read umi.*_sample_stats.txt for one batch folder.
    Returns dict: plate_id -> specimen_count (excluding control wells).
    """
    counts = defaultdict(int)
    for f in glob.glob(os.path.join(batch_path, _SAMPLE_PATTERN)):
        if 'control_neg' in f or 'control_pos' in f:
            continue
        try:
            df = pd.read_csv(f, sep='\t', dtype=str)
            if 'Label' not in df.columns:
                continue
            for _, row in df.iterrows():
                label = str(row.get('Label', '')).strip()
                plate = _normalise_plate(row.get('Sample Plate ID', ''))
                if not plate or plate in ('nan', ''):
                    continue
                if _is_control_specimen(label):
                    continue
                counts[plate] += 1
        except Exception:
            pass
    return dict(counts)


def load_qc_pass_rates(qc_batch_path):
    """
    Read qc_portal_batch*.csv for one QC batch folder.
    Returns dict: plate_id -> {'pass': int, 'total': int, 'pass_rate': float}
    """
    per_plate = defaultdict(lambda: {'pass': 0, 'on_hold': 0, 'fail': 0})

    for f in glob.glob(os.path.join(qc_batch_path, _QC_PORTAL_PAT)):
        try:
            df = pd.read_csv(f, dtype=str, header=None)
            # Handle two formats: with or without header row
            if len(df.columns) >= 2:
                first = str(df.iloc[0, 0]).strip().lower()
                if first in ('pid', '"pid"', 'label'):
                    df.columns = list(range(len(df.columns)))
                    df = df.iloc[1:].reset_index(drop=True)
                df.columns = list(range(len(df.columns)))
                pid_col = 0
                dec_col = 1
                for _, row in df.iterrows():
                    pid = str(row[pid_col]).strip().strip('"')
                    dec = str(row[dec_col]).strip().strip('"').upper()
                    if not pid or pid in ('nan', ''):
                        continue
                    # Derive plate from pid: strip well suffix
                    plate = re.sub(r'_[A-H]\d{1,2}$', '', pid)
                    plate = _normalise_plate(plate)
                    if not plate:
                        continue
                    if dec == 'YES':
                        per_plate[plate]['pass'] += 1
                    elif dec == 'ON_HOLD':
                        per_plate[plate]['on_hold'] += 1
                    else:
                        per_plate[plate]['fail'] += 1
        except Exception:
            pass

    result = {}
    for plate, d in per_plate.items():
        total = d['pass'] + d['on_hold'] + d['fail']
        result[plate] = {
            'pass':      d['pass'],
            'on_hold':   d['on_hold'],
            'fail':      d['fail'],
            'total_qc':  total,
            'pass_rate': round(100 * d['pass'] / total, 1) if total > 0 else None,
        }
    return result


# ── Portal expected counts ────────────────────────────────────────────────────

def load_portal_expected(portal_dump_path):
    """
    Returns dict: plate_id -> expected specimen count from portal dump.
    Counts rows per plate_id in the dump (each row = one specimen).
    """
    try:
        df = pd.read_csv(portal_dump_path, sep='\t', dtype=str,
                         usecols=['sts_rackid'], low_memory=False)
        df['plate_id'] = df['sts_rackid'].apply(_normalise_plate)
        counts = df['plate_id'].value_counts().to_dict()
        return counts
    except Exception as e:
        print(f"  Warning: could not load portal dump for expected counts: {e}")
        return {}


# ── Main analysis ─────────────────────────────────────────────────────────────

def run_analysis(mbrave_dir, qc_dir, portal_dump, exclude_bge=False,
                 threshold=POS_CTRL_THRESHOLD, verbose=False):

    # All batch folders in mBRAVE (including special so RnD/repeat runs are covered)
    import os as _os
    all_mbrave = sorted([
        d for d in _os.listdir(mbrave_dir)
        if _os.path.isdir(_os.path.join(mbrave_dir, d))
        and d.startswith('batch')
        and 'EXCLUDED' not in d
        and '_merged' not in d
        and 'PCR1_volume' not in d
    ])

    # QC folders — use resolve_batches with special to match above
    qc_resolved, _ = resolve_batches(qc_dir, include_special=True)
    # Index QC folders by normalised name for lookup
    qc_folder_set = set(qc_resolved)

    print(f"Scanning {len(all_mbrave)} mBRAVE batch folders...")

    portal_expected = load_portal_expected(portal_dump) if portal_dump else {}

    plate_rows = []   # one row per plate per batch

    for batch_folder in all_mbrave:
        batch_path = os.path.join(mbrave_dir, batch_folder)

        # Load positive controls from this mBRAVE batch
        pos_controls = load_pos_controls(batch_path)
        if not pos_controls:
            continue

        # Load UMI specimen counts
        umi_counts = load_umi_specimen_counts(batch_path)

        # Find matching QC folder (may be same name or the plain version)
        qc_folder = None
        if batch_folder in qc_folder_set:
            qc_folder = batch_folder
        else:
            # Try stripping split suffix: batch34_2 -> batch34
            base = re.sub(r'_\d+$', '', batch_folder)
            if base in qc_folder_set:
                qc_folder = base

        qc_pass_rates = {}
        if qc_folder:
            qc_batch_path = os.path.join(qc_dir, qc_folder)
            qc_pass_rates = load_qc_pass_rates(qc_batch_path)

        for entry in pos_controls:
            plate = entry['plate_id']
            if exclude_bge and is_bge_plate(plate):
                continue

            qc = qc_pass_rates.get(plate, {})
            umi_n = umi_counts.get(plate)
            portal_n = portal_expected.get(plate)

            plate_rows.append({
                'batch':               batch_folder,
                'plate_id':            plate,
                'pos_control_reads':   entry['pos_control_reads'],
                'n_specimens_umi':     umi_n,
                'n_specimens_portal':  portal_n,
                'n_pass':              qc.get('pass'),
                'n_on_hold':           qc.get('on_hold'),
                'n_fail':              qc.get('fail'),
                'n_total_qc':          qc.get('total_qc'),
                'pass_rate':           qc.get('pass_rate'),
            })

        if verbose:
            print(f"  {batch_folder}: {len(pos_controls)} plates with pos control data")

    if not plate_rows:
        print("No positive control data found.")
        return pd.DataFrame(), pd.DataFrame()

    df = pd.DataFrame(plate_rows)
    df['pos_control_reads'] = pd.to_numeric(df['pos_control_reads'], errors='coerce')
    df['pass_rate']         = pd.to_numeric(df['pass_rate'],         errors='coerce')

    # ── Per-batch statistics ──────────────────────────────────────────────────
    batch_stats = df.groupby('batch').agg(
        n_plates            = ('plate_id', 'count'),
        median_pos_ctrl     = ('pos_control_reads', 'median'),
        mean_pos_ctrl       = ('pos_control_reads', 'mean'),
        min_pos_ctrl        = ('pos_control_reads', 'min'),
        max_pos_ctrl        = ('pos_control_reads', 'max'),
        mean_pass_rate      = ('pass_rate', 'mean'),
        median_pass_rate    = ('pass_rate', 'median'),
    ).reset_index()

    # Flag batches in bottom 20% by mean pass rate (only batches with QC data)
    has_qc = batch_stats['mean_pass_rate'].notna()
    cutoff  = np.percentile(
        batch_stats.loc[has_qc, 'mean_pass_rate'].dropna(), POOR_BATCH_PERCENTILE
    ) if has_qc.sum() > 0 else None

    batch_stats['poor_batch_flag'] = False
    if cutoff is not None:
        batch_stats.loc[
            has_qc & (batch_stats['mean_pass_rate'] <= cutoff),
            'poor_batch_flag'
        ] = True
        print(f"  Poor-batch pass-rate cutoff (bottom {POOR_BATCH_PERCENTILE}%): "
              f"{cutoff:.1f}%")

    # ── Per-plate flagging ────────────────────────────────────────────────────
    batch_medians = batch_stats.set_index('batch')['median_pos_ctrl'].to_dict()
    poor_batches  = set(
        batch_stats.loc[batch_stats['poor_batch_flag'], 'batch']
    )

    df['batch_median_pos_ctrl'] = df['batch'].map(batch_medians)
    df['pos_ctrl_pct_of_median'] = (
        100 * df['pos_control_reads'] / df['batch_median_pos_ctrl']
    ).round(1)

    df['low_pos_ctrl_flag'] = (
        df['pos_control_reads'] < (df['batch_median_pos_ctrl'] * threshold)
    )
    df['poor_batch_flag'] = df['batch'].isin(poor_batches)
    df['resequence_candidate'] = df['low_pos_ctrl_flag'] | df['poor_batch_flag']

    # Round for readability
    df['batch_median_pos_ctrl'] = df['batch_median_pos_ctrl'].round(0)
    df['mean_pass_rate'] = df['batch'].map(
        batch_stats.set_index('batch')['mean_pass_rate'].to_dict()
    ).round(1)

    # Tidy column order
    col_order = [
        'batch', 'plate_id',
        'pos_control_reads', 'batch_median_pos_ctrl', 'pos_ctrl_pct_of_median',
        'low_pos_ctrl_flag', 'poor_batch_flag', 'resequence_candidate',
        'n_specimens_umi', 'n_specimens_portal',
        'n_pass', 'n_on_hold', 'n_fail', 'n_total_qc', 'pass_rate',
        'mean_pass_rate',
    ]
    df = df[[c for c in col_order if c in df.columns]]
    df = df.sort_values(['resequence_candidate', 'pos_ctrl_pct_of_median'],
                        ascending=[False, True]).reset_index(drop=True)

    batch_stats = batch_stats.sort_values('mean_pass_rate').reset_index(drop=True)

    n_flagged_plates  = int(df['low_pos_ctrl_flag'].sum())
    n_flagged_batches = int(batch_stats['poor_batch_flag'].sum())
    n_candidates      = int(df['resequence_candidate'].sum())

    print(f"\nPositive control analysis complete:")
    print(f"  Total plate-batch records : {len(df):,}")
    print(f"  Low pos-ctrl plates       : {n_flagged_plates:,} "
          f"(<{threshold*100:.0f}% of batch median)")
    print(f"  Poor batches              : {n_flagged_batches} "
          f"(bottom {POOR_BATCH_PERCENTILE}% by pass rate)")
    print(f"  Re-sequence candidates    : {n_candidates:,} unique plates")

    return df, batch_stats


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Analyse positive controls and flag re-sequencing candidates'
    )
    parser.add_argument('--exclude-bge', action='store_true')
    parser.add_argument('--threshold', type=float, default=POS_CTRL_THRESHOLD,
        help=f'Fraction of batch median below which pos-ctrl is "low" '
             f'(default {POS_CTRL_THRESHOLD})')
    parser.add_argument('--run-dir', default=None,
        help='Output directory (overrides BIOSCAN_RUN_DIR)')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    run_dir = resolve_run_dir(args.run_dir)
    os.makedirs(run_dir, exist_ok=True)

    print("=" * 60)
    print("Positive Control Analysis")
    print("=" * 60)
    print(f"  mBRAVE dir : {config.MBRAVE_DIR}")
    print(f"  QC dir     : {config.QC_DIR}")
    print(f"  Output dir : {run_dir}")
    print(f"  Threshold  : <{args.threshold*100:.0f}% of batch median")
    print(f"  Excl. BGE  : {args.exclude_bge}")
    print()

    df_plates, df_batches = run_analysis(
        mbrave_dir   = config.MBRAVE_DIR,
        qc_dir       = config.QC_DIR,
        portal_dump  = config.PORTAL_DUMP_TSV,
        exclude_bge  = args.exclude_bge,
        threshold    = args.threshold,
        verbose      = args.verbose,
    )

    if df_plates.empty:
        print("No data to save.")
        return

    plates_csv  = os.path.join(run_dir, f'positive_control_plates_{TODAY}.csv')
    batches_csv = os.path.join(run_dir, f'positive_control_batch_summary_{TODAY}.csv')

    df_plates.to_csv(plates_csv, index=False)
    df_batches.to_csv(batches_csv, index=False)

    print(f"\nOutputs written to: {run_dir}")
    print(f"  {os.path.basename(plates_csv)}")
    print(f"  {os.path.basename(batches_csv)}")


if __name__ == '__main__':
    main()
