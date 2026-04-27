"""
repeat_analysis_specimens.py

Specimen-level repeat analysis. For every specimen that appears in more
than one sequencing batch, shows the QC decision (PASS/ON_HOLD/FAILED)
and description in each batch — allowing you to track improvement or
decline across repeat sequencings.

Source: qc_portal_batchN.csv files — contains PASS, ON_HOLD and FAILED
for all specimens. Two formats handled:
  Older batches: header row "pid,category_decision,description" (quoted)
  Newer batches: no header, unquoted (pid is first field)

Output:
  repeat_specimens_wide_YYYYMMDD.csv  - one row per specimen, one column
                                        per batch (wide format, easy to read)
  repeat_specimens_long_YYYYMMDD.csv  - one row per specimen per batch
                                        (long format, easier to analyse)
  repeat_specimens_summary_YYYYMMDD.csv - summary per specimen showing
                                          first/last/best decision and
                                          whether outcome improved

Usage:
    python3 repeat_analysis_specimens.py
    python3 repeat_analysis_specimens.py --partner BGEP
    python3 repeat_analysis_specimens.py --min-appearances 2
    python3 repeat_analysis_specimens.py --decision-filter FAILED
"""

import argparse
import datetime
import glob
import os
import re
import pandas as pd
from collections import defaultdict

import config
from utils import (build_batch_cross_map, resolve_batches,
                   batch_sort_key, matches_partner, safe_read_csv)


# Decision ranking — higher = better
_DECISION_RANK = {'PASS': 3, 'ON_HOLD': 2, 'FAILED': 1, 'UNKNOWN': 0}
_PID_RE = re.compile(r'^[A-Z]')  # first field looks like a pid = no header


def read_qc_portal(batch_folder, batch_path, verbose=False):
    """
    Read qc_portal file handling both header and headerless formats.
    Returns DataFrame with columns: pid, decision, description, batch
    """
    files = glob.glob(os.path.join(batch_path, 'qc_portal_batch*.csv'))
    if not files:
        if verbose:
            print(f"  {batch_folder}: no qc_portal file")
        return pd.DataFrame()

    qc_file = files[0]
    try:
        # Peek at first value to detect format
        peek = safe_read_csv(qc_file, nrows=1, header=None, dtype=str)
        first_val = str(peek.iloc[0, 0]).strip().strip('"')

        if _PID_RE.match(first_val) and first_val.lower() != 'pid':
            # No header
            df = safe_read_csv(qc_file, header=None, dtype=str)
            df = df.iloc[:, :3].copy()
            df.columns = ['pid', 'decision', 'description']
        else:
            # Has header
            df = safe_read_csv(qc_file, dtype=str)
            # Handle quoted column names
            df.columns = [c.strip().strip('"') for c in df.columns]
            df = df.rename(columns={
                'category_decision': 'decision',
                'category_explanation': 'description',
            })
            df = df[['pid', 'decision', 'description']].copy()

        # Clean
        for col in df.columns:
            df[col] = df[col].str.strip().str.strip('"')

        df['batch'] = batch_folder
        df = df[df['pid'].notna() & (df['pid'] != 'pid')]

        if verbose:
            counts = df['decision'].value_counts().to_dict()
            print(f"  {batch_folder}: {len(df)} specimens "
                  f"PASS={counts.get('PASS',0)} "
                  f"ON_HOLD={counts.get('ON_HOLD',0)} "
                  f"FAILED={counts.get('FAILED',0)}")
        return df

    except Exception as e:
        print(f"  WARNING: {batch_folder}: {e}")
        return pd.DataFrame()


def load_all_qc_portal(qc_dir=None, partner=None, verbose=False):
    """
    Load qc_portal data from all resolved QC batches.
    Returns long-format DataFrame: pid | decision | description | batch
    """
    if qc_dir is None:
        qc_dir = config.QC_DIR

    (mbrave_to_qc, qc_to_mbrave, issues,
     mbrave_resolved, qc_resolved,
     mbrave_skipped, qc_skipped) = build_batch_cross_map(config.MBRAVE_DIR, qc_dir)

    all_dfs = []
    for qc_folder in qc_resolved:
        batch_path = os.path.join(qc_dir, qc_folder)
        df = read_qc_portal(qc_folder, batch_path, verbose=verbose)
        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)

    # Extract plate_id from pid
    combined['plate_id'] = combined['pid'].apply(
        lambda s: re.sub(r'_[A-H]\d{1,2}$', '', str(s)) if pd.notna(s) else None)

    # Apply partner filter
    if partner and partner.upper() != 'ALL':
        combined = combined[combined['plate_id'].apply(
            lambda p: matches_partner(str(p), partner) if p else False)]
        print(f"  Filtered to partner '{partner}': {len(combined)} specimen-batch rows")

    return combined


def build_repeat_tables(df, min_appearances=2):
    """
    Build wide and long repeat tables for specimens appearing in
    >= min_appearances batches.

    Wide format: one row per specimen, columns per batch showing decision
    Long format: one row per specimen-batch
    Summary: one row per specimen with first/last/best decision
    """
    # Count how many batches each specimen appears in
    specimen_batch_counts = df.groupby('pid')['batch'].nunique()
    repeated_pids = specimen_batch_counts[
        specimen_batch_counts >= min_appearances].index

    print(f"\nSpecimens appearing in >= {min_appearances} batches: "
          f"{len(repeated_pids)}")

    if len(repeated_pids) == 0:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    repeated = df[df['pid'].isin(repeated_pids)].copy()

    # Sort batches numerically
    repeated['batch_sort'] = repeated['batch'].apply(batch_sort_key)
    repeated = repeated.sort_values(['pid', 'batch_sort'])

    # ── Long format ───────────────────────────────────────────────────────────
    long_df = repeated[['pid', 'plate_id', 'batch', 'decision',
                         'description']].copy()

    # ── Summary per specimen ──────────────────────────────────────────────────
    summary_rows = []
    for pid, grp in repeated.groupby('pid'):
        grp = grp.sort_values('batch_sort')
        batches     = grp['batch'].tolist()
        decisions   = grp['decision'].tolist()
        first_dec   = decisions[0]
        last_dec    = decisions[-1]
        best_dec    = max(decisions,
                         key=lambda d: _DECISION_RANK.get(d, 0))
        # Did outcome improve from first to last?
        improved = (_DECISION_RANK.get(last_dec, 0) >
                    _DECISION_RANK.get(first_dec, 0))
        declined = (_DECISION_RANK.get(last_dec, 0) <
                    _DECISION_RANK.get(first_dec, 0))

        summary_rows.append({
            'pid':            pid,
            'plate_id':       grp['plate_id'].iloc[0],
            'n_batches':      len(batches),
            'batches':        ','.join(batches),
            'decisions':      ','.join(decisions),
            'first_batch':    batches[0],
            'first_decision': first_dec,
            'last_batch':     batches[-1],
            'last_decision':  last_dec,
            'best_decision':  best_dec,
            'improved':       improved,
            'declined':       declined,
            'ever_passed':    'PASS' in decisions,
            'ever_failed':    'FAILED' in decisions,
            'ever_on_hold':   'ON_HOLD' in decisions,
        })

    summary_df = pd.DataFrame(summary_rows)

    # ── Wide format ───────────────────────────────────────────────────────────
    # One column per batch showing decision
    all_batches = sorted(repeated['batch'].unique(), key=batch_sort_key)
    wide_rows   = []
    for pid, grp in repeated.groupby('pid'):
        grp = grp.sort_values('batch_sort')
        row = {
            'pid':      pid,
            'plate_id': grp['plate_id'].iloc[0],
            'n_batches': grp['batch'].nunique(),
        }
        for batch in all_batches:
            batch_rows = grp[grp['batch'] == batch]
            if len(batch_rows) > 0:
                row[f'decision_{batch}'] = batch_rows['decision'].iloc[0]
                row[f'description_{batch}'] = batch_rows['description'].iloc[0]
            else:
                row[f'decision_{batch}'] = ''
                row[f'description_{batch}'] = ''
        wide_rows.append(row)

    wide_df = pd.DataFrame(wide_rows)

    return long_df, wide_df, summary_df


def print_summary(summary_df, df):
    """Print summary statistics."""
    print("\n" + "=" * 60)
    print("SPECIMEN-LEVEL REPEAT ANALYSIS SUMMARY")
    print("=" * 60)

    n = len(summary_df)
    print(f"\nTotal repeated specimens   : {n}")
    print(f"  Improved (FAIL/HOLD→PASS): {summary_df['improved'].sum()}")
    print(f"  Declined                 : {summary_df['declined'].sum()}")
    print(f"  No change                : {n - summary_df['improved'].sum() - summary_df['declined'].sum()}")
    print(f"  Ever passed              : {summary_df['ever_passed'].sum()}")
    print(f"  Ever failed              : {summary_df['ever_failed'].sum()}")
    print(f"  Ever on hold             : {summary_df['ever_on_hold'].sum()}")

    print(f"\nFirst decision distribution:")
    print(summary_df['first_decision'].value_counts().to_string())
    print(f"\nLast decision distribution:")
    print(summary_df['last_decision'].value_counts().to_string())

    print(f"\nTransition matrix (first → last decision):")
    transitions = summary_df.groupby(
        ['first_decision', 'last_decision']).size().reset_index(name='n')
    for _, row in transitions.sort_values('n', ascending=False).iterrows():
        print(f"  {row['first_decision']:<10} → {row['last_decision']:<10}: {int(row['n'])}")


def main():
    parser = argparse.ArgumentParser(
        description='Specimen-level repeat analysis with QC decisions per batch'
    )
    parser.add_argument('--partner', default='ALL')
    parser.add_argument('--min-appearances', type=int, default=2,
        help='Minimum number of batches a specimen must appear in (default: 2)')
    parser.add_argument('--decision-filter', default=None,
        choices=['PASS', 'ON_HOLD', 'FAILED'],
        help='Only include specimens with this decision in at least one batch')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    today = datetime.datetime.now().strftime('%Y%m%d')
    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    print("=" * 60)
    print("Loading QC portal data from all batches...")
    print("=" * 60)
    df = load_all_qc_portal(partner=args.partner, verbose=args.verbose)

    if df.empty:
        print("No QC portal data found.")
        return

    print(f"\nTotal specimen-batch records: {len(df)}")
    print(f"Unique specimens: {df['pid'].nunique()}")
    print(f"Decision breakdown:")
    print(df['decision'].value_counts().to_string())

    # Optional filter
    if args.decision_filter:
        filter_pids = df[df['decision'] == args.decision_filter]['pid'].unique()
        df = df[df['pid'].isin(filter_pids)]
        print(f"\nFiltered to specimens with at least one {args.decision_filter}: "
              f"{df['pid'].nunique()} specimens")

    long_df, wide_df, summary_df = build_repeat_tables(
        df, min_appearances=args.min_appearances)

    if summary_df.empty:
        print("No repeated specimens found.")
        return

    print_summary(summary_df, df)

    # Save outputs
    long_path    = os.path.join(config.RESULTS_DIR,
                                f'repeat_specimens_long_{today}.csv')
    wide_path    = os.path.join(config.RESULTS_DIR,
                                f'repeat_specimens_wide_{today}.csv')
    summary_path = os.path.join(config.RESULTS_DIR,
                                f'repeat_specimens_summary_{today}.csv')

    # Save transition matrix
    transitions = summary_df.groupby(
        ['first_decision', 'last_decision']).size().reset_index(name='n')
    transitions['pct_of_total'] = (
        100 * transitions['n'] / len(summary_df)).round(1)
    transitions = transitions.sort_values('n', ascending=False)
    transition_path = os.path.join(config.RESULTS_DIR,
                                   f'repeat_specimens_transitions_{today}.csv')
    transitions.to_csv(transition_path, index=False)

    long_df.to_csv(long_path, index=False)
    wide_df.to_csv(wide_path, index=False)
    summary_df.to_csv(summary_path, index=False)

    print(f"\nOutputs written:")
    print(f"  {summary_path}     <- one row per specimen, trajectory summary")
    print(f"  {transition_path}  <- transition matrix (first → last decision)")
    print(f"  {long_path}        <- one row per specimen per batch")
    print(f"  {wide_path}        <- one row per specimen, decisions as columns")


if __name__ == '__main__':
    main()
