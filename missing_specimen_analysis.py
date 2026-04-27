"""
missing_specimen_analysis.py

Identifies specimens that are expected in a sequencing run (present in the
UMI sample_stats file) but entirely absent from the consensusseq_network
table — meaning they were never demultiplexed from the FASTQ file.

These are distinct from specimens with zero reads (which would appear with
low counts) — these specimens are completely missing from the output.

UMI stats file pattern : umi.*_sample_stats.txt  (NOT control_neg/pos)
Consensusseq file      : *consensusseq_network.tsv

Usage:
    python3 missing_specimen_analysis.py
    python3 missing_specimen_analysis.py --partner BGEP
    python3 missing_specimen_analysis.py --batch batch30
"""

import argparse
import datetime
import glob
import os
import pandas as pd
from collections import defaultdict

import config
from utils import resolve_batches, find_file_in_batch, safe_read_csv, batch_sort_key, matches_partner


UMI_SAMPLE_PATTERN     = "umi.*_sample_stats.txt"
UMI_LABEL_COL          = "Label"
UMI_PLATE_COL          = "Sample Plate ID"
CONSENSUSSEQ_PID_COL   = "pid"


def get_umi_specimens(batch_path, batch_folder, verbose=False):
    """
    Read UMI sample_stats files for a batch.
    Returns set of specimen IDs (Label column) expected in this run.
    Excludes control_neg and control_pos files.
    """
    all_files = glob.glob(os.path.join(batch_path, UMI_SAMPLE_PATTERN))
    # Keep only sample_stats, not control files
    sample_files = [f for f in all_files
                    if '_sample_stats.txt' in f
                    and '_control_neg_stats' not in f
                    and '_control_pos_stats' not in f]

    if not sample_files:
        if verbose:
            print(f"  {batch_folder}: no UMI sample_stats files found")
        return set(), set()

    expected = set()
    plates = set()
    for f in sample_files:
        try:
            df = safe_read_csv(f, sep='\t', dtype=str)
            if UMI_LABEL_COL not in df.columns:
                print(f"  WARNING: '{UMI_LABEL_COL}' not in {f}")
                continue
            for label in df[UMI_LABEL_COL].dropna():
                label = str(label).strip()
                if label and label != 'Label':
                    expected.add(label)
            if UMI_PLATE_COL in df.columns:
                for plate in df[UMI_PLATE_COL].dropna():
                    plates.add(str(plate).strip())
        except Exception as e:
            print(f"  WARNING: failed reading {f}: {e}")

    if verbose:
        print(f"  {batch_folder}: {len(expected)} expected specimens "
              f"from {len(sample_files)} UMI file(s)")
    return expected, plates


def get_consensusseq_specimens(batch_path, batch_folder, verbose=False):
    """
    Read consensusseq_network.tsv and return set of specimen IDs present.
    """
    tsv_files = sorted(glob.glob(os.path.join(batch_path,
                                               config.CONSENSUSSEQ_NETWORK_PATTERN)))
    if not tsv_files:
        if verbose:
            print(f"  {batch_folder}: no consensusseq_network.tsv found")
        return set()

    present = set()
    for f in tsv_files:
        try:
            df = safe_read_csv(f, sep='\t', dtype=str,
                               usecols=[CONSENSUSSEQ_PID_COL])
            for pid in df[CONSENSUSSEQ_PID_COL].dropna():
                present.add(str(pid).strip())
        except Exception as e:
            print(f"  WARNING: failed reading {f}: {e}")

    if verbose:
        print(f"  {batch_folder}: {len(present)} specimens in consensusseq")
    return present


def run_missing_specimen_analysis(mbrave_dir=None, partner=None,
                                  batch_filter=None, verbose=False):
    """
    For each batch, compare UMI expected specimens vs consensusseq present.
    Returns DataFrame of missing specimens.
    """
    if mbrave_dir is None:
        mbrave_dir = config.MBRAVE_DIR

    resolved, skipped = resolve_batches(mbrave_dir)
    if batch_filter:
        resolved = [b for b in resolved if b == batch_filter]

    rows = []
    batch_summaries = []

    for batch_folder in resolved:
        batch_path = os.path.join(mbrave_dir, batch_folder)

        expected, plates = get_umi_specimens(batch_path, batch_folder, verbose)
        present = get_consensusseq_specimens(batch_path, batch_folder, verbose)

        if not expected:
            continue

        missing = expected - present

        # Filter by partner if specified
        if partner and partner.upper() != 'ALL':
            missing = {s for s in missing if matches_partner(
                '_'.join(s.split('_')[:-1]), partner)}
            expected = {s for s in expected if matches_partner(
                '_'.join(s.split('_')[:-1]), partner)}

        n_expected = len(expected)
        n_present  = len(expected & present)
        n_missing  = len(missing)
        pct_missing = 100 * n_missing / n_expected if n_expected > 0 else 0

        batch_summaries.append({
            'batch':      batch_folder,
            'n_expected': n_expected,
            'n_present':  n_present,
            'n_missing':  n_missing,
            'pct_missing': round(pct_missing, 1),
        })

        for specimen_id in sorted(missing):
            # Extract plate from specimen ID
            parts = specimen_id.rsplit('_', 1)
            plate_id = parts[0] if len(parts) == 2 else specimen_id
            rows.append({
                'batch':       batch_folder,
                'specimen_id': specimen_id,
                'plate_id':    plate_id,
            })

        if verbose and n_missing > 0:
            print(f"  {batch_folder}: {n_missing}/{n_expected} specimens "
                  f"missing from consensusseq ({pct_missing:.1f}%)")

    df_missing   = pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=['batch', 'specimen_id', 'plate_id'])
    df_summaries = pd.DataFrame(batch_summaries)

    return df_missing, df_summaries


def main():
    parser = argparse.ArgumentParser(
        description='Find specimens missing from mBRAVE consensusseq'
    )
    parser.add_argument('--partner', default='ALL')
    parser.add_argument('--batch', default=None,
        help='Restrict to a single batch folder (e.g. batch30)')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    print("Scanning UMI stats and consensusseq files...")
    df_missing, df_summaries = run_missing_specimen_analysis(
        partner=args.partner,
        batch_filter=args.batch,
        verbose=args.verbose,
    )

    today = datetime.datetime.now().strftime('%Y%m%d')
    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    # Batch summary
    print("\nBatch summary (batches with missing specimens):")
    has_missing = df_summaries[df_summaries['n_missing'] > 0]
    print(has_missing.sort_values('pct_missing', ascending=False)
          .to_string(index=False))

    # Overall
    total_missing = len(df_missing)
    total_expected = df_summaries['n_expected'].sum()
    print(f"\nTotal missing specimens: {total_missing} "
          f"of {total_expected} expected "
          f"({100*total_missing//total_expected if total_expected else 0}%)")

    # Plates with missing specimens
    if total_missing > 0:
        plate_counts = df_missing.groupby(['plate_id', 'batch']).size().reset_index(
            name='n_missing_specimens')
        print(f"\nPlates with missing specimens: "
              f"{plate_counts['plate_id'].nunique()}")

    # Save outputs
    csv_path = os.path.join(config.RESULTS_DIR,
                            f'missing_specimens_{today}.csv')
    summary_path = os.path.join(config.RESULTS_DIR,
                                f'missing_specimens_batch_summary_{today}.csv')
    df_missing.to_csv(csv_path, index=False)
    df_summaries.to_csv(summary_path, index=False)
    print(f"\nOutputs written:")
    print(f"  {csv_path}")
    print(f"  {summary_path}")


if __name__ == '__main__':
    main()
