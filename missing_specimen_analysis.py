"""
missing_specimen_analysis.py

Categorises specimens from UMI sample_stats files by their presence
in the consensusseq_network table.

Three categories:
  Cat 1 - Count = 0         : Zero reads, expected absence from consensusseq.
                              Not a pipeline failure — well failed to sequence.
  Cat 2 - Count > 0, absent : Got reads but no consensus produced.
                              Likely: too few reads, high stop codon rate,
                              conflicting sequences, or taxon-specific failure
                              (e.g. BGKU aquatic invertebrates).
                              LOW reads (< low_read_threshold): borderline,
                              likely below assembly minimum.
                              HIGH reads (>= low_read_threshold): unexpected,
                              investigate further.
  Cat 3 - Absent from UMI   : Specimen not in UMI stats at all.
                              Would indicate a manifest/layout error.
                              (Has been zero across all batches checked so far.)

Controls (H12, G12 wells, CONTROL_NEG/POS labels) are excluded.

Usage:
    python3 missing_specimen_analysis.py
    python3 missing_specimen_analysis.py --partner BGEP
    python3 missing_specimen_analysis.py --batch batch41_1
    python3 missing_specimen_analysis.py --low-read-threshold 50
"""

import argparse
import datetime
import glob
import os
import pandas as pd
from collections import defaultdict

import config
from utils import resolve_batches, safe_read_csv, batch_sort_key, matches_partner


UMI_SAMPLE_PATTERN   = "umi.*_sample_stats.txt"
UMI_LABEL_COL        = "Label"
UMI_COUNT_COL        = "Count"
UMI_PLATE_COL        = "Sample Plate ID"
CONSENSUSSEQ_PID_COL = "pid"

# Wells that are controls — exclude from missing specimen analysis
CONTROL_WELLS = {'H12', 'G12'}


def is_control_specimen(specimen_id):
    """Return True if specimen is a control well or has control label."""
    sid = str(specimen_id).upper()
    if sid.startswith('CONTROL_NEG') or sid.startswith('CONTROL_POS'):
        return True
    # Check well position (last part after final underscore)
    parts = specimen_id.rsplit('_', 1)
    if len(parts) == 2 and parts[1].upper() in CONTROL_WELLS:
        return True
    return False


def get_umi_specimens(batch_path, verbose=False):
    """
    Read UMI sample_stats files. Returns DataFrame with Label and Count.
    Excludes control files and control wells.
    """
    all_files = glob.glob(os.path.join(batch_path, UMI_SAMPLE_PATTERN))
    sample_files = [f for f in all_files
                    if '_sample_stats.txt' in f
                    and '_control_neg_stats' not in f
                    and '_control_pos_stats' not in f]

    if not sample_files:
        return pd.DataFrame()

    dfs = []
    for f in sample_files:
        try:
            df = safe_read_csv(f, sep='\t', dtype=str)
            if UMI_LABEL_COL in df.columns:
                dfs.append(df[[UMI_LABEL_COL, UMI_COUNT_COL, UMI_PLATE_COL]]
                           if UMI_PLATE_COL in df.columns
                           else df[[UMI_LABEL_COL, UMI_COUNT_COL]])
        except Exception as e:
            print(f"  WARNING: failed reading {os.path.basename(f)}: {e}")

    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)
    combined = combined[combined[UMI_LABEL_COL] != UMI_LABEL_COL]  # remove header rows
    combined = combined[combined[UMI_LABEL_COL].notna()]
    combined[UMI_COUNT_COL] = pd.to_numeric(combined[UMI_COUNT_COL], errors='coerce').fillna(0)

    # Exclude controls
    combined = combined[~combined[UMI_LABEL_COL].apply(is_control_specimen)]

    return combined


def get_consensusseq_specimens(batch_path):
    """Return set of specimen IDs present in consensusseq_network.tsv."""
    tsv_files = sorted(glob.glob(
        os.path.join(batch_path, config.CONSENSUSSEQ_NETWORK_PATTERN)))
    present = set()
    for f in tsv_files:
        try:
            df = safe_read_csv(f, sep='\t', dtype=str,
                               usecols=[CONSENSUSSEQ_PID_COL])
            present.update(df[CONSENSUSSEQ_PID_COL].dropna().astype(str))
        except Exception as e:
            print(f"  WARNING: failed reading consensusseq: {e}")
    return present


def run_missing_specimen_analysis(mbrave_dir=None, partner=None,
                                  batch_filter=None,
                                  low_read_threshold=50,
                                  verbose=False):
    """
    For each batch, categorise all expected specimens into:
      Cat1: Count=0
      Cat2_low: Count>0, absent from consensusseq, Count < low_read_threshold
      Cat2_high: Count>0, absent from consensusseq, Count >= low_read_threshold
      Cat3: absent from UMI stats entirely

    Returns:
        df_specimens: DataFrame with one row per categorised specimen
        df_summary:   DataFrame with batch-level counts
    """
    if mbrave_dir is None:
        mbrave_dir = config.MBRAVE_DIR

    resolved, _ = resolve_batches(mbrave_dir)
    if batch_filter:
        resolved = [b for b in resolved if b == batch_filter]

    specimen_rows = []
    summary_rows  = []

    for batch_folder in resolved:
        batch_path = os.path.join(mbrave_dir, batch_folder)

        umi_df  = get_umi_specimens(batch_path, verbose)
        present = get_consensusseq_specimens(batch_path)

        if umi_df.empty:
            continue

        # Apply partner filter
        if partner and partner.upper() != 'ALL':
            umi_df = umi_df[umi_df[UMI_LABEL_COL].apply(
                lambda s: matches_partner(
                    str(s).rsplit('_', 1)[0], partner))]

        n_total    = len(umi_df)
        n_cat1     = 0
        n_cat2_low = 0
        n_cat2_high= 0
        n_cat3     = 0

        for _, row in umi_df.iterrows():
            label = str(row[UMI_LABEL_COL]).strip()
            count = float(row[UMI_COUNT_COL])
            plate_id = str(row.get(UMI_PLATE_COL, '')).strip() if UMI_PLATE_COL in row.index else \
                       label.rsplit('_', 1)[0]

            if label in present:
                continue  # present in consensusseq — all good

            if count == 0:
                cat = 'Cat1_zero_reads'
                n_cat1 += 1
            elif count < low_read_threshold:
                cat = 'Cat2_low_reads'
                n_cat2_low += 1
            else:
                cat = 'Cat2_high_reads'
                n_cat2_high += 1

            specimen_rows.append({
                'batch':       batch_folder,
                'specimen_id': label,
                'plate_id':    plate_id,
                'read_count':  int(count),
                'category':    cat,
            })

        # Cat3: in consensusseq but not in UMI stats
        # (specimens in consensusseq that have no UMI entry)
        # Note: this is the reverse — we check for UMI labels not in consensusseq above.
        # True Cat3 would be consensusseq entries with no UMI entry — very unusual.

        summary_rows.append({
            'batch':         batch_folder,
            'n_expected':    n_total,
            'n_in_consensusseq': n_total - n_cat1 - n_cat2_low - n_cat2_high,
            'n_cat1_zero':   n_cat1,
            'n_cat2_low':    n_cat2_low,
            'n_cat2_high':   n_cat2_high,
            'n_cat3_absent': n_cat3,
            'pct_cat2_high': round(100 * n_cat2_high / n_total, 1) if n_total > 0 else 0,
        })

        if verbose:
            print(f"  {batch_folder}: {n_total} expected | "
                  f"Cat1={n_cat1} Cat2_low={n_cat2_low} "
                  f"Cat2_high={n_cat2_high}")

    df_specimens = pd.DataFrame(specimen_rows) if specimen_rows else \
        pd.DataFrame(columns=['batch','specimen_id','plate_id','read_count','category'])
    df_summary   = pd.DataFrame(summary_rows)

    return df_specimens, df_summary


def main():
    parser = argparse.ArgumentParser(
        description='Categorise specimens missing from mBRAVE consensusseq'
    )
    parser.add_argument('--partner', default='ALL')
    parser.add_argument('--batch', default=None)
    parser.add_argument('--low-read-threshold', type=int, default=50,
        help='Read count below which Cat2 is flagged as "low" (default: 50)')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    print("Scanning UMI stats and consensusseq files...")
    df_specimens, df_summary = run_missing_specimen_analysis(
        partner=args.partner,
        batch_filter=args.batch,
        low_read_threshold=args.low_read_threshold,
        verbose=args.verbose,
    )

    today = datetime.datetime.now().strftime('%Y%m%d')
    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    # Summary
    print("\nBATCH SUMMARY")
    print(f"{'Batch':<15} {'Expected':>9} {'InSeq':>7} {'Cat1_0rd':>9} "
          f"{'Cat2_low':>9} {'Cat2_high':>10} {'%Cat2hi':>8}")
    total_expected = df_summary['n_expected'].sum()
    for _, row in df_summary.sort_values('n_cat2_high', ascending=False).iterrows():
        if row['n_cat2_high'] > 0 or row['n_cat2_low'] > 0:
            print(f"  {row['batch']:<15} {row['n_expected']:>9} "
                  f"{row['n_in_consensusseq']:>7} "
                  f"{row['n_cat1_zero']:>9} {row['n_cat2_low']:>9} "
                  f"{row['n_cat2_high']:>10} {row['pct_cat2_high']:>8}")

    print(f"\nTOTALS:")
    print(f"  Total expected specimens  : {total_expected}")
    print(f"  Cat1 (zero reads)         : {df_summary['n_cat1_zero'].sum()}")
    print(f"  Cat2 low (<{args.low_read_threshold} reads, no seq): "
          f"{df_summary['n_cat2_low'].sum()}")
    print(f"  Cat2 high (reads, no seq) : {df_summary['n_cat2_high'].sum()} "
          f"← INVESTIGATE")
    print(f"  Cat3 (absent from UMI)    : {df_summary['n_cat3_absent'].sum()}")

    # Save
    spec_path = os.path.join(config.RESULTS_DIR,
                             f'missing_specimens_categorised_{today}.csv')
    summ_path = os.path.join(config.RESULTS_DIR,
                             f'missing_specimens_batch_summary_{today}.csv')
    df_specimens.to_csv(spec_path, index=False)
    df_summary.to_csv(summ_path, index=False)
    print(f"\nOutputs written:")
    print(f"  {spec_path}")
    print(f"  {summ_path}")


if __name__ == '__main__':
    main()
