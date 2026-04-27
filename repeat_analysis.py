"""
repeat_analysis.py

Analyses plates that were sequenced more than once across BIOSCAN batches.

For each repeated plate:
  - Lists all batches it appeared in
  - Compares pass rates between earliest and latest sequencing
  - Identifies which batch was used for BOLD upload (from portal dump)
  - Flags cases where the BOLD batch differs from the best QC batch

Usage:
    python3 repeat_analysis.py
    python3 repeat_analysis.py --partner BGEP
    python3 repeat_analysis.py --min-sequencings 2
"""

import argparse
import datetime
import os
import pandas as pd
import glob

import config
from utils import resolve_batches, batch_sort_key, safe_read_csv, matches_partner, extract_plate_from_pid


_PLATE_COL    = 'Sample.Plate.ID'
_PID_COL      = 'pid'
_DECISION_COL = 'category_decision'
_DECISION_MAP = {'YES': 'PASS', 'NO': 'FAIL', 'ON_HOLD': 'ON_HOLD'}


def get_plate_qc_per_batch(qc_dir, resolved_batches, partner=None, verbose=False):
    """
    For each plate in each QC batch, get per-specimen pass counts.
    Returns DataFrame: plate_id | qc_batch | n_pass | n_fail | n_onhold | n_total | pct_pass
    """
    rows = []
    from utils import build_batch_cross_map
    (mbrave_to_qc, qc_to_mbrave, issues,
     mbrave_resolved, qc_resolved,
     mbrave_skipped, qc_skipped) = build_batch_cross_map(config.MBRAVE_DIR, qc_dir)

    for qc_folder in qc_resolved:
        batch_path = os.path.join(qc_dir, qc_folder)
        meta_files = glob.glob(os.path.join(batch_path, 'filtered_metadata_batch*.csv'))
        if not meta_files:
            continue
        try:
            df = safe_read_csv(meta_files[0], dtype=str,
                               usecols=[_PLATE_COL, _PID_COL, _DECISION_COL])
        except Exception:
            try:
                df = safe_read_csv(meta_files[0], dtype=str)
                if _PLATE_COL not in df.columns:
                    continue
            except Exception as e:
                print(f"  WARNING: {qc_folder}: {e}")
                continue

        df['plate_id'] = df[_PLATE_COL].str.replace('_NA$', '', regex=True)
        df['status']   = df[_DECISION_COL].str.strip().map(_DECISION_MAP).fillna('UNKNOWN')

        if partner and partner.upper() != 'ALL':
            df = df[df['plate_id'].apply(
                lambda p: matches_partner(str(p), partner) if p else False)]

        for plate_id, grp in df.groupby('plate_id'):
            counts = grp['status'].value_counts().to_dict()
            n_pass  = counts.get('PASS', 0)
            n_total = len(grp)
            rows.append({
                'plate_id':  plate_id,
                'qc_batch':  qc_folder,
                'n_pass':    n_pass,
                'n_fail':    counts.get('FAIL', 0),
                'n_onhold':  counts.get('ON_HOLD', 0),
                'n_total':   n_total,
                'pct_pass':  round(100 * n_pass / n_total, 1) if n_total > 0 else 0,
            })

    return pd.DataFrame(rows)


def run_repeat_analysis(qc_dir=None, partner=None,
                        min_sequencings=2, verbose=False):
    """
    Build repeat analysis table for plates sequenced >= min_sequencings times.
    Returns DataFrame with one row per repeated plate.
    """
    if qc_dir is None:
        qc_dir = config.QC_DIR

    print("Loading QC data per batch...")
    df_qc = get_plate_qc_per_batch(qc_dir,
                                    resolve_batches(qc_dir)[0],
                                    partner=partner,
                                    verbose=verbose)

    if df_qc.empty:
        print("No QC data found.")
        return pd.DataFrame()

    # Find repeated plates
    plate_batch_counts = df_qc.groupby('plate_id')['qc_batch'].nunique()
    repeated_plates = plate_batch_counts[
        plate_batch_counts >= min_sequencings].index.tolist()

    print(f"Found {len(repeated_plates)} plates sequenced >= "
          f"{min_sequencings} times")

    if not repeated_plates:
        return pd.DataFrame()

    # Load portal dump for BOLD status
    portal_index = {}
    try:
        from read_portal_dump import load_portal_plate_summary
        portal_df = load_portal_plate_summary(config.PORTAL_PLATES_CSV)
        portal_index = portal_df.set_index('plate_id').to_dict('index')
    except Exception as e:
        print(f"  WARNING: could not load portal data: {e}")

    rows = []
    for plate_id in sorted(repeated_plates):
        plate_data = df_qc[df_qc['plate_id'] == plate_id].copy()
        plate_data = plate_data.sort_values('qc_batch', key=lambda x: x.map(batch_sort_key))

        batches      = plate_data['qc_batch'].tolist()
        pct_passes   = plate_data['pct_pass'].tolist()
        first_batch  = batches[0]
        last_batch   = batches[-1]
        first_pct    = pct_passes[0]
        last_pct     = pct_passes[-1]
        best_batch   = plate_data.loc[plate_data['pct_pass'].idxmax(), 'qc_batch']
        best_pct     = plate_data['pct_pass'].max()
        improvement  = round(last_pct - first_pct, 1)

        portal_info  = portal_index.get(plate_id, {})
        bold_uploaded = portal_info.get('bold_uploaded', False)
        submit_date   = portal_info.get('submit_date')
        partner_code  = portal_info.get('partner')

        rows.append({
            'plate_id':        plate_id,
            'partner':         partner_code,
            'submit_date':     submit_date,
            'n_sequencings':   len(batches),
            'batches':         ','.join(batches),
            'first_batch':     first_batch,
            'last_batch':      last_batch,
            'first_pct_pass':  first_pct,
            'last_pct_pass':   last_pct,
            'best_batch':      best_batch,
            'best_pct_pass':   best_pct,
            'improvement':     improvement,
            'bold_uploaded':   bold_uploaded,
        })

    return pd.DataFrame(rows)


def print_repeat_summary(df):
    if df.empty:
        print("No repeated plates found.")
        return

    print(f"\nREPEAT ANALYSIS SUMMARY")
    print(f"=" * 60)
    print(f"Total repeated plates    : {len(df)}")
    print(f"Plates with improvement  : {(df['improvement'] > 0).sum()}")
    print(f"Plates with decline      : {(df['improvement'] < 0).sum()}")
    print(f"Avg improvement          : {df['improvement'].mean():.1f}%")
    print(f"Max improvement          : {df['improvement'].max():.1f}%")
    print(f"Uploaded to BOLD         : {df['bold_uploaded'].sum()}")

    if 'partner' in df.columns:
        print(f"\nBy partner:")
        for partner, grp in df.groupby('partner'):
            print(f"  {partner:<8}: {len(grp)} plates, "
                  f"avg improvement={grp['improvement'].mean():.1f}%")

    print(f"\nTop 10 most improved plates:")
    top = df.nlargest(10, 'improvement')[
        ['plate_id', 'partner', 'n_sequencings',
         'first_pct_pass', 'last_pct_pass', 'improvement', 'batches']]
    print(top.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(
        description='Repeat sequencing analysis for BIOSCAN plates'
    )
    parser.add_argument('--partner', default='ALL')
    parser.add_argument('--min-sequencings', type=int, default=2)
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    df = run_repeat_analysis(partner=args.partner,
                             min_sequencings=args.min_sequencings,
                             verbose=args.verbose)
    print_repeat_summary(df)

    today = datetime.datetime.now().strftime('%Y%m%d')
    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    csv_path  = os.path.join(config.RESULTS_DIR,
                             f'repeat_analysis_{today}.csv')
    xlsx_path = os.path.join(config.RESULTS_DIR,
                             f'repeat_analysis_{today}.xlsx')
    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)
    print(f"\nOutputs written:")
    print(f"  {csv_path}")
    print(f"  {xlsx_path}")


if __name__ == '__main__':
    main()
