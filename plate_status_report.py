"""
plate_status_report.py  —  MAIN ENTRY POINT

Builds a master plate status table joining:
  - mBRAVE coverage   (mbrave_checker)
  - QC coverage       (qc_checker)
  - Portal presence   (portal_query)
  - BOLD upload       (portal bold_nuc field)

Usage:
    python plate_status_report.py --partner ALL
    python plate_status_report.py --partner BGEP
    python plate_status_report.py --partner ALL --skip-portal
    python plate_status_report.py --partner ALL --missing-only
"""

import argparse
import datetime
import os
import sys
import pandas as pd

import config
from utils import resolve_batches, batch_sort_key
from mbrave_checker import build_mbrave_plate_index, summarise_mbrave
from qc_checker import build_qc_plate_index, summarise_qc


def build_master_table(partner='ALL', skip_portal=False, verbose=False):
    """
    Build the master plate status DataFrame.

    Columns:
        plate_id          | str
        partner           | str  (from portal; None if skip_portal)
        mbrave_status     | FOUND / MISSING
        mbrave_batches    | comma-separated batch list
        n_sequencings     | int  (number of mBRAVE batches)
        qc_status         | FOUND / MISSING
        qc_batches        | comma-separated batch list
        best_qc_result    | PASS / ON_HOLD / FAIL / UNKNOWN / MISSING
        portal_status     | FOUND / MISSING / SKIPPED
        bold_status       | HAS_DATA / NO_DATA / UNKNOWN
        pipeline_stage    | furthest stage reached
        missing_at        | stage where plate dropped out (or None)
    """

    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    # ── 1. mBRAVE ─────────────────────────────────────────────────────────────
    print("=" * 60)
    print("Step 1: Scanning mBRAVE data...")
    print("=" * 60)
    plate_to_mbrave_batches, mbrave_resolved, mbrave_skipped = build_mbrave_plate_index(
        partner=partner, verbose=verbose
    )
    summarise_mbrave(plate_to_mbrave_batches)

    # ── 2. QC ─────────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Step 2: Scanning QC reports...")
    print("=" * 60)
    plate_qc_summary, qc_resolved, qc_skipped, batches_missing_qc = build_qc_plate_index(
        partner=partner, verbose=verbose
    )
    summarise_qc(plate_qc_summary, batches_missing_qc)

    # ── 3. Union all plate IDs ────────────────────────────────────────────────
    all_plates = sorted(
        set(plate_to_mbrave_batches.keys()) | set(plate_qc_summary.keys())
    )
    print(f"\nTotal unique plates across mBRAVE + QC: {len(all_plates)}")

    # ── 4. Portal query ───────────────────────────────────────────────────────
    portal_summary = {}
    portal_df = pd.DataFrame()

    if not skip_portal:
        print("\n" + "=" * 60)
        print("Step 3: Querying ToL Portal...")
        print("=" * 60)
        try:
            from portal_query import get_portal_plate_summary
            portal_summary, portal_df = get_portal_plate_summary(
                all_plates, verbose=verbose
            )
            n_found = sum(1 for v in portal_summary.values() if v['portal_found'])
            print(f"  Portal: {n_found}/{len(all_plates)} plates found")
        except Exception as e:
            print(f"  ERROR querying portal: {e}")
            print("  Continuing without portal data (use --skip-portal to suppress this)")
    else:
        print("\nStep 3: Portal query SKIPPED (--skip-portal flag)")

    # ── 5. Build rows ─────────────────────────────────────────────────────────
    rows = []
    for plate_id in all_plates:
        mbrave_batches = plate_to_mbrave_batches.get(plate_id, [])
        qc_info = plate_qc_summary.get(plate_id, None)
        portal_info = portal_summary.get(plate_id, None)

        mbrave_status = 'FOUND' if mbrave_batches else 'MISSING'
        qc_status     = 'FOUND' if qc_info else 'MISSING'

        best_qc = qc_info['best_status'] if qc_info else 'MISSING'

        if skip_portal or portal_info is None:
            portal_status = 'SKIPPED'
            partner_code  = None
            bold_status   = 'UNKNOWN'
        else:
            portal_status = 'FOUND' if portal_info['portal_found'] else 'MISSING'
            partner_code  = portal_info.get('partner')
            bold_status   = 'HAS_DATA' if portal_info['bold_uploaded'] else 'NO_DATA'

        # Pipeline stage (furthest point reached)
        # Order: portal → mbrave → qc → bold
        stages_reached = []
        if portal_status == 'FOUND':
            stages_reached.append('portal')
        if mbrave_status == 'FOUND':
            stages_reached.append('mbrave')
        if qc_status == 'FOUND':
            stages_reached.append('qc')
        if bold_status == 'HAS_DATA':
            stages_reached.append('bold')

        pipeline_stage = stages_reached[-1] if stages_reached else 'none'

        # Where did it drop out?
        all_stages = ['portal', 'mbrave', 'qc', 'bold']
        missing_at = None
        if not skip_portal:
            for stage in all_stages:
                if stage not in stages_reached:
                    # Only flag as missing_at if a prior stage was reached
                    prior_idx = all_stages.index(stage) - 1
                    if prior_idx >= 0 and all_stages[prior_idx] in stages_reached:
                        missing_at = stage
                        break

        rows.append({
            'plate_id':       plate_id,
            'partner':        partner_code,
            'mbrave_status':  mbrave_status,
            'mbrave_batches': ','.join(mbrave_batches),
            'n_sequencings':  len(mbrave_batches),
            'qc_status':      qc_status,
            'qc_batches':     ','.join(qc_info['batches']) if qc_info else '',
            'best_qc_result': best_qc,
            'portal_status':  portal_status,
            'bold_status':    bold_status,
            'pipeline_stage': pipeline_stage,
            'missing_at':     missing_at,
        })

    df = pd.DataFrame(rows)
    return df, mbrave_skipped, qc_skipped, batches_missing_qc


def print_missing_summary(df):
    print("\n" + "=" * 60)
    print("MISSING PLATE SUMMARY")
    print("=" * 60)

    if 'missing_at' in df.columns:
        for stage in ['mbrave', 'qc', 'bold']:
            missing = df[df['missing_at'] == stage]
            if len(missing) > 0:
                print(f"\nDropped out at '{stage}' ({len(missing)} plates):")
                for _, row in missing.iterrows():
                    print(f"  {row['plate_id']}"
                          f"  [partner={row.get('partner','?')}]"
                          f"  mbrave={row['mbrave_status']}"
                          f"  qc={row['qc_status']}")

    # Plates in QC but not in mBRAVE (unusual)
    qc_no_mbrave = df[(df['qc_status'] == 'FOUND') & (df['mbrave_status'] == 'MISSING')]
    if len(qc_no_mbrave) > 0:
        print(f"\nIn QC but NOT in mBRAVE ({len(qc_no_mbrave)} plates) — investigate:")
        for _, row in qc_no_mbrave.iterrows():
            print(f"  {row['plate_id']}  qc_batches={row['qc_batches']}")


def save_outputs(df, partner, results_dir=None):
    if results_dir is None:
        results_dir = config.RESULTS_DIR
    os.makedirs(results_dir, exist_ok=True)

    today = datetime.datetime.now().strftime('%Y%m%d')
    partner_tag = partner.upper() if partner else 'ALL'

    csv_path  = os.path.join(results_dir, f'bioscan_plate_status_{partner_tag}_{today}.csv')
    xlsx_path = os.path.join(results_dir, f'bioscan_plate_status_{partner_tag}_{today}.xlsx')
    txt_path  = os.path.join(results_dir, f'missing_plates_{partner_tag}_{today}.txt')

    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)

    # Text report of missing plates
    missing = df[df['missing_at'].notna()]
    with open(txt_path, 'w') as f:
        f.write(f"BIOSCAN MISSING PLATES REPORT — {today} — partner={partner_tag}\n")
        f.write("=" * 70 + "\n\n")
        for stage in ['mbrave', 'qc', 'bold']:
            subset = missing[missing['missing_at'] == stage]
            if len(subset) > 0:
                f.write(f"Dropped at '{stage}' ({len(subset)} plates):\n")
                for _, row in subset.iterrows():
                    f.write(f"  {row['plate_id']}\t"
                            f"partner={row.get('partner','?')}\t"
                            f"mbrave_batches={row['mbrave_batches']}\t"
                            f"qc_batches={row['qc_batches']}\n")
                f.write("\n")

    print(f"\nOutputs written:")
    print(f"  {csv_path}")
    print(f"  {xlsx_path}")
    print(f"  {txt_path}")
    return csv_path, xlsx_path, txt_path


def main():
    parser = argparse.ArgumentParser(
        description='BIOSCAN plate pipeline status checker'
    )
    parser.add_argument('--partner', default='ALL',
        help='4-letter partner code (e.g. BGEP) or ALL')
    parser.add_argument('--skip-portal', action='store_true',
        help='Skip ToL portal query (faster, no BOLD/partner info)')
    parser.add_argument('--missing-only', action='store_true',
        help='Print only plates with missing_at set')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    df, mbrave_skipped, qc_skipped, batches_missing_qc = build_master_table(
        partner=args.partner,
        skip_portal=args.skip_portal,
        verbose=args.verbose,
    )

    if args.missing_only:
        print_missing_summary(df)
        display_df = df[df['missing_at'].notna()]
    else:
        print_missing_summary(df)
        display_df = df

    print(f"\nMaster table: {len(df)} plates")
    print(display_df.to_string(index=False) if len(display_df) <= 50 else
          display_df.head(20).to_string(index=False) + f"\n... ({len(display_df)} rows total)")

    if mbrave_skipped:
        print(f"\nmBRAVE skipped folders (dedup): {mbrave_skipped}")
    if qc_skipped:
        print(f"QC skipped folders (dedup): {qc_skipped}")
    if batches_missing_qc:
        print(f"Batches with no QC file: {batches_missing_qc}")

    save_outputs(df, args.partner)


if __name__ == '__main__':
    main()
