"""
plate_status_report.py  —  MAIN ENTRY POINT

Builds a master plate status table joining:
  - Portal dump   : all submitted plates, submit date, partner, BOLD status
  - mBRAVE        : which batches each plate was sequenced in
  - QC            : QC results per plate

Controls (CONTROL_NEG*, CONTROL_POS*, CONTROL_*) are excluded throughout.

Usage:
    python3 plate_status_report.py --partner ALL
    python3 plate_status_report.py --partner BGEP
    python3 plate_status_report.py --partner ALL --skip-portal
    python3 plate_status_report.py --partner ALL --missing-only
"""

import argparse
import datetime
import os
import pandas as pd

import config
from mbrave_checker import build_mbrave_plate_index, summarise_mbrave
from qc_checker import build_qc_plate_index, summarise_qc
from utils import matches_partner


# ── Control filtering ─────────────────────────────────────────────────────────

def is_control(plate_id):
    if plate_id is None:
        return True
    pid = str(plate_id).upper()
    return (pid.startswith('CONTROL_') or
            pid.startswith('CONTROL-') or
            'CONTROL_NEG' in pid or
            'CONTROL_POS' in pid)


def filter_controls(plate_dict):
    return {k: v for k, v in plate_dict.items() if not is_control(k)}


# ── Main table builder ────────────────────────────────────────────────────────

def build_master_table(partner='ALL', skip_portal=False, verbose=False):

    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    # ── 1. mBRAVE ─────────────────────────────────────────────────────────────
    print("=" * 60)
    print("Step 1: Scanning mBRAVE data...")
    print("=" * 60)
    plate_to_mbrave_batches, mbrave_resolved, mbrave_skipped = \
        build_mbrave_plate_index(partner=partner, verbose=verbose)
    plate_to_mbrave_batches = filter_controls(plate_to_mbrave_batches)
    summarise_mbrave(plate_to_mbrave_batches)

    # ── 2. QC ─────────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Step 2: Scanning QC reports...")
    print("=" * 60)
    plate_qc_summary, mbrave_to_qc, qc_to_mbrave, cross_issues, batches_missing_qc = \
        build_qc_plate_index(partner=partner, verbose=verbose)
    plate_qc_summary = filter_controls(plate_qc_summary)
    summarise_qc(plate_qc_summary, batches_missing_qc)

    # ── 3. Portal ─────────────────────────────────────────────────────────────
    portal_index = {}

    if not skip_portal:
        print("\n" + "=" * 60)
        print("Step 3: Loading portal plate summary...")
        print("=" * 60)
        try:
            from read_portal_dump import load_portal_plate_summary
            portal_plates_df = load_portal_plate_summary(config.PORTAL_PLATES_CSV)
            if not portal_plates_df.empty:
                print(f"  Portal: {len(portal_plates_df)} plates loaded from dump")
                print(f"  BOLD uploaded: {portal_plates_df['bold_uploaded'].sum()} plates")
                portal_index = portal_plates_df.set_index('plate_id').to_dict('index')
        except Exception as e:
            print(f"  ERROR loading portal dump: {e}")
            print(f"  Run: python3 read_portal_dump.py")
    else:
        print("\nStep 3: Portal SKIPPED (--skip-portal flag)")

    # ── 4. Union all plates ───────────────────────────────────────────────────
    all_plates = sorted(
        set(portal_index.keys()) |
        set(plate_to_mbrave_batches.keys()) |
        set(plate_qc_summary.keys())
    )
    all_plates = [p for p in all_plates if not is_control(p)]

    if partner and partner.upper() != 'ALL':
        all_plates = [p for p in all_plates if matches_partner(p, partner)]

    print(f"\nTotal unique plates (controls excluded): {len(all_plates)}")

    # ── 5. Build rows ─────────────────────────────────────────────────────────
    rows = []
    for plate_id in all_plates:
        mbrave_batches = plate_to_mbrave_batches.get(plate_id, [])
        qc_info        = plate_qc_summary.get(plate_id, None)
        portal_info    = portal_index.get(plate_id, None)

        if portal_info:
            portal_status  = 'FOUND'
            partner_code   = portal_info.get('partner')
            submit_date    = portal_info.get('submit_date')
            bold_status    = 'HAS_DATA' if portal_info.get('bold_uploaded') else 'NO_DATA'
            n_wells_portal = portal_info.get('n_wells_portal', 0)
        elif skip_portal:
            portal_status  = 'SKIPPED'
            partner_code   = None
            submit_date    = None
            bold_status    = 'UNKNOWN'
            n_wells_portal = None
        else:
            portal_status  = 'MISSING'
            partner_code   = None
            submit_date    = None
            bold_status    = 'UNKNOWN'
            n_wells_portal = 0

        mbrave_status = 'FOUND' if mbrave_batches else 'MISSING'
        qc_status     = 'FOUND' if qc_info else 'MISSING'
        best_qc       = qc_info['best_status'] if qc_info else 'MISSING'

        stages = []
        if portal_status == 'FOUND':  stages.append('portal')
        if mbrave_status == 'FOUND':  stages.append('mbrave')
        if qc_status == 'FOUND':      stages.append('qc')
        if bold_status == 'HAS_DATA': stages.append('bold')
        pipeline_stage = stages[-1] if stages else 'none'

        missing_at = None
        if not skip_portal:
            all_stages = ['portal', 'mbrave', 'qc', 'bold']
            for stage in all_stages:
                if stage not in stages:
                    idx = all_stages.index(stage)
                    if idx > 0 and all_stages[idx - 1] in stages:
                        missing_at = stage
                        break

        rows.append({
            'plate_id':       plate_id,
            'partner':        partner_code,
            'submit_date':    submit_date,
            'portal_status':  portal_status,
            'portal_n_wells': n_wells_portal,
            'mbrave_status':  mbrave_status,
            'mbrave_batches': ','.join(mbrave_batches),
            'n_sequencings':  len(mbrave_batches),
            'qc_status':      qc_status,
            'qc_batches':     ','.join(qc_info['qc_batches']) if qc_info else '',
            'best_qc_result': best_qc,
            'bold_status':    bold_status,
            'pipeline_stage': pipeline_stage,
            'missing_at':     missing_at,
        })

    df = pd.DataFrame(rows)
    return df, mbrave_skipped, cross_issues, batches_missing_qc


# ── Summaries ─────────────────────────────────────────────────────────────────

def print_missing_summary(df):
    print("\n" + "=" * 60)
    print("MISSING PLATE SUMMARY")
    print("=" * 60)

    if 'missing_at' in df.columns:
        for stage in ['mbrave', 'qc', 'bold']:
            missing = df[df['missing_at'] == stage]
            if len(missing) > 0:
                print(f"\nDropped out at '{stage}' ({len(missing)} plates):")
                for _, row in missing.head(20).iterrows():
                    print(f"  {row['plate_id']}"
                          f"  partner={row.get('partner','?')}"
                          f"  submitted={row.get('submit_date','?')}"
                          f"  mbrave={row['mbrave_status']}"
                          f"  qc={row['qc_status']}")
                if len(missing) > 20:
                    print(f"  ... and {len(missing)-20} more (see CSV output)")

    qc_no_mbrave = df[(df['qc_status'] == 'FOUND') & (df['mbrave_status'] == 'MISSING')]
    if len(qc_no_mbrave) > 0:
        print(f"\nIn QC but NOT in mBRAVE ({len(qc_no_mbrave)} plates):")
        for _, row in qc_no_mbrave.iterrows():
            print(f"  {row['plate_id']}  qc_batches={row['qc_batches']}")


def save_outputs(df, partner, results_dir=None):
    if results_dir is None:
        results_dir = config.RESULTS_DIR
    os.makedirs(results_dir, exist_ok=True)

    today       = datetime.datetime.now().strftime('%Y%m%d')
    partner_tag = partner.upper() if partner else 'ALL'
    csv_path    = os.path.join(results_dir, f'bioscan_plate_status_{partner_tag}_{today}.csv')
    xlsx_path   = os.path.join(results_dir, f'bioscan_plate_status_{partner_tag}_{today}.xlsx')
    txt_path    = os.path.join(results_dir, f'missing_plates_{partner_tag}_{today}.txt')

    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)

    missing = df[df['missing_at'].notna()]
    with open(txt_path, 'w') as f:
        f.write(f"BIOSCAN MISSING PLATES — {today} — partner={partner_tag}\n")
        f.write("=" * 70 + "\n\n")
        for stage in ['mbrave', 'qc', 'bold']:
            subset = missing[missing['missing_at'] == stage]
            if len(subset) > 0:
                f.write(f"Dropped at '{stage}' ({len(subset)} plates):\n")
                for _, row in subset.iterrows():
                    f.write(f"  {row['plate_id']}\t"
                            f"partner={row.get('partner','?')}\t"
                            f"submitted={row.get('submit_date','?')}\t"
                            f"mbrave_batches={row['mbrave_batches']}\t"
                            f"qc_batches={row['qc_batches']}\n")
                f.write("\n")

    print(f"\nOutputs written:")
    print(f"  {csv_path}")
    print(f"  {xlsx_path}")
    print(f"  {txt_path}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='BIOSCAN plate pipeline status checker'
    )
    parser.add_argument('--partner', default='ALL',
        help='4-letter partner code (e.g. BGEP) or ALL')
    parser.add_argument('--skip-portal', action='store_true',
        help='Skip portal data (no submit date/BOLD/partner info)')
    parser.add_argument('--missing-only', action='store_true',
        help='Print only plates with missing_at set')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    df, mbrave_skipped, cross_issues, batches_missing_qc = build_master_table(
        partner=args.partner,
        skip_portal=args.skip_portal,
        verbose=args.verbose,
    )

    print_missing_summary(df)

    print(f"\nMaster table: {len(df)} plates")
    print(df.head(20).to_string(index=False))
    if len(df) > 20:
        print(f"... ({len(df)} rows total)")

    if mbrave_skipped:
        print(f"\nmBRAVE skipped folders (dedup): {mbrave_skipped}")
    if cross_issues:
        print(f"\nCross-mapping issues: {cross_issues}")
    if batches_missing_qc:
        print(f"Batches with no filtered_metadata: {batches_missing_qc}")

    save_outputs(df, args.partner)


if __name__ == '__main__':
    main()
