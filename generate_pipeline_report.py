"""
generate_pipeline_report.py

Generates a human-readable pipeline status report from the master plate
status CSV. Run this after plate_status_report.py has completed.

Produces:
  - A text summary report
  - A per-partner breakdown
  - Flagged plates requiring investigation (old submissions not yet sequenced)

Usage:
    python3 generate_pipeline_report.py
    python3 generate_pipeline_report.py --input /path/to/bioscan_plate_status_ALL_20260411.csv
    python3 generate_pipeline_report.py --old-threshold-days 180
"""

import argparse
import datetime
import os
import glob
import pandas as pd

import config


def find_latest_status_csv(results_dir):
    """Find the most recently generated plate status CSV."""
    pattern = os.path.join(results_dir, 'bioscan_plate_status_ALL_*.csv')
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(
            f"No plate status CSV found in {results_dir}\n"
            f"Run: python3 plate_status_report.py --partner ALL"
        )
    return files[-1]


def generate_report(df, old_threshold_days=180, output_path=None):
    """
    Generate pipeline status report from master plate DataFrame.
    """
    today = datetime.date.today()
    threshold_date = str(today - datetime.timedelta(days=old_threshold_days))
    lines = []

    def h(title):
        lines.append("")
        lines.append("=" * 70)
        lines.append(title)
        lines.append("=" * 70)

    def s(title):
        lines.append("")
        lines.append(f"--- {title} ---")

    # ── Header ────────────────────────────────────────────────────────────────
    lines.append("BIOSCAN PLATE PIPELINE STATUS REPORT")
    lines.append(f"Generated: {today}")
    lines.append(f"Input: {len(df)} plates")

    # ── Overall summary ───────────────────────────────────────────────────────
    h("OVERALL PIPELINE SUMMARY")

    total = len(df)
    n_portal   = (df['portal_status'] == 'FOUND').sum()
    n_mbrave   = (df['mbrave_status'] == 'FOUND').sum()
    n_qc       = (df['qc_status'] == 'FOUND').sum()
    n_bold     = (df['bold_status'] == 'HAS_DATA').sum()

    lines.append(f"  Submitted to portal : {n_portal:>6}  ({100*n_portal//total}%)")
    lines.append(f"  Through mBRAVE      : {n_mbrave:>6}  ({100*n_mbrave//total}%)")
    lines.append(f"  Through QC          : {n_qc:>6}  ({100*n_qc//total}%)")
    lines.append(f"  Uploaded to BOLD    : {n_bold:>6}  ({100*n_bold//total}%)")
    lines.append("")
    lines.append(f"  Repeated plates (sequenced >1 batch): "
                 f"{(df['n_sequencings'] > 1).sum()}")

    # ── Missing plates summary ────────────────────────────────────────────────
    h("PLATES MISSING AT EACH STAGE")

    for stage in ['mbrave', 'qc', 'bold']:
        missing = df[df['missing_at'] == stage]
        if len(missing) == 0:
            continue
        lines.append(f"\n  Dropped at '{stage}': {len(missing)} plates")

        # Split by old vs recent
        if 'submit_date' in missing.columns and stage == 'mbrave':
            has_date = missing[missing['submit_date'].notna()]
            old = has_date[has_date['submit_date'] < threshold_date]
            recent = has_date[has_date['submit_date'] >= threshold_date]
            no_date = missing[missing['submit_date'].isna()]

            if len(old) > 0:
                lines.append(f"    ⚠  OLD (submitted >{old_threshold_days} days ago): "
                             f"{len(old)} plates — INVESTIGATE")
            if len(recent) > 0:
                lines.append(f"    ·  Recent (submitted <{old_threshold_days} days ago): "
                             f"{len(recent)} plates — likely in queue")
            if len(no_date) > 0:
                lines.append(f"    ·  No submit date: {len(no_date)} plates "
                             f"(R&D/test runs)")

    # ── Old missing plates detail ─────────────────────────────────────────────
    h(f"PLATES REQUIRING INVESTIGATION (submitted >{old_threshold_days} days ago, not yet sequenced)")

    missing_mbrave = df[df['missing_at'] == 'mbrave']
    if 'submit_date' in missing_mbrave.columns:
        old_missing = missing_mbrave[
            missing_mbrave['submit_date'].notna() &
            (missing_mbrave['submit_date'] < threshold_date)
        ].sort_values(['partner', 'submit_date', 'plate_id'])

        if len(old_missing) == 0:
            lines.append("  None — all old submissions have been sequenced.")
        else:
            lines.append(f"  {len(old_missing)} plates submitted before "
                        f"{threshold_date} with no mBRAVE data:\n")
            current_partner = None
            for _, row in old_missing.iterrows():
                if row['partner'] != current_partner:
                    current_partner = row['partner']
                    lines.append(f"  {current_partner}:")
                days_waiting = (today - datetime.date.fromisoformat(
                    row['submit_date'])).days
                lines.append(f"    {row['plate_id']:<20} "
                            f"submitted={row['submit_date']}  "
                            f"({days_waiting} days ago)")

    # ── Per-partner breakdown ─────────────────────────────────────────────────
    h("PER-PARTNER BREAKDOWN")

    portal_plates = df[df['portal_status'] == 'FOUND'].copy()
    if len(portal_plates) > 0:
        lines.append(f"  {'Partner':<8} {'Submitted':>9} {'mBRAVE':>8} "
                    f"{'QC':>6} {'BOLD':>6} {'Missing@':>10}")
        lines.append(f"  {'-'*8} {'-'*9} {'-'*8} {'-'*6} {'-'*6} {'-'*10}")

        partners = sorted(portal_plates['partner'].dropna().unique())
        for partner in partners:
            p = portal_plates[portal_plates['partner'] == partner]
            n_sub    = len(p)
            n_mb     = (p['mbrave_status'] == 'FOUND').sum()
            n_qc     = (p['qc_status'] == 'FOUND').sum()
            n_bold   = (p['bold_status'] == 'HAS_DATA').sum()
            # Most common missing stage
            missing_counts = p['missing_at'].value_counts()
            top_missing = missing_counts.index[0] if len(missing_counts) > 0 else '-'
            lines.append(f"  {partner:<8} {n_sub:>9} {n_mb:>8} "
                        f"{n_qc:>6} {n_bold:>6} {top_missing:>10}")

    # ── BOLD upload status ────────────────────────────────────────────────────
    h("BOLD UPLOAD STATUS")

    qc_done = df[df['qc_status'] == 'FOUND']
    bold_done = qc_done[qc_done['bold_status'] == 'HAS_DATA']
    bold_missing = qc_done[qc_done['bold_status'] == 'NO_DATA']

    lines.append(f"  Plates through QC          : {len(qc_done)}")
    lines.append(f"  Uploaded to BOLD           : {len(bold_done)} "
                f"({100*len(bold_done)//len(qc_done) if len(qc_done) else 0}%)")
    lines.append(f"  QC done but not on BOLD    : {len(bold_missing)}")

    if len(bold_missing) > 0:
        s("Partners with plates not yet on BOLD")
        partner_counts = bold_missing['partner'].value_counts()
        for partner, count in partner_counts.items():
            lines.append(f"    {partner:<8}: {count} plates")

    # ── Footer ────────────────────────────────────────────────────────────────
    lines.append("")
    lines.append("=" * 70)
    lines.append(f"Report generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("=" * 70)

    report_text = "\n".join(lines)

    if output_path:
        with open(output_path, 'w') as f:
            f.write(report_text)
        print(f"Report written to: {output_path}")

    print(report_text)
    return report_text


def main():
    parser = argparse.ArgumentParser(
        description='Generate BIOSCAN pipeline status report'
    )
    parser.add_argument('--input', default=None,
        help='Path to plate status CSV (default: latest in RESULTS_DIR)')
    parser.add_argument('--old-threshold-days', type=int, default=180,
        help='Days after which an unsequenced submission is flagged (default: 180)')
    parser.add_argument('--output', default=None,
        help='Output report path (default: RESULTS_DIR/pipeline_report_YYYYMMDD.txt)')
    args = parser.parse_args()

    # Find input
    if args.input is None:
        args.input = find_latest_status_csv(config.RESULTS_DIR)
    print(f"Reading: {args.input}")

    # Output path
    if args.output is None:
        today = datetime.datetime.now().strftime('%Y%m%d')
        args.output = os.path.join(
            config.RESULTS_DIR, f'pipeline_report_{today}.txt')

    df = pd.read_csv(args.input, dtype=str)
    # Fix numeric columns
    df['n_sequencings'] = pd.to_numeric(df['n_sequencings'], errors='coerce').fillna(0)
    df['bold_uploaded'] = df['bold_status'] == 'HAS_DATA'

    generate_report(df, old_threshold_days=args.old_threshold_days,
                   output_path=args.output)


if __name__ == '__main__':
    main()
