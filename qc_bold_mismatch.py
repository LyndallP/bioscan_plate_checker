"""
qc_bold_mismatch.py

Flags specimens that have QC result = FAILED (category_decision = NO) but
still have a sequence record in BOLD (bold_nuc is non-empty in the portal dump).

This should never happen: only QC-passed specimens should be transferred to BOLD.
Any match here indicates a data integrity issue requiring investigation.

Outputs
-------
    qc_bold_mismatch_YYYYMMDD.csv   — specimen-level mismatch table
    qc_bold_mismatch_YYYYMMDD.txt   — human-readable summary report

Key columns in the CSV:
    specimen_id             — specimen+well ID (from QC files)
    plate_id                — plate ID (derived from specimen_id)
    partner                 — 4-letter partner code
    qc_batch                — QC batch folder where FAILED decision was found
    qc_decision_raw         — raw value from category_decision (NO)
    bold_nuc                — sequence string from portal dump (non-empty = in BOLD)
    bold_sequence_upload_date — date the sequence was uploaded to BOLD
    bold_bin_uri            — BIN URI assigned on BOLD (if any)

BGE partner plates (BGEP, BGEG, BGKU, BGPT) are excluded from this analysis,
consistent with their removal from all other pipeline outputs.

Usage:
    python3 qc_bold_mismatch.py
    python3 qc_bold_mismatch.py --input /path/to/sts_manifests.tsv
    python3 qc_bold_mismatch.py --partner FACE
    python3 qc_bold_mismatch.py --verbose
"""

import argparse
import datetime
import glob
import os
import re
import pandas as pd

import config
from utils import extract_plate_from_pid, is_bge_plate, safe_read_csv, batch_sort_key


# ── Column names ──────────────────────────────────────────────────────────────

# QC files (filtered_metadata_batch*.csv / qc_portal_batch*.csv)
_QC_PID_COL      = 'pid'
_QC_DECISION_COL = 'category_decision'
_QC_FAILED_VALUE = 'NO'    # raw value that means FAILED

# Portal dump columns
_PORTAL_SPECIMEN_COL  = 'sts_specimen.id'
_PORTAL_BOLD_NUC_COL  = 'bold_nuc'
_PORTAL_UPLOAD_DATE   = 'bold_sequence_upload_date'
_PORTAL_BIN_URI       = 'bold_bin_uri'
_PORTAL_SUBMIT_DATE   = 'sts_submit_date'


# ── QC loader ─────────────────────────────────────────────────────────────────

def _normalise_pid(pid):
    """Strip TOL- prefix from a specimen/plate ID."""
    s = str(pid).strip()
    return s[4:] if s.upper().startswith('TOL-') else s


def load_failed_qc_specimens(qc_dir, verbose=False):
    """
    Scan all QC batch folders and collect specimens whose final (any-batch)
    category_decision is NO (FAILED).

    Returns DataFrame: specimen_id | plate_id | partner | qc_batch | qc_decision_raw
    """
    # Scan ALL non-special QC batch folders (same logic as plate_summary_all.py)
    all_qc_batches = sorted([
        d for d in os.listdir(qc_dir)
        if os.path.isdir(os.path.join(qc_dir, d))
        and d.startswith('batch')
        and 'EXCLUDED' not in d
        and 'RnD'        not in d
        and 'PCR1_volume' not in d
        and '_repeat_'   not in d
        and '_rep_'      not in d
        and '_merged'    not in d
    ], key=batch_sort_key)

    records = []

    for batch_folder in all_qc_batches:
        batch_path = os.path.join(qc_dir, batch_folder)

        # Prefer qc_portal file (includes FAILED specimens; filtered_metadata
        # may omit them in some batch formats)
        portal_files = glob.glob(os.path.join(batch_path, 'qc_portal_batch*.csv'))
        meta_files   = glob.glob(os.path.join(batch_path, 'filtered_metadata_batch*.csv'))

        source_file = (portal_files or meta_files or [None])[0]
        if source_file is None:
            if verbose:
                print(f"  {batch_folder}: no QC file found — skipping")
            continue

        try:
            df = safe_read_csv(source_file, dtype=str)
            # Normalise column names (some files wrap in quotes)
            df.columns = [c.strip().strip('"') for c in df.columns]

            # Map to canonical column names
            if _QC_DECISION_COL not in df.columns:
                # qc_portal files may already have 'category_decision'
                # or may use 'decision' after earlier processing — check
                if 'decision' in df.columns:
                    df = df.rename(columns={'decision': _QC_DECISION_COL})
                else:
                    if verbose:
                        print(f"  {batch_folder}: no decision column — skipping "
                              f"(found: {list(df.columns[:8])})")
                    continue

            if _QC_PID_COL not in df.columns:
                if verbose:
                    print(f"  {batch_folder}: no 'pid' column — skipping")
                continue

            df[_QC_PID_COL]      = df[_QC_PID_COL].str.strip().str.strip('"')
            df[_QC_DECISION_COL] = df[_QC_DECISION_COL].str.strip().str.strip('"')

            # Skip header rows that leaked through
            df = df[df[_QC_PID_COL] != 'pid']
            df = df[df[_QC_PID_COL].notna()]

            # Keep only FAILED records
            failed = df[df[_QC_DECISION_COL] == _QC_FAILED_VALUE].copy()
            if failed.empty:
                if verbose:
                    print(f"  {batch_folder}: 0 FAILED specimens")
                continue

            # Derive plate ID and partner
            failed['specimen_id_norm'] = failed[_QC_PID_COL].apply(_normalise_pid)
            failed['plate_id'] = failed['specimen_id_norm'].apply(extract_plate_from_pid)

            # Exclude BGE partner plates
            failed = failed[~failed['plate_id'].apply(is_bge_plate)]

            for _, row in failed.iterrows():
                plate_id = row['plate_id']
                if not plate_id:
                    continue
                # Extract partner from plate ID
                partner = None
                m = re.match(r'^TOL-([A-Z]{4})-', str(plate_id))
                if m:
                    partner = m.group(1)
                else:
                    m = re.match(r'^([A-Z]{4})[-_]', str(plate_id))
                    if m:
                        partner = m.group(1)
                    elif str(plate_id).upper().startswith('MOZZ'):
                        partner = 'MOZZ'

                records.append({
                    'specimen_id':      row['specimen_id_norm'],
                    'plate_id':         plate_id,
                    'partner':          partner,
                    'qc_batch':         batch_folder,
                    'qc_decision_raw':  row[_QC_DECISION_COL],
                })

            if verbose:
                print(f"  {batch_folder}: {len(failed)} FAILED specimens retained")

        except Exception as e:
            print(f"  WARNING: {batch_folder}: error reading QC file: {e}")
            continue

    if not records:
        return pd.DataFrame(columns=[
            'specimen_id', 'plate_id', 'partner',
            'qc_batch', 'qc_decision_raw',
        ])
    return pd.DataFrame(records)


# ── Portal dump loader ────────────────────────────────────────────────────────

def load_bold_specimens(dump_path):
    """
    Load specimens from the portal dump that have a sequence on BOLD
    (bold_nuc is non-empty).

    Returns DataFrame: specimen_id | bold_nuc | bold_sequence_upload_date |
                       bold_bin_uri | submit_date
    """
    print(f"Reading portal dump: {dump_path}")

    wanted = [
        _PORTAL_SPECIMEN_COL,
        _PORTAL_BOLD_NUC_COL,
        _PORTAL_UPLOAD_DATE,
        _PORTAL_BIN_URI,
        _PORTAL_SUBMIT_DATE,
    ]
    peek = pd.read_csv(dump_path, sep='\t', dtype=str, nrows=0)
    available = [c for c in wanted if c in peek.columns]
    missing_cols = [c for c in wanted if c not in peek.columns]
    if missing_cols:
        print(f"  NOTE: columns not in dump (may be older dump): {missing_cols}")

    df = pd.read_csv(dump_path, sep='\t', dtype=str,
                     usecols=available, low_memory=False)
    for col in wanted:
        if col not in df.columns:
            df[col] = None

    print(f"  {len(df)} rows loaded")

    # Normalise None strings
    for col in [_PORTAL_BOLD_NUC_COL, _PORTAL_UPLOAD_DATE,
                _PORTAL_BIN_URI, _PORTAL_SUBMIT_DATE]:
        df[col] = df[col].replace({'None': None, 'nan': None, '': None})

    # Keep only specimens with a sequence on BOLD
    has_seq = (
        df[_PORTAL_BOLD_NUC_COL].notna() &
        (df[_PORTAL_BOLD_NUC_COL].str.strip() != '') &
        (df[_PORTAL_BOLD_NUC_COL].str.len() > 10)
    )
    df = df[has_seq].copy()
    print(f"  {len(df)} specimens with bold_nuc populated")

    # Normalise specimen ID (strip TOL- prefix)
    df['specimen_id'] = df[_PORTAL_SPECIMEN_COL].apply(_normalise_pid)

    # Exclude BGE partner plates
    df['plate_id'] = df['specimen_id'].apply(extract_plate_from_pid)
    df = df[~df['plate_id'].apply(is_bge_plate)]

    return df[['specimen_id', _PORTAL_BOLD_NUC_COL,
               _PORTAL_UPLOAD_DATE, _PORTAL_BIN_URI,
               _PORTAL_SUBMIT_DATE]].copy()


# ── Cross-reference ───────────────────────────────────────────────────────────

def find_mismatches(failed_df, bold_df):
    """
    Return rows where specimen is in both:
      - failed_df  (QC FAILED)
      - bold_df    (has sequence on BOLD)
    """
    bold_set = set(bold_df['specimen_id'].dropna())
    mask = failed_df['specimen_id'].isin(bold_set)
    mismatches = failed_df[mask].copy()

    bold_indexed = bold_df.set_index('specimen_id')
    mismatches[_PORTAL_BOLD_NUC_COL]  = mismatches['specimen_id'].map(
        bold_indexed[_PORTAL_BOLD_NUC_COL])
    mismatches[_PORTAL_UPLOAD_DATE]   = mismatches['specimen_id'].map(
        bold_indexed[_PORTAL_UPLOAD_DATE])
    mismatches[_PORTAL_BIN_URI]       = mismatches['specimen_id'].map(
        bold_indexed[_PORTAL_BIN_URI])
    mismatches[_PORTAL_SUBMIT_DATE]   = mismatches['specimen_id'].map(
        bold_indexed[_PORTAL_SUBMIT_DATE])

    return mismatches.sort_values(['partner', 'plate_id', 'specimen_id'])


# ── Report ────────────────────────────────────────────────────────────────────

def generate_report(mismatches, failed_total, bold_total, partner, output_path):
    lines = []

    def h(t): lines.extend(['', '=' * 70, t, '=' * 70])
    def s(t): lines.extend(['', f'--- {t} ---'])

    h("QC-FAILED but BOLD SEQUENCE PRESENT — MISMATCH REPORT")
    lines.append(f"Generated : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"Partner   : {partner}")
    lines.append(f"QC dir    : {config.QC_DIR}")
    lines.append(f"Portal    : {config.PORTAL_DUMP_TSV}")

    h("SUMMARY")
    lines.append(f"  Total QC-FAILED specimens scanned : {failed_total}")
    lines.append(f"  Specimens with BOLD sequence      : {bold_total}")
    lines.append(f"  MISMATCHES (FAILED + in BOLD)     : {len(mismatches)}")
    if len(mismatches) == 0:
        lines.append("")
        lines.append("  No mismatches found — all QC-FAILED specimens are absent from BOLD.")
    else:
        lines.append("")
        lines.append("  *** THESE SPECIMENS SHOULD NOT HAVE A BOLD SEQUENCE ***")
        lines.append("  Investigate: was sequence uploaded before QC was run,")
        lines.append("  or was a FAILED specimen mistakenly included in a BOLD transfer?")

    if len(mismatches) > 0:
        h("MISMATCHES BY PARTNER")
        partner_counts = mismatches['partner'].value_counts()
        for p, n in partner_counts.items():
            lines.append(f"  {str(p):<10}: {n}")

        h("MISMATCHES BY QC BATCH")
        batch_counts = mismatches['qc_batch'].value_counts().sort_index(key=lambda x: x.map(batch_sort_key))
        for b, n in batch_counts.items():
            lines.append(f"  {b:<25}: {n}")

        h("MISMATCH DETAIL (first 100)")
        lines.append(f"  {'Specimen ID':<30} {'Partner':<8} {'QC Batch':<20} "
                     f"{'Upload Date':<13} {'BIN URI':<20}")
        lines.append(f"  {'-'*30} {'-'*8} {'-'*20} {'-'*13} {'-'*20}")
        for _, row in mismatches.head(100).iterrows():
            lines.append(
                f"  {str(row['specimen_id']):<30} "
                f"{str(row.get('partner','?')):<8} "
                f"{str(row['qc_batch']):<20} "
                f"{str(row.get(_PORTAL_UPLOAD_DATE,'?') or '?'):<13} "
                f"{str(row.get(_PORTAL_BIN_URI,'') or ''):<20}"
            )
        if len(mismatches) > 100:
            lines.append(f"  ... {len(mismatches)-100} more rows — see CSV output")

    report = '\n'.join(lines)
    print(report)
    with open(output_path, 'w') as f:
        f.write(report)
    print(f"\nReport written to: {output_path}")
    return report


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Flag QC-FAILED specimens that have a sequence on BOLD'
    )
    parser.add_argument('--input', default=config.PORTAL_DUMP_TSV,
        help='Path to portal dump TSV')
    parser.add_argument('--partner', default='ALL',
        help='4-letter partner code to filter (default: ALL)')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    today = datetime.datetime.now().strftime('%Y%m%d')
    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    # ── 1. Load QC FAILED specimens ───────────────────────────────────────────
    print("Loading QC-FAILED specimens...")
    failed_df = load_failed_qc_specimens(config.QC_DIR, verbose=args.verbose)
    print(f"  {len(failed_df)} QC-FAILED specimen records loaded")

    # Optional partner filter
    if args.partner and args.partner.upper() != 'ALL':
        failed_df = failed_df[failed_df['partner'] == args.partner.upper()]
        print(f"  Filtered to partner '{args.partner}': {len(failed_df)} records")

    if failed_df.empty:
        print("No QC-FAILED records found — nothing to cross-check.")
        return

    # ── 2. Load BOLD specimens from portal dump ───────────────────────────────
    print("\nLoading BOLD sequences from portal dump...")
    bold_df = load_bold_specimens(args.input)

    if args.partner and args.partner.upper() != 'ALL':
        # Extract partner from specimen_id for filtering
        bold_df['_plate'] = bold_df['specimen_id'].apply(extract_plate_from_pid)
        bold_df['_partner'] = bold_df['_plate'].apply(
            lambda p: re.match(r'^([A-Z]{4})[-_]', str(p)).group(1)
            if re.match(r'^([A-Z]{4})[-_]', str(p)) else None
        )
        bold_df = bold_df[bold_df['_partner'] == args.partner.upper()]
        bold_df = bold_df.drop(columns=['_plate', '_partner'])
        print(f"  Filtered to partner '{args.partner}': {len(bold_df)} BOLD records")

    # ── 3. Cross-reference ────────────────────────────────────────────────────
    print("\nCross-referencing...")
    mismatches = find_mismatches(failed_df, bold_df)
    print(f"  Found {len(mismatches)} mismatches "
          f"(QC-FAILED specimens present in BOLD)")

    # ── 4. Save outputs ───────────────────────────────────────────────────────
    partner_tag = args.partner.upper() if args.partner else 'ALL'
    csv_path    = os.path.join(config.RESULTS_DIR,
                               f'qc_bold_mismatch_{partner_tag}_{today}.csv')
    report_path = os.path.join(config.RESULTS_DIR,
                               f'qc_bold_mismatch_{partner_tag}_{today}.txt')

    mismatches.to_csv(csv_path, index=False)
    print(f"  CSV saved: {csv_path}")

    generate_report(mismatches, len(failed_df), len(bold_df),
                    partner_tag, report_path)

    print(f"\nOutputs:")
    print(f"  {csv_path}")
    print(f"  {report_path}")


if __name__ == '__main__':
    main()
