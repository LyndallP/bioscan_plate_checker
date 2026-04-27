"""
bold_workbench_analysis.py

Analyses BOLD workbench exports to:
  1. Concatenate annual workbench files into a single combined file
  2. Report quality flags (Stop Codon, Contamination, Flagged Record, BIN status)
  3. Compare flagged sequences against QC-passed sequences from filtered_metadata
     and BOLD_filtered_sequences FASTA files — to determine if a better sequence
     now exists that should replace the flagged BOLD record

Input files (place in RESULTS_DIR):
    bold_workbench_2021.csv   ┐
    bold_workbench_2022.csv   │  BOLD workbench exports, filtered by year
    bold_workbench_2023.csv   │  header=2 (skip 2 rows, row 3 = column names)
    bold_workbench_2024.csv   │  columns include: Sample ID, BIN, Stop Codon,
    bold_workbench_2025.csv   │  Contamination, Flagged Record, Barcode Compliant
    bold_workbench_2026.csv   ┘

Portal dump (for sequences):
    config.PORTAL_DUMP_TSV — contains bold_nuc (sequence on BOLD)

QC FASTA files (for QC-passed sequences):
    BOLD_filtered_sequences_batchN.fasta  in each QC batch folder
    These are exactly what was uploaded to BOLD.

Usage:
    python3 bold_workbench_analysis.py
    python3 bold_workbench_analysis.py --partner FACE
    python3 bold_workbench_analysis.py --skip-sequence-comparison
    python3 bold_workbench_analysis.py --rebuild-cache
"""

import argparse
import datetime
import glob
import os
import re
import pandas as pd
from collections import defaultdict

import config
from utils import extract_plate_from_pid, matches_partner, resolve_batches


# ── Constants ─────────────────────────────────────────────────────────────────

WORKBENCH_YEARS     = [2021, 2022, 2023, 2024, 2025, 2026]
WORKBENCH_PATTERN   = "bold_workbench_{year}.csv"
COMBINED_CACHE      = "bold_workbench_combined.csv"
BOLD_FASTA_PATTERN  = "BOLD_filtered_sequences_batch*.fasta"

# Workbench columns
WB_SAMPLE_ID_COL    = 'Sample ID'
WB_PROCESS_ID_COL   = 'Process ID'
WB_PROJECT_COL      = 'Project Code'
WB_BIN_COL          = 'BIN'
WB_STOP_CODON_COL   = 'Stop Codon'
WB_CONTAM_COL       = 'Contamination'
WB_FLAGGED_COL      = 'Flagged Record'
WB_COMPLIANT_COL    = 'Barcode Compliant'
WB_SEQ_LENGTH_COL   = 'COI-5P Seq. Length'
WB_UPLOAD_DATE_COL  = 'Collection Date'  # note: this is sequence upload date in workbench

# Portal dump columns
PORTAL_SPECIMEN_COL  = 'sts_specimen.id'
PORTAL_BOLD_NUC_COL  = 'bold_nuc'
PORTAL_UPLOAD_COL    = 'bold_sequence_upload_date'
PORTAL_BIN_COL       = 'bold_bin_uri'


# ── Step 1: Load and combine workbench files ──────────────────────────────────

def load_workbench(results_dir=None, rebuild_cache=False, verbose=False):
    """
    Load and concatenate annual workbench CSV files.
    Caches combined file to avoid re-reading every run.

    Returns DataFrame with all workbench records.
    """
    if results_dir is None:
        results_dir = config.RESULTS_DIR

    cache_path = os.path.join(results_dir, COMBINED_CACHE)

    if os.path.exists(cache_path) and not rebuild_cache:
        print(f"Loading cached workbench data: {cache_path}")
        df = pd.read_csv(cache_path, dtype=str, low_memory=False)
        print(f"  {len(df)} records loaded from cache")
        return df

    print("Loading BOLD workbench annual files...")
    dfs = []
    for year in WORKBENCH_YEARS:
        path = os.path.join(results_dir, WORKBENCH_PATTERN.format(year=year))
        if not os.path.exists(path):
            if verbose:
                print(f"  {year}: file not found ({path})")
            continue
        try:
            df = pd.read_csv(path, header=2, dtype=str, low_memory=False)
            df['source_year'] = str(year)
            dfs.append(df)
            print(f"  {year}: {len(df)} records")
        except Exception as e:
            print(f"  {year}: ERROR reading {path}: {e}")

    if not dfs:
        raise FileNotFoundError(
            f"No workbench files found in {results_dir}\n"
            f"Expected: bold_workbench_YYYY.csv for years {WORKBENCH_YEARS}"
        )

    combined = pd.concat(dfs, ignore_index=True)

    # Deduplicate on Sample ID (keep most recent year)
    combined = combined.sort_values('source_year', ascending=False)
    combined = combined.drop_duplicates(subset=WB_SAMPLE_ID_COL, keep='first')
    combined = combined.reset_index(drop=True)

    print(f"  Combined: {len(combined)} unique specimens across "
          f"{len(dfs)} year files")

    # Save cache
    combined.to_csv(cache_path, index=False)
    print(f"  Cached to: {cache_path}")

    return combined


# ── Step 2: Add plate/partner info and quality flags ─────────────────────────

def enrich_workbench(wb_df, partner=None):
    """Add plate_id, partner, and boolean quality flag columns."""
    wb_df = wb_df.copy()

    wb_df['plate_id'] = wb_df[WB_SAMPLE_ID_COL].apply(
        lambda s: re.sub(r'_[^_]+$', '', str(s)) if pd.notna(s) else None)

    wb_df['partner_code'] = wb_df['plate_id'].apply(
        lambda p: _extract_partner(p) if p else None)

    # Boolean flags
    wb_df['has_bin']       = wb_df[WB_BIN_COL].notna() & \
                             (wb_df[WB_BIN_COL].str.strip() != '') & \
                             (wb_df[WB_BIN_COL] != 'None')
    wb_df['has_stop_codon']  = wb_df[WB_STOP_CODON_COL].notna() & \
                               (wb_df[WB_STOP_CODON_COL].str.strip() != '')
    wb_df['has_contam']      = wb_df[WB_CONTAM_COL].notna() & \
                               (wb_df[WB_CONTAM_COL].str.strip() != '')
    wb_df['is_flagged']      = wb_df[WB_FLAGGED_COL].str.strip().str.lower() == 'yes'
    wb_df['is_compliant']    = wb_df[WB_COMPLIANT_COL].str.strip().str.lower() == 'yes'

    if partner and partner.upper() != 'ALL':
        wb_df = wb_df[wb_df['partner_code'] == partner.upper()]
        print(f"  Filtered to partner '{partner}': {len(wb_df)} records")

    return wb_df


def _extract_partner(plate_id):
    if not plate_id:
        return None
    m = re.match(r'^TOL-([A-Z]{4})-', str(plate_id))
    if m:
        return m.group(1)
    m = re.match(r'^([A-Z]{4})[-_]', str(plate_id))
    if m:
        return m.group(1)
    if str(plate_id).upper().startswith('MOZZ'):
        return 'MOZZ'
    return None


# ── Step 3: Load QC FASTA sequences ──────────────────────────────────────────

def load_qc_fasta_sequences(qc_dir=None, partner=None, verbose=False):
    """
    Load all BOLD_filtered_sequences FASTA files from QC batch folders.
    These are exactly what was submitted to BOLD.

    Returns dict: specimen_id -> sequence (uppercase, no gaps)
    """
    if qc_dir is None:
        qc_dir = config.QC_DIR

    resolved, _ = resolve_batches(qc_dir)
    sequences = {}
    n_loaded = 0

    for batch_folder in resolved:
        batch_path = os.path.join(qc_dir, batch_folder)
        fasta_files = glob.glob(os.path.join(batch_path, BOLD_FASTA_PATTERN))
        if not fasta_files:
            continue

        for fasta_file in fasta_files:
            try:
                seqs = _parse_fasta(fasta_file)
                for specimen_id, seq in seqs.items():
                    # Apply partner filter
                    plate = re.sub(r'_[^_]+$', '', specimen_id)
                    if partner and partner.upper() != 'ALL':
                        if _extract_partner(plate) != partner.upper():
                            continue
                    # Keep most recent if duplicate
                    if specimen_id not in sequences:
                        sequences[specimen_id] = seq
                        n_loaded += 1
            except Exception as e:
                if verbose:
                    print(f"  WARNING: failed reading {fasta_file}: {e}")

    print(f"  Loaded {n_loaded} QC sequences from {len(resolved)} batches")
    return sequences


def _parse_fasta(fasta_path):
    """Parse FASTA file. Returns dict: header -> sequence."""
    sequences = {}
    current_id = None
    current_seq = []

    with open(fasta_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith('>'):
                if current_id is not None:
                    sequences[current_id] = ''.join(current_seq).upper()
                # Extract specimen ID from header — take first field
                current_id = line[1:].split()[0].split('|')[0].strip()
                current_seq = []
            elif line:
                current_seq.append(line)

    if current_id is not None:
        sequences[current_id] = ''.join(current_seq).upper()

    return sequences


# ── Step 4: Load portal sequences ────────────────────────────────────────────

def load_portal_sequences(dump_path=None, specimen_ids=None):
    """
    Load bold_nuc sequences from portal dump for a set of specimen IDs.
    Returns dict: specimen_id -> bold_nuc sequence
    """
    if dump_path is None:
        dump_path = config.PORTAL_DUMP_TSV

    print(f"  Loading portal sequences from dump...")
    df = pd.read_csv(dump_path, sep='\t', dtype=str,
                     usecols=[PORTAL_SPECIMEN_COL, PORTAL_BOLD_NUC_COL],
                     low_memory=False)

    if specimen_ids:
        df = df[df[PORTAL_SPECIMEN_COL].isin(specimen_ids)]

    # Filter to rows with actual sequences
    df = df[df[PORTAL_BOLD_NUC_COL].notna() &
            (df[PORTAL_BOLD_NUC_COL] != 'None') &
            (df[PORTAL_BOLD_NUC_COL].str.len() > 10)]

    seq_dict = dict(zip(df[PORTAL_SPECIMEN_COL], df[PORTAL_BOLD_NUC_COL].str.upper()))
    print(f"  {len(seq_dict)} portal sequences loaded")
    return seq_dict


# ── Step 5: Sequence comparison ───────────────────────────────────────────────

def compare_sequences(bold_seq, qc_seq):
    """
    Compare BOLD sequence vs QC sequence.
    Returns: 'IDENTICAL', 'DIFFERENT', 'QC_ONLY', 'BOLD_ONLY', 'NEITHER'
    """
    has_bold = bool(bold_seq and len(str(bold_seq).strip()) > 10)
    has_qc   = bool(qc_seq  and len(str(qc_seq).strip())  > 10)

    if not has_bold and not has_qc:
        return 'NEITHER'
    if has_bold and not has_qc:
        return 'BOLD_ONLY'
    if has_qc and not has_bold:
        return 'QC_ONLY'

    # Both present — compare (strip gaps and whitespace)
    b = re.sub(r'[-\s]', '', str(bold_seq)).upper()
    q = re.sub(r'[-\s]', '', str(qc_seq)).upper()
    return 'IDENTICAL' if b == q else 'DIFFERENT'


# ── Step 6: Generate report ───────────────────────────────────────────────────

def generate_report(wb_df, comparison_df, partner, output_path):
    lines = []

    def h(t): lines.extend(['', '=' * 65, t, '=' * 65])
    def s(t): lines.extend(['', f'--- {t} ---'])

    h("BOLD WORKBENCH ANALYSIS REPORT")
    lines.append(f"Generated : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"Partner   : {partner}")
    lines.append(f"Total workbench records: {len(wb_df)}")

    h("OVERALL QUALITY FLAGS")
    lines.append(f"  Total specimens on BOLD       : {len(wb_df)}")
    lines.append(f"  With BIN URI                  : {wb_df['has_bin'].sum()}")
    lines.append(f"  Without BIN URI               : {(~wb_df['has_bin']).sum()}")
    lines.append(f"  Has stop codon flag           : {wb_df['has_stop_codon'].sum()}")
    lines.append(f"  Has contamination flag        : {wb_df['has_contam'].sum()}")
    lines.append(f"  Flagged record                : {wb_df['is_flagged'].sum()}")
    lines.append(f"  Barcode compliant             : {wb_df['is_compliant'].sum()}")

    h("FLAGS BY PARTNER")
    partner_grp = wb_df.groupby('partner_code').agg(
        n_total       = ('has_bin', 'count'),
        n_with_bin    = ('has_bin', 'sum'),
        n_stop_codon  = ('has_stop_codon', 'sum'),
        n_contam      = ('has_contam', 'sum'),
        n_flagged     = ('is_flagged', 'sum'),
        n_compliant   = ('is_compliant', 'sum'),
    ).reset_index().sort_values('n_flagged', ascending=False)

    lines.append(f"  {'Partner':<8} {'Total':>7} {'BIN':>6} {'StopCdn':>8} "
                f"{'Contam':>7} {'Flagged':>8} {'Compliant':>10}")
    lines.append(f"  {'-'*8} {'-'*7} {'-'*6} {'-'*8} {'-'*7} {'-'*8} {'-'*10}")
    for _, row in partner_grp.iterrows():
        lines.append(f"  {str(row['partner_code']):<8} {int(row['n_total']):>7} "
                    f"{int(row['n_with_bin']):>6} {int(row['n_stop_codon']):>8} "
                    f"{int(row['n_contam']):>7} {int(row['n_flagged']):>8} "
                    f"{int(row['n_compliant']):>10}")

    if comparison_df is not None and len(comparison_df) > 0:
        h("SEQUENCE COMPARISON — FLAGGED RECORDS vs QC PASSED")
        lines.append("  For specimens with stop codon / contamination / flagged status,")
        lines.append("  compares the BOLD sequence against the QC-passed FASTA sequence.")
        lines.append("  DIFFERENT = QC has found a better sequence → update BOLD")
        lines.append("  IDENTICAL  = same sequence → flag is genuine quality issue")
        lines.append("  QC_ONLY    = specimen passed QC but no BOLD sequence found")
        lines.append("")

        comp_counts = comparison_df['sequence_status'].value_counts()
        for status, count in comp_counts.items():
            lines.append(f"  {status:<15} : {count}")

        s("Specimens needing BOLD update (DIFFERENT — QC has better sequence)")
        needs_update = comparison_df[comparison_df['sequence_status'] == 'DIFFERENT']
        lines.append(f"  {len(needs_update)} specimens where QC sequence differs from BOLD")
        if len(needs_update) > 0:
            partner_update = needs_update.groupby('partner_code').size().sort_values(ascending=False)
            lines.append(f"\n  By partner:")
            for p, n in partner_update.items():
                lines.append(f"    {p:<8}: {n}")

    report = '\n'.join(lines)
    print(report)
    with open(output_path, 'w') as f:
        f.write(report)
    print(f"\nReport written to: {output_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='BOLD workbench quality flag analysis and QC sequence comparison'
    )
    parser.add_argument('--partner', default='ALL')
    parser.add_argument('--skip-sequence-comparison', action='store_true',
        help='Skip loading FASTA files and comparing sequences (faster)')
    parser.add_argument('--rebuild-cache', action='store_true',
        help='Force re-reading of annual workbench files even if cache exists')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    today = datetime.datetime.now().strftime('%Y%m%d')
    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    # ── Load workbench ────────────────────────────────────────────────────────
    print("=" * 60)
    print("Step 1: Loading BOLD workbench files...")
    print("=" * 60)
    wb_df = load_workbench(config.RESULTS_DIR, rebuild_cache=args.rebuild_cache,
                           verbose=args.verbose)
    wb_df = enrich_workbench(wb_df, partner=args.partner)

    # ── Sequence comparison ───────────────────────────────────────────────────
    comparison_df = None
    if not args.skip_sequence_comparison:
        print("\n" + "=" * 60)
        print("Step 2: Loading QC FASTA sequences...")
        print("=" * 60)

        # Only compare flagged specimens — no point comparing good ones
        flagged = wb_df[
            wb_df['has_stop_codon'] | wb_df['has_contam'] | wb_df['is_flagged']
        ].copy()
        print(f"  {len(flagged)} flagged specimens to compare")

        if len(flagged) > 0:
            qc_seqs = load_qc_fasta_sequences(
                partner=args.partner, verbose=args.verbose)

            print("\nStep 3: Loading portal sequences for flagged specimens...")
            portal_seqs = load_portal_sequences(
                specimen_ids=set(flagged[WB_SAMPLE_ID_COL].dropna()))

            print("\nStep 4: Comparing sequences...")
            rows = []
            for _, row in flagged.iterrows():
                specimen_id = str(row[WB_SAMPLE_ID_COL]).strip()
                bold_seq    = portal_seqs.get(specimen_id)
                qc_seq      = qc_seqs.get(specimen_id)
                status      = compare_sequences(bold_seq, qc_seq)
                rows.append({
                    'specimen_id':     specimen_id,
                    'plate_id':        row['plate_id'],
                    'partner_code':    row['partner_code'],
                    'has_stop_codon':  row['has_stop_codon'],
                    'has_contam':      row['has_contam'],
                    'is_flagged':      row['is_flagged'],
                    'bold_bin':        row.get(WB_BIN_COL),
                    'sequence_status': status,
                })
            comparison_df = pd.DataFrame(rows)

            # Save comparison
            comp_path = os.path.join(config.RESULTS_DIR,
                                     f'bold_sequence_comparison_{today}.csv')
            comparison_df.to_csv(comp_path, index=False)
            print(f"  Sequence comparison saved: {comp_path}")

            status_counts = comparison_df['sequence_status'].value_counts()
            print(f"\n  Sequence comparison results:")
            for status, count in status_counts.items():
                print(f"    {status:<15}: {count}")

    # ── Plate-level summary ───────────────────────────────────────────────────
    plate_summary = wb_df.groupby('plate_id').agg(
        partner       = ('partner_code', 'first'),
        n_specimens   = (WB_SAMPLE_ID_COL, 'count'),
        n_with_bin    = ('has_bin', 'sum'),
        n_stop_codon  = ('has_stop_codon', 'sum'),
        n_contam      = ('has_contam', 'sum'),
        n_flagged     = ('is_flagged', 'sum'),
        n_compliant   = ('is_compliant', 'sum'),
    ).reset_index()

    # ── Save outputs ──────────────────────────────────────────────────────────
    report_path      = os.path.join(config.RESULTS_DIR,
                                    f'bold_workbench_report_{today}.txt')
    plate_path       = os.path.join(config.RESULTS_DIR,
                                    f'bold_workbench_plates_{today}.csv')
    flagged_path     = os.path.join(config.RESULTS_DIR,
                                    f'bold_workbench_flagged_{today}.csv')

    plate_summary.to_csv(plate_path, index=False)

    flagged_df = wb_df[
        wb_df['has_stop_codon'] | wb_df['has_contam'] | wb_df['is_flagged']
    ][[WB_SAMPLE_ID_COL, 'plate_id', 'partner_code',
       WB_BIN_COL, WB_STOP_CODON_COL, WB_CONTAM_COL,
       WB_FLAGGED_COL, WB_COMPLIANT_COL, WB_SEQ_LENGTH_COL]]
    flagged_df.to_csv(flagged_path, index=False)

    generate_report(wb_df, comparison_df, args.partner, report_path)

    print(f"\nAll outputs written to {config.RESULTS_DIR}:")
    print(f"  {report_path}")
    print(f"  {plate_path}")
    print(f"  {flagged_path}")
    if comparison_df is not None:
        print(f"  {comp_path}")


if __name__ == '__main__':
    main()
