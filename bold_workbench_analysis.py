"""
bold_workbench_analysis.py

Two analyses in one script:

ROUTINE (default):
  Analyses flagged specimens from BOLD workbench exports.
  For specimens with Stop Codon / Contamination / Flagged Record status,
  compares the sequence currently on BOLD (from portal dump bold_nuc) against
  the sequence that passed QC (from BOLD_filtered_sequences_batchN.fasta).
  DIFFERENT = QC has found a better sequence -> BOLD needs updating.
  IDENTICAL = same sequence -> flag is genuine, no update needed.

AD HOC (--full-concordance):
  Full sense check across ALL specimens on BOLD.
  Confirms every sequence on BOLD exactly matches the QC FASTA.
  Run occasionally to catch any drift between QC output and BOLD records.

Input files (place in RESULTS_DIR):
    bold_workbench_2021.csv  ┐
    bold_workbench_2022.csv  │  BOLD workbench exports filtered by year
    bold_workbench_2023.csv  │  Skip 2 header rows (header=2)
    bold_workbench_2024.csv  │  Columns: Sample ID, BIN, Stop Codon,
    bold_workbench_2025.csv  │  Contamination, Flagged Record, Barcode Compliant
    bold_workbench_2026.csv  ┘

Portal dump (for BOLD sequences):
    config.PORTAL_DUMP_TSV -> bold_nuc column

QC FASTA (for QC-passed sequences):
    BOLD_filtered_sequences_batchN.fasta in each QC batch folder
    These are exactly what was submitted to BOLD.

Usage:
    # Routine — flagged specimens only
    python3 bold_workbench_analysis.py
    python3 bold_workbench_analysis.py --partner FACE

    # Ad hoc — full concordance check
    python3 bold_workbench_analysis.py --full-concordance
    python3 bold_workbench_analysis.py --full-concordance --partner BGEP

    # Rebuild workbench cache if new year files added
    python3 bold_workbench_analysis.py --rebuild-cache

    # Skip sequence comparison (flag summary only)
    python3 bold_workbench_analysis.py --skip-sequence-comparison
"""

import argparse
import datetime
import glob
import os
import re
import pandas as pd
from collections import defaultdict

import config
from utils import resolve_batches, matches_partner


# ── Constants ─────────────────────────────────────────────────────────────────

WORKBENCH_YEARS    = [2021, 2022, 2023, 2024, 2025, 2026]
WORKBENCH_PATTERN  = "bold_workbench_{year}.csv"
COMBINED_CACHE     = "bold_workbench_combined.csv"
BOLD_FASTA_PATTERN = "BOLD_filtered_sequences_batch*.fasta"

# Workbench columns
WB_SAMPLE_ID  = 'Sample ID'
WB_PROCESS_ID = 'Process ID'
WB_PROJECT    = 'Project Code'
WB_BIN        = 'BIN'
WB_STOP_CODON = 'Stop Codon'
WB_CONTAM     = 'Contamination'
WB_FLAGGED    = 'Flagged Record'
WB_COMPLIANT  = 'Barcode Compliant'
WB_SEQ_LEN    = 'COI-5P Seq. Length'

# Portal dump columns
PORTAL_SPECIMEN = 'sts_specimen.id'
PORTAL_BOLD_NUC = 'bold_nuc'


# ── Helpers ───────────────────────────────────────────────────────────────────

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


def _clean_seq(seq):
    """Strip gaps, whitespace, return uppercase."""
    if not seq or pd.isna(seq):
        return None
    s = re.sub(r'[-\s]', '', str(seq)).upper()
    return s if len(s) > 10 else None


def compare_sequences(bold_seq, qc_seq):
    """
    Compare BOLD sequence vs QC sequence.
    Returns: 'IDENTICAL' | 'DIFFERENT' | 'QC_ONLY' | 'BOLD_ONLY' | 'NEITHER'
    """
    b = _clean_seq(bold_seq)
    q = _clean_seq(qc_seq)
    if not b and not q:
        return 'NEITHER'
    if b and not q:
        return 'BOLD_ONLY'
    if q and not b:
        return 'QC_ONLY'
    return 'IDENTICAL' if b == q else 'DIFFERENT'


# ── Step 1: Load workbench ────────────────────────────────────────────────────

def load_workbench(results_dir, rebuild_cache=False, verbose=False):
    """Load and combine annual workbench files. Caches result."""
    cache_path = os.path.join(results_dir, COMBINED_CACHE)

    if os.path.exists(cache_path) and not rebuild_cache:
        print(f"Loading cached workbench: {cache_path}")
        df = pd.read_csv(cache_path, dtype=str, low_memory=False)
        print(f"  {len(df)} records")
        return df

    print("Reading annual workbench files...")
    dfs = []
    for year in WORKBENCH_YEARS:
        # Match bold_workbench_2024.xlsx/csv OR bold_workbench_2024a.xlsx/csv etc.
        year_files = sorted(
            glob.glob(os.path.join(results_dir, f"bold_workbench_{year}*.xlsx")) +
            glob.glob(os.path.join(results_dir, f"bold_workbench_{year}*.csv"))
        )
        # Exclude the combined cache file
        year_files = [f for f in year_files if 'combined' not in os.path.basename(f)]
        if not year_files:
            if verbose:
                print(f"  {year}: not found")
            continue
        for path in year_files:
            try:
                ext = os.path.splitext(path)[1].lower()
                if ext in ('.xlsx', '.xls'):
                    df = pd.read_excel(path, sheet_name='Lab Sheet',
                                       header=2, dtype=str)
                else:
                    df = pd.read_csv(path, header=2, dtype=str,
                                     low_memory=False)
                df['source_year'] = str(year)
                dfs.append(df)
                print(f"  {os.path.basename(path)}: {len(df)} records")
            except Exception as e:
                print(f"  {os.path.basename(path)}: ERROR — {e}")

    if not dfs:
        raise FileNotFoundError(
            f"No workbench files found in {results_dir}\n"
            f"Expected: bold_workbench_YYYY.csv or bold_workbench_YYYYa.csv etc."
        )

    combined = pd.concat(dfs, ignore_index=True)
    combined = combined.sort_values('source_year', ascending=False)
    combined = combined.drop_duplicates(subset=WB_SAMPLE_ID, keep='first')
    combined = combined.reset_index(drop=True)
    print(f"  Combined: {len(combined)} unique specimens")

    combined.to_csv(cache_path, index=False)
    print(f"  Cached to: {cache_path}")
    return combined


def enrich_workbench(wb_df, partner=None):
    """Add plate_id, partner_code and boolean quality flag columns."""
    wb_df = wb_df.copy()
    wb_df['plate_id']     = wb_df[WB_SAMPLE_ID].apply(
        lambda s: re.sub(r'_[^_]+$', '', str(s)) if pd.notna(s) else None)
    wb_df['partner_code'] = wb_df['plate_id'].apply(_extract_partner)

    wb_df['has_bin']       = wb_df[WB_BIN].notna() & \
                             (wb_df[WB_BIN].str.strip() != '') & \
                             (wb_df[WB_BIN] != 'None')
    wb_df['has_stop_codon']= wb_df[WB_STOP_CODON].notna() & \
                             (wb_df[WB_STOP_CODON].str.strip() != '')
    wb_df['has_contam']    = wb_df[WB_CONTAM].notna() & \
                             (wb_df[WB_CONTAM].str.strip() != '')
    wb_df['is_flagged']    = wb_df[WB_FLAGGED].str.strip().str.lower() == 'yes'
    wb_df['is_compliant']  = wb_df[WB_COMPLIANT].str.strip().str.lower() == 'yes'
    wb_df['any_flag']      = wb_df['has_stop_codon'] | wb_df['has_contam'] | \
                             wb_df['is_flagged']

    if partner and partner.upper() != 'ALL':
        wb_df = wb_df[wb_df['partner_code'] == partner.upper()]
        print(f"  Filtered to partner '{partner}': {len(wb_df)} records")

    return wb_df


# ── Step 2: Load QC FASTA sequences ──────────────────────────────────────────

def load_qc_fasta_sequences(qc_dir=None, partner=None, verbose=False):
    """
    Load BOLD_filtered_sequences FASTA files from all QC batch folders.
    These are exactly what was submitted to BOLD.
    Returns dict: specimen_id -> sequence
    """
    if qc_dir is None:
        qc_dir = config.QC_DIR

    resolved, _ = resolve_batches(qc_dir)
    sequences = {}

    for batch_folder in resolved:
        batch_path = os.path.join(qc_dir, batch_folder)
        fasta_files = glob.glob(os.path.join(batch_path, BOLD_FASTA_PATTERN))
        for fasta_file in fasta_files:
            try:
                for specimen_id, seq in _parse_fasta(fasta_file).items():
                    plate = re.sub(r'_[^_]+$', '', specimen_id)
                    if partner and partner.upper() != 'ALL':
                        if _extract_partner(plate) != partner.upper():
                            continue
                    if specimen_id not in sequences:
                        sequences[specimen_id] = seq
            except Exception as e:
                if verbose:
                    print(f"  WARNING: {fasta_file}: {e}")

    print(f"  {len(sequences)} QC sequences loaded from {len(resolved)} batches")
    return sequences


def _parse_fasta(path):
    seqs = {}
    current_id, current_seq = None, []
    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith('>'):
                if current_id:
                    seqs[current_id] = ''.join(current_seq).upper()
                current_id = line[1:].split()[0].split('|')[0].strip()
                current_seq = []
            elif line:
                current_seq.append(line)
    if current_id:
        seqs[current_id] = ''.join(current_seq).upper()
    return seqs


# ── Step 3: Load portal sequences ────────────────────────────────────────────

def load_portal_sequences(specimen_ids=None, dump_path=None):
    """Load bold_nuc from portal dump. Returns dict: specimen_id -> sequence."""
    if dump_path is None:
        dump_path = config.PORTAL_DUMP_TSV
    print(f"  Loading portal sequences...")
    df = pd.read_csv(dump_path, sep='\t', dtype=str,
                     usecols=[PORTAL_SPECIMEN, PORTAL_BOLD_NUC],
                     low_memory=False)
    if specimen_ids:
        df = df[df[PORTAL_SPECIMEN].isin(specimen_ids)]
    df = df[df[PORTAL_BOLD_NUC].notna() &
            (df[PORTAL_BOLD_NUC] != 'None') &
            (df[PORTAL_BOLD_NUC].str.len() > 10)]
    result = dict(zip(df[PORTAL_SPECIMEN], df[PORTAL_BOLD_NUC].str.upper()))
    print(f"  {len(result)} portal sequences loaded")
    return result


# ── Step 4: Run comparison ────────────────────────────────────────────────────

def run_sequence_comparison(specimens_df, qc_seqs, portal_seqs, mode):
    """
    Compare sequences for specimens_df.
    mode: 'flagged' or 'full'
    Returns DataFrame with comparison results.
    """
    rows = []
    for _, row in specimens_df.iterrows():
        sid       = str(row[WB_SAMPLE_ID]).strip()
        bold_seq  = portal_seqs.get(sid)
        qc_seq    = qc_seqs.get(sid)
        status    = compare_sequences(bold_seq, qc_seq)
        rows.append({
            'specimen_id':    sid,
            'plate_id':       row.get('plate_id'),
            'partner_code':   row.get('partner_code'),
            'has_stop_codon': row.get('has_stop_codon', False),
            'has_contam':     row.get('has_contam', False),
            'is_flagged':     row.get('is_flagged', False),
            'has_bin':        row.get('has_bin', False),
            'bold_bin':       row.get(WB_BIN),
            'seq_length_bold': row.get(WB_SEQ_LEN),
            'sequence_status': status,
            'comparison_mode': mode,
        })
    return pd.DataFrame(rows)


# ── Step 5: Report ────────────────────────────────────────────────────────────

def generate_report(wb_df, flagged_comp, full_comp, partner, output_path):
    lines = []
    def h(t): lines.extend(['', '=' * 65, t, '=' * 65])
    def s(t): lines.extend(['', f'--- {t} ---'])

    h("BOLD WORKBENCH ANALYSIS REPORT")
    lines.append(f"Generated : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"Partner   : {partner}")
    lines.append(f"Total workbench records: {len(wb_df)}")

    h("OVERALL QUALITY FLAGS")
    lines.append(f"  Total specimens on BOLD   : {len(wb_df)}")
    lines.append(f"  With BIN URI              : {wb_df['has_bin'].sum()}")
    lines.append(f"  Without BIN URI           : {(~wb_df['has_bin']).sum()}")
    lines.append(f"  Has stop codon flag       : {wb_df['has_stop_codon'].sum()}")
    lines.append(f"  Has contamination flag    : {wb_df['has_contam'].sum()}")
    lines.append(f"  Flagged record            : {wb_df['is_flagged'].sum()}")
    lines.append(f"  Barcode compliant         : {wb_df['is_compliant'].sum()}")
    lines.append(f"  Any flag (stop/contam/flagged): {wb_df['any_flag'].sum()}")

    h("FLAGS BY PARTNER")
    pg = wb_df.groupby('partner_code').agg(
        n_total      =('has_bin','count'),
        n_with_bin   =('has_bin','sum'),
        n_stop_codon =('has_stop_codon','sum'),
        n_contam     =('has_contam','sum'),
        n_flagged    =('is_flagged','sum'),
        n_compliant  =('is_compliant','sum'),
    ).reset_index().sort_values('n_flagged', ascending=False)
    lines.append(f"  {'Partner':<8} {'Total':>7} {'BIN':>6} "
                f"{'StopCdn':>8} {'Contam':>7} {'Flagged':>8} {'Compliant':>10}")
    lines.append(f"  {'-'*8} {'-'*7} {'-'*6} {'-'*8} {'-'*7} {'-'*8} {'-'*10}")
    for _, row in pg.iterrows():
        lines.append(f"  {str(row['partner_code']):<8} {int(row['n_total']):>7} "
                    f"{int(row['n_with_bin']):>6} {int(row['n_stop_codon']):>8} "
                    f"{int(row['n_contam']):>7} {int(row['n_flagged']):>8} "
                    f"{int(row['n_compliant']):>10}")

    if flagged_comp is not None and len(flagged_comp) > 0:
        h("ROUTINE CHECK — FLAGGED SPECIMENS: BOLD vs QC SEQUENCE")
        lines.append("  Compares BOLD sequence vs QC-passed FASTA for flagged specimens.")
        lines.append("  DIFFERENT = QC has a better sequence -> BOLD needs updating")
        lines.append("  IDENTICAL  = same sequence -> flag is genuine quality issue")
        lines.append("")
        for status, count in flagged_comp['sequence_status'].value_counts().items():
            lines.append(f"  {status:<15}: {count}")

        needs_update = flagged_comp[flagged_comp['sequence_status'] == 'DIFFERENT']
        s(f"Specimens needing BOLD update: {len(needs_update)}")
        if len(needs_update) > 0:
            for p, n in needs_update.groupby('partner_code').size().sort_values(ascending=False).items():
                lines.append(f"  {p:<8}: {n}")

    if full_comp is not None and len(full_comp) > 0:
        h("AD HOC CHECK — FULL CONCORDANCE: ALL BOLD SEQUENCES vs QC FASTA")
        lines.append("  Sense check: every sequence on BOLD vs QC FASTA output.")
        lines.append("")
        total = len(full_comp)
        for status, count in full_comp['sequence_status'].value_counts().items():
            pct = 100 * count // total if total else 0
            lines.append(f"  {status:<15}: {count:>7}  ({pct}%)")

        diff = full_comp[full_comp['sequence_status'] == 'DIFFERENT']
        s(f"Discrepancies (BOLD != QC FASTA): {len(diff)}")
        if len(diff) > 0:
            lines.append("  By partner:")
            for p, n in diff.groupby('partner_code').size().sort_values(ascending=False).items():
                lines.append(f"    {p:<8}: {n}")

    report = '\n'.join(lines)
    print(report)
    with open(output_path, 'w') as f:
        f.write(report)
    print(f"\nReport written to: {output_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='BOLD workbench quality flag analysis and sequence concordance'
    )
    parser.add_argument('--partner', default='ALL')
    parser.add_argument('--full-concordance', action='store_true',
        help='Ad hoc: compare ALL specimens on BOLD vs QC FASTA (slow)')
    parser.add_argument('--skip-sequence-comparison', action='store_true',
        help='Flag summary only — no sequence loading or comparison')
    parser.add_argument('--rebuild-cache', action='store_true',
        help='Force re-read of annual workbench files')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    today = datetime.datetime.now().strftime('%Y%m%d')
    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    # Load workbench
    print("=" * 60)
    print("Step 1: Loading BOLD workbench...")
    print("=" * 60)
    wb_df = load_workbench(config.RESULTS_DIR,
                           rebuild_cache=args.rebuild_cache,
                           verbose=args.verbose)
    wb_df = enrich_workbench(wb_df, partner=args.partner)

    flagged_comp = None
    full_comp    = None

    if not args.skip_sequence_comparison:
        print("\n" + "=" * 60)
        print("Step 2: Loading QC FASTA sequences...")
        print("=" * 60)
        qc_seqs = load_qc_fasta_sequences(
            partner=args.partner, verbose=args.verbose)

        # ── Routine: flagged specimens ─────────────────────────────────────
        flagged_df = wb_df[wb_df['any_flag']].copy()
        print(f"\n{len(flagged_df)} flagged specimens (routine check)")

        if len(flagged_df) > 0:
            print("\nStep 3a: Loading portal sequences for flagged specimens...")
            portal_seqs = load_portal_sequences(
                specimen_ids=set(flagged_df[WB_SAMPLE_ID].dropna()))
            flagged_comp = run_sequence_comparison(
                flagged_df, qc_seqs, portal_seqs, mode='flagged')
            flagged_path = os.path.join(config.RESULTS_DIR,
                f'bold_flagged_comparison_{today}.csv')
            flagged_comp.to_csv(flagged_path, index=False)
            print(f"  Flagged comparison saved: {flagged_path}")

        # ── Ad hoc: full concordance ───────────────────────────────────────
        if args.full_concordance:
            print("\nStep 3b: Full concordance — loading ALL portal sequences...")
            all_portal_seqs = load_portal_sequences(
                specimen_ids=set(wb_df[WB_SAMPLE_ID].dropna()))
            print(f"Comparing {len(wb_df)} specimens...")
            full_comp = run_sequence_comparison(
                wb_df, qc_seqs, all_portal_seqs, mode='full')
            full_path = os.path.join(config.RESULTS_DIR,
                f'bold_full_concordance_{today}.csv')
            full_comp.to_csv(full_path, index=False)
            print(f"  Full concordance saved: {full_path}")


    # ── Cross-analysis: DIFFERENT vs missing BIN list ────────────────────────
    if flagged_comp is not None and len(flagged_comp) > 0:
        print("\n" + "=" * 60)
        print("Step: Cross-analysis — flagged comparison vs missing BIN list")
        print("=" * 60)

        import glob as _glob
        bin_files = sorted(_glob.glob(
            os.path.join(config.RESULTS_DIR, 'bold_missing_bin_*.csv')))

        if bin_files:
            no_bin_df  = pd.read_csv(bin_files[-1], dtype=str)
            no_bin_ids = set(no_bin_df['sts_specimen.id'].str.strip())

            # DIFFERENT + no BIN = resubmit to BOLD with better QC sequence
            diff       = flagged_comp[flagged_comp['sequence_status'] == 'DIFFERENT'].copy()
            actionable = diff[diff['specimen_id'].isin(no_bin_ids)].copy()
            actionable = actionable.merge(
                no_bin_df[['sts_specimen.id','upload_date','submit_date']].rename(
                    columns={'sts_specimen.id':'specimen_id'}),
                on='specimen_id', how='left')

            resub_path = os.path.join(config.RESULTS_DIR,
                                      f'bold_needs_resubmission_{today}.csv')
            actionable.to_csv(resub_path, index=False)
            print(f"  {len(actionable)} specimens: no BIN + flagged + QC has better sequence")
            print(f"  Saved: {resub_path}")
            print(f"  These should be resubmitted to BOLD with the QC sequence")
            print(f"\n  By partner:")
            for p, n in actionable.groupby('partner_code').size().sort_values(
                    ascending=False).items():
                print(f"    {p:<8}: {n}")

            # IDENTICAL + flagged = genuine flag, no better sequence, needs manual review
            identical = flagged_comp[flagged_comp['sequence_status'] == 'IDENTICAL'].copy()
            identical = identical.merge(
                no_bin_df[['sts_specimen.id','upload_date','submit_date']].rename(
                    columns={'sts_specimen.id':'specimen_id'}),
                on='specimen_id', how='left')

            identical_path = os.path.join(config.RESULTS_DIR,
                                          f'bold_flagged_no_alternative_{today}.csv')
            identical.to_csv(identical_path, index=False)
            print(f"\n  {len(identical)} specimens: flagged, IDENTICAL sequence in QC FASTA")
            print(f"  Flag is genuine — no better sequence available")
            print(f"  Saved: {identical_path}")
            print(f"  These need manual sequence assessment")
            print(f"\n  By flag type:")
            print(f"    stop_codon : {identical['has_stop_codon'].astype(str).eq('True').sum()}")
            print(f"    contam     : {identical['has_contam'].astype(str).eq('True').sum()}")
            print(f"    is_flagged : {identical['is_flagged'].astype(str).eq('True').sum()}")
            print(f"\n  By partner:")
            for p, n in identical.groupby('partner_code').size().sort_values(
                    ascending=False).items():
                print(f"    {p:<8}: {n}")
        else:
            print("  WARNING: no bold_missing_bin_*.csv found.")
            print("  Run bold_summary_from_portal.py first.")

    # Plate summary
    plate_summary = wb_df.groupby('plate_id').agg(
        partner      =('partner_code','first'),
        n_specimens  =(WB_SAMPLE_ID,'count'),
        n_with_bin   =('has_bin','sum'),
        n_stop_codon =('has_stop_codon','sum'),
        n_contam     =('has_contam','sum'),
        n_flagged    =('is_flagged','sum'),
        n_any_flag   =('any_flag','sum'),
    ).reset_index()

    plate_path = os.path.join(config.RESULTS_DIR,
                              f'bold_workbench_plates_{today}.csv')
    plate_summary.to_csv(plate_path, index=False)

    report_path = os.path.join(config.RESULTS_DIR,
                               f'bold_workbench_report_{today}.txt')
    generate_report(wb_df, flagged_comp, full_comp,
                    args.partner, report_path)

    print(f"\nOutputs:")
    print(f"  {report_path}       <- full flag report")
    print(f"  {plate_path}  <- plate-level flag counts")
    if flagged_comp is not None:
        print(f"  bold_needs_resubmission_{today}.csv   <- BOLD records to update (DIFFERENT + no BIN)")
        print(f"  bold_flagged_no_alternative_{today}.csv <- genuine flags, manual review needed (IDENTICAL)")


if __name__ == '__main__':
    main()
