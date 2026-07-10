"""
bold_sequence_concordance.py

Compares sequences currently on BOLD (from portal dump bold_nuc column)
against the sequences in BOLD_filtered_sequences_batch*.fasta files —
the actual FASTA files that were uploaded to BOLD.

Since BOLD_filtered_sequences_batch*.fasta is the direct upload source,
any non-100% match is a genuine discrepancy worth investigating.

For specimens in multiple batches, all are checked and the output records
which batch(es) have an identical match.

Status values:
    IDENTICAL      — 100% match in at least one batch FASTA
    NEAR_IDENTICAL — >99% match (likely minor formatting difference)
    CLOSE          — 95-99% match
    DIFFERENT      — found in FASTA but sequence differs (<95%)
    BOLD_ONLY      — on BOLD but not found in any batch FASTA

Usage:
    python3 bold_sequence_concordance.py --partner CAMP
    python3 bold_sequence_concordance.py --exclude-bge
    python3 bold_sequence_concordance.py --input /path/to/sts_manifests.tsv --exclude-bge
"""

import argparse
import datetime
import glob
import json
import os
import re
import pandas as pd
from collections import defaultdict

import config
from utils import is_bge_plate, resolve_run_dir, resolve_batches

QC_DIR         = '/lustre/scratch126/tol/teams/lawniczak/projects/bioscan/bioscan_qc/qc_reports_rerun_Feb2026'
_FASTA_PATTERN = 'BOLD_filtered_sequences_batch*.fasta'

_SPECIMEN_COL    = 'sts_specimen.id'
_RACK_COL        = 'sts_rackid'
_BOLD_NUC_COL    = 'bold_nuc'
_BOLD_UPLOAD_COL = 'bold_sequence_upload_date'
_BOLD_BIN_COL    = 'bold_bin_uri'
_PARTNER_COL     = 'bold_bold_recordset_code_arr'


def clean_seq(seq):
    if not seq or str(seq) in ('None', 'nan', ''):
        return None
    return re.sub(r'\s', '', str(seq).upper())


def pct_identity(seq1, seq2):
    """Strict position-by-position percent identity, shorter length as denominator."""
    if not seq1 or not seq2:
        return 0.0
    n = min(len(seq1), len(seq2))
    if n == 0:
        return 0.0
    matches = sum(a == b for a, b in zip(seq1[:n], seq2[:n]))
    return round(100 * matches / n, 4)


def clean_partner(val):
    if not val or str(val) in ('None', 'nan', ''):
        return None
    tokens = re.findall(r"['\"]([^'\"]+)['\"]", str(val))
    if not tokens:
        tokens = [str(val).strip().strip("[]'\"")]
    for token in tokens:
        token = token.strip()
        if token.startswith('DS-') or not token:
            continue
        if re.match(r'[A-Z][A-Z]{4}$', token):
            token = token[1:]
        if re.match(r'^[A-Z]{4}$', token):
            return token
    return None


def extract_partner_from_rack(rack_id):
    if not rack_id:
        return None
    m = re.match(r'^(?:TOL-)?([A-Z]{4})[_-]', str(rack_id))
    return m.group(1) if m else None


def find_fasta_files(qc_dir):
    """Return {batch_name: fasta_path} for all resolved batches including special
    (RnD, repeat sub-batches). Uses resolve_batches so the logic stays consistent
    with the rest of the pipeline — critical once repeat-batch sequences are on BOLD."""
    resolved, _ = resolve_batches(qc_dir, include_special=True)
    result = {}
    for batch in resolved:
        batch_path = os.path.join(qc_dir, batch)
        files = glob.glob(os.path.join(batch_path, _FASTA_PATTERN))
        if files:
            result[batch] = files[0]
    return result


def parse_fasta(filepath, partner_filter=None):
    result = {}
    current_id = None
    current_seq = []
    try:
        with open(filepath) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if line.startswith('>'):
                    if current_id is not None:
                        result[current_id] = ''.join(current_seq).upper()
                    current_id = line[1:].split()[0]
                    current_seq = []
                else:
                    current_seq.append(line)
            if current_id is not None:
                result[current_id] = ''.join(current_seq).upper()
    except Exception as e:
        print(f"  Warning: could not read {filepath}: {e}")
        return {}
    if partner_filter:
        result = {k: v for k, v in result.items()
                  if k.startswith(partner_filter + '_') or
                  k.startswith('TOL-' + partner_filter + '-')}
    return result


def find_latest_dump(results_dir):
    candidates = sorted(glob.glob(os.path.join(results_dir, 'sts_manifests_*.tsv')))
    return candidates[-1] if candidates else None


def load_portal_sequences(dump_path, partner_filter=None, exclude_bge=False):
    print(f"Reading portal dump: {os.path.basename(dump_path)}")
    peek = pd.read_csv(dump_path, sep='\t', dtype=str, nrows=0)
    use_cols = [c for c in [_SPECIMEN_COL, _RACK_COL, _BOLD_NUC_COL,
                             _BOLD_UPLOAD_COL, _BOLD_BIN_COL, _PARTNER_COL]
                if c in peek.columns]
    df = pd.read_csv(dump_path, sep='\t', dtype=str, usecols=use_cols, low_memory=False)
    print(f"  {len(df):,} rows loaded")

    for col in df.columns:
        df[col] = df[col].replace({'None': None, 'nan': None, '': None})

    df['plate_id'] = df[_RACK_COL] if _RACK_COL in df.columns else None
    df['partner'] = df[_PARTNER_COL].apply(clean_partner) if _PARTNER_COL in df.columns else None
    missing = df['partner'].isna()
    if missing.any():
        df.loc[missing, 'partner'] = df.loc[missing, 'plate_id'].apply(extract_partner_from_rack)

    if exclude_bge:
        n = len(df)
        df = df[~df['plate_id'].apply(lambda x: is_bge_plate(str(x)) if x else False)]
        print(f"  Excluded {n - len(df):,} BGE rows")

    if partner_filter:
        df = df[df['partner'] == partner_filter]
        print(f"  Filtered to {partner_filter}: {len(df):,} rows")

    df = df[df[_BOLD_NUC_COL].notna()]
    print(f"  {len(df):,} specimens with BOLD sequence")

    df = df.rename(columns={
        _SPECIMEN_COL: 'specimen_id',
        _BOLD_NUC_COL: 'bold_nuc',
        _BOLD_UPLOAD_COL: 'bold_upload_date',
        _BOLD_BIN_COL: 'bold_bin_uri',
    })
    return df[['specimen_id', 'plate_id', 'partner',
               'bold_nuc', 'bold_upload_date', 'bold_bin_uri']].copy()


def run_comparison(portal_df, fasta_files, verbose=False):
    total = len(portal_df)
    print(f"\nComparing {total:,} BOLD specimens against {len(fasta_files)} batch FASTA files...")
    print("Loading FASTA sequences...")

    specimen_ids = set(portal_df['specimen_id'].dropna())
    fasta_index = defaultdict(dict)

    for i, (batch, filepath) in enumerate(sorted(fasta_files.items())):
        seqs = parse_fasta(filepath)
        found = 0
        for pid, seq in seqs.items():
            if pid in specimen_ids:
                fasta_index[pid][batch] = seq
                found += 1
        if verbose and found > 0:
            print(f"  {batch}: {found} matching specimens")
        elif (i + 1) % 20 == 0:
            print(f"  Processed {i+1}/{len(fasta_files)} FASTA files...")

    print(f"\n  {len(fasta_index):,} specimens found in batch FASTA files")
    print(f"  {total - len(fasta_index):,} on BOLD but not in any FASTA")
    print(f"\nRunning comparisons...")

    results = []
    for _, row in portal_df.iterrows():
        sid      = row['specimen_id']
        bold_seq = clean_seq(row['bold_nuc'])
        bold_len = len(bold_seq) if bold_seq else 0
        batch_data = fasta_index.get(sid, {})
        n_batches  = len(batch_data)

        if n_batches == 0:
            results.append({
                'specimen_id':        sid,
                'plate_id':           row['plate_id'],
                'partner':            row['partner'],
                'bold_upload_date':   row.get('bold_upload_date'),
                'bold_bin_uri':       row.get('bold_bin_uri'),
                'bold_seq_length':    bold_len,
                'n_batches_in_fasta': 0,
                'batches_in_fasta':   '',
                'any_identical':      False,
                'identical_batches':  '',
                'best_pct_identity':  None,
                'best_batch':         '',
                'fasta_seq_length':   None,
                'length_diff':        None,
                'all_pct_identities': '{}',
                'status':             'BOLD_ONLY',
            })
            continue

        identities    = {}
        fasta_lengths = {}
        for batch, fasta_seq in batch_data.items():
            fasta_clean = clean_seq(fasta_seq)
            if bold_seq and fasta_clean:
                identities[batch]    = pct_identity(bold_seq, fasta_clean)
                fasta_lengths[batch] = len(fasta_clean)
            else:
                identities[batch]    = 0.0
                fasta_lengths[batch] = None

        # Compute best batch first, then overlap identities
        identical_batches = [b for b, p in identities.items() if p == 100.0]
        best_batch  = max(identities, key=identities.get) if identities else ''
        best_pct    = identities[best_batch] if best_batch else None
        best_flen   = fasta_lengths.get(best_batch)
        lendiff     = (bold_len - best_flen) if (bold_len and best_flen) else None

        # For the best batch, compute overlap identities:
        # left-aligned (trim 3' end of longer) and right-aligned (trim 5' end of longer)
        # These directly answer: "if you remove the length difference, is the sequence identical?"
        best_fasta_seq = clean_seq(batch_data.get(best_batch, '')) if best_batch else None
        if bold_seq and best_fasta_seq:
            n = min(len(bold_seq), len(best_fasta_seq))
            overlap_left  = round(100 * sum(a==b for a,b in zip(
                bold_seq[:n], best_fasta_seq[:n])) / n, 4) if n else None
            overlap_right = round(100 * sum(a==b for a,b in zip(
                bold_seq[-n:], best_fasta_seq[-n:])) / n, 4) if n else None
        else:
            overlap_left = overlap_right = None

        if identical_batches:
            status = 'IDENTICAL'
        elif overlap_right == 100.0:
            status = 'TRIM_5PRIME'    # extra bases at 5' end of FASTA only
        elif overlap_left == 100.0:
            status = 'TRIM_3PRIME'    # extra bases at 3' end of FASTA only
        elif (overlap_right is not None and overlap_right >= 99.0) or \
             (overlap_left  is not None and overlap_left  >= 99.0):
            status = 'NEAR_IDENTICAL'
        elif best_pct is not None and best_pct >= 95.0:
            status = 'CLOSE'
        elif best_pct is not None and best_pct > 0:
            status = 'DIFFERENT'      # genuinely different sequence from QC rerun
        else:
            status = 'NO_SEQUENCE'

        results.append({
            'specimen_id':        sid,
            'plate_id':           row['plate_id'],
            'partner':            row['partner'],
            'bold_upload_date':   row.get('bold_upload_date'),
            'bold_bin_uri':       row.get('bold_bin_uri'),
            'bold_seq_length':    bold_len,
            'n_batches_in_fasta': n_batches,
            'batches_in_fasta':   ','.join(sorted(batch_data.keys())),
            'any_identical':      bool(identical_batches),
            'identical_batches':  ','.join(sorted(identical_batches)),
            'best_pct_identity':  best_pct,
            'best_batch':         best_batch,
            'fasta_seq_length':   best_flen,
            'length_diff':        lendiff,
            'all_pct_identities':  json.dumps(identities),
            'overlap_pct_left':    overlap_left,   # identity if left-aligned (trim 3' end)
            'overlap_pct_right':   overlap_right,  # identity if right-aligned (trim 5' end)
            'status':              status,
        })

    return pd.DataFrame(results)


def print_summary(df, partner=None):
    label = f" ({partner})" if partner else ""
    total = len(df)
    print(f"\n{'='*60}")
    print(f"BOLD SEQUENCE CONCORDANCE SUMMARY{label}")
    print(f"{'='*60}")
    print(f"Total specimens on BOLD: {total:,}")
    print()
    for status, n in df['status'].value_counts().items():
        print(f"  {status:20s}: {n:6,} ({100*n/total:.1f}%)")

    non_id = df[df['status'].isin(['NEAR_IDENTICAL', 'CLOSE', 'DIFFERENT'])]
    if len(non_id) > 0:
        print(f"\nNon-identical sequences ({len(non_id):,}):")
        pcts = non_id['best_pct_identity'].dropna().astype(float)
        print(f"  Mean:   {pcts.mean():.4f}%")
        print(f"  Median: {pcts.median():.4f}%")
        print(f"  Min:    {pcts.min():.4f}%")
        print()
        ld = non_id['length_diff'].dropna().astype(float)
        if len(ld):
            print(f"  Length diff (BOLD - FASTA): mean {ld.mean():.1f}bp, range {ld.min():.0f} to {ld.max():.0f}bp")
        print()
        print("  By partner:")
        print(non_id.groupby('partner').size().sort_values(ascending=False).to_string())
        print()
        print("  Most divergent:")
        worst = non_id.nsmallest(10, 'best_pct_identity')
        print(worst[['specimen_id', 'partner', 'best_pct_identity',
                      'bold_seq_length', 'fasta_seq_length',
                      'length_diff', 'best_batch']].to_string(index=False))

    bold_only = df[df['status'] == 'BOLD_ONLY']
    if len(bold_only) > 0:
        print(f"\nBOLD_ONLY ({len(bold_only):,}) — by partner:")
        print(bold_only.groupby('partner').size().sort_values(ascending=False).to_string())


def main():
    parser = argparse.ArgumentParser(
        description='Compare BOLD sequences vs BOLD_filtered_sequences FASTA files'
    )
    parser.add_argument('--partner', default=None,
        help='Single partner filter e.g. CAMP')
    parser.add_argument('--exclude-bge', action='store_true',
        help='Exclude BGE partners')
    parser.add_argument('--input', default=None,
        help='Portal dump TSV path')
    parser.add_argument('--output', default=None,
        help='Output CSV path')
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--run-dir', default=None,
        help='Output directory (overrides BIOSCAN_RUN_DIR env var and auto-generate)')
    args = parser.parse_args()

    run_dir = resolve_run_dir(args.run_dir)
    run_ts  = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

    dump_path = args.input or find_latest_dump(config.RESULTS_DIR)
    if not dump_path or not os.path.exists(dump_path):
        print("ERROR: No portal dump found.")
        return

    suffix = f"_{args.partner}" if args.partner else "_ALL"
    if args.output is None:
        args.output = os.path.join(run_dir, f'bold_sequence_concordance{suffix}_{run_ts}.csv')

    portal_df = load_portal_sequences(dump_path,
                                      partner_filter=args.partner,
                                      exclude_bge=args.exclude_bge)
    if portal_df.empty:
        print("No specimens found.")
        return

    print(f"\nScanning FASTA files in: {QC_DIR}")
    fasta_files = find_fasta_files(QC_DIR)
    print(f"  Found {len(fasta_files)} FASTA files")

    results = run_comparison(portal_df, fasta_files, verbose=args.verbose)
    print_summary(results, partner=args.partner)

    # Save only non-identical specimens to CSV — actionable cases only
    # IDENTICAL rows are captured in the summary printed above
    identical_statuses = {'IDENTICAL'}
    to_save = results[~results['status'].isin(identical_statuses)].copy()
    to_save.to_csv(args.output, index=False)

    # Also save a summary TXT alongside the CSV
    summary_path = args.output.replace('.csv', '_summary.txt')
    with open(summary_path, 'w') as f:
        f.write(f'BOLD SEQUENCE CONCORDANCE SUMMARY\n')
        f.write(f'Generated: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}\n')
        f.write(f'Partner filter: {args.partner or "ALL (BGE excluded)"}\n')
        f.write(f'Portal dump: {os.path.basename(dump_path)}\n')
        f.write(f'\nTotal specimens on BOLD: {len(results):,}\n')
        for status, n in results['status'].value_counts().items():
            f.write(f'  {status:20s}: {n:6,} ({100*n/len(results):.1f}%)\n')
        f.write(f'\nNon-identical saved to: {args.output}\n')
        f.write(f'  {len(to_save):,} rows (excludes IDENTICAL)\n')

    print(f'\nCSV output (non-identical only): {args.output}')
    print(f'  {len(to_save):,} rows saved (IDENTICAL excluded)')
    print(f'Summary log: {summary_path}')


if __name__ == '__main__':
    main()
