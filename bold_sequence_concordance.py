"""
bold_sequence_concordance.py

Compares sequences uploaded to BOLD (from portal dump bold_nuc column)
against QC sequences in BOLDfiltered_metadata_batch*.csv files.

For each specimen on BOLD:
  - Finds all BOLDfiltered batch files containing that specimen
  - Compares the BOLD sequence against the QC sequence in each batch
  - Reports: identical match, % identity, which batch(es) match

Output columns:
    specimen_id         — e.g. BAYS_067_A1
    plate_id            — e.g. BAYS_067
    partner             — e.g. BAYS
    bold_upload_date    — date sequence was uploaded to BOLD
    bold_seq_length     — length of sequence on BOLD
    n_batches_in_qc     — how many BOLDfiltered files contain this specimen
    batches_in_qc       — comma-separated list of batch names
    any_identical       — True if any batch has 100% identity
    identical_batches   — which batches have identical sequence
    best_pct_identity   — highest % identity across all batches
    best_batch          — batch with highest % identity
    all_pct_identities  — per-batch % identities (JSON)
    status              — IDENTICAL / CLOSE (>95%) / DIVERGENT / QC_ONLY / BOLD_ONLY

Usage:
    # Test on CAMP partner first
    python3 bold_sequence_concordance.py --partner CAMP

    # All partners (excluding BGE)
    python3 bold_sequence_concordance.py --exclude-bge

    # Specific portal dump
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
from utils import is_bge_plate

# ── Paths ─────────────────────────────────────────────────────────────────────
QC_DIR = '/lustre/scratch126/tol/teams/lawniczak/projects/bioscan/bioscan_qc/qc_reports_rerun_Feb2026'

# ── Column names ──────────────────────────────────────────────────────────────
_SPECIMEN_COL    = 'sts_specimen.id'
_RACK_COL        = 'sts_rackid'
_BOLD_NUC_COL    = 'bold_nuc'
_BOLD_UPLOAD_COL = 'bold_sequence_upload_date'
_BOLD_BIN_COL    = 'bold_bin_uri'
_PARTNER_COL     = 'bold_bold_recordset_code_arr'
_QC_PID_COL      = 'pid'
_QC_SEQ_COL      = 'sequence'


# ── Sequence comparison ───────────────────────────────────────────────────────

def clean_seq(seq):
    """Uppercase, strip whitespace and gap characters."""
    if not seq or str(seq) in ('None', 'nan', ''):
        return None
    return re.sub(r'[\s\-]', '', str(seq).upper())


def reverse_complement(seq):
    """Return reverse complement of a DNA sequence."""
    complement = str.maketrans('ACGTN', 'TGCAN')
    return seq.translate(complement)[::-1]


def pct_identity(seq1, seq2):
    """
    Calculate percent identity between two sequences.
    Aligns by length — uses the shorter sequence length as denominator
    to handle cases where BOLD sequence is trimmed vs QC sequence.
    Returns float 0-100.
    """
    if not seq1 or not seq2:
        return 0.0
    # Compare over the length of the shorter sequence
    min_len = min(len(seq1), len(seq2))
    if min_len == 0:
        return 0.0
    matches = sum(a == b for a, b in zip(seq1[:min_len], seq2[:min_len]))
    return round(100 * matches / min_len, 2)


# ── Partner extraction ────────────────────────────────────────────────────────

def clean_partner(val):
    """Extract 4-letter BIOSCAN partner code from recordset field."""
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


# ── QC file loading ───────────────────────────────────────────────────────────

def find_bold_filtered_files(qc_dir):
    """
    Find all BOLDfiltered_metadata_*.csv files.
    Returns dict: batch_name -> filepath
    Excludes merged/RnD batches.
    """
    pattern = os.path.join(qc_dir, '**', 'BOLDfiltered_metadata_*.csv')
    files = glob.glob(pattern, recursive=True)
    result = {}
    skip = {'merged', 'rnd', 'r&d', 'repeat'}
    for f in sorted(files):
        batch = os.path.basename(os.path.dirname(f))
        # Skip merged/RnD batches
        if any(s in batch.lower() for s in skip):
            continue
        result[batch] = f
    return result


def load_qc_sequences(filepath, partner_filter=None):
    """
    Load pid -> sequence mapping from a BOLDfiltered file.
    Optionally filter to a specific partner.
    Returns dict: pid -> sequence string
    """
    try:
        df = pd.read_csv(filepath, dtype=str, usecols=[_QC_PID_COL, _QC_SEQ_COL])
    except Exception as e:
        return {}

    df = df.dropna(subset=[_QC_PID_COL, _QC_SEQ_COL])

    if partner_filter:
        # Filter by partner prefix in pid
        mask = df[_QC_PID_COL].str.startswith(partner_filter + '_') | \
               df[_QC_PID_COL].str.startswith('TOL-' + partner_filter + '-')
        df = df[mask]

    return dict(zip(df[_QC_PID_COL], df[_QC_SEQ_COL]))


# ── Portal dump loading ───────────────────────────────────────────────────────

def find_latest_dump(results_dir):
    candidates = sorted(glob.glob(os.path.join(results_dir, 'sts_manifests_*.tsv')))
    return candidates[-1] if candidates else None


def load_portal_sequences(dump_path, partner_filter=None, exclude_bge=False):
    """
    Load specimens with BOLD sequences from portal dump.
    Returns DataFrame with specimen_id, plate_id, partner,
    bold_nuc, bold_upload_date, bold_bin_uri.
    """
    print(f"Reading portal dump: {os.path.basename(dump_path)}")

    # Check available columns
    peek = pd.read_csv(dump_path, sep='\t', dtype=str, nrows=0)
    use_cols = [c for c in [_SPECIMEN_COL, _RACK_COL, _BOLD_NUC_COL,
                             _BOLD_UPLOAD_COL, _BOLD_BIN_COL, _PARTNER_COL]
                if c in peek.columns]

    df = pd.read_csv(dump_path, sep='\t', dtype=str, usecols=use_cols,
                     low_memory=False)
    print(f"  {len(df):,} rows loaded")

    # Clean None strings
    for col in df.columns:
        df[col] = df[col].replace({'None': None, 'nan': None, '': None})

    # Extract plate_id and partner
    df['plate_id'] = df[_RACK_COL] if _RACK_COL in df.columns else None
    df['partner'] = df[_PARTNER_COL].apply(clean_partner) \
        if _PARTNER_COL in df.columns else None
    missing_partner = df['partner'].isna()
    if missing_partner.any():
        df.loc[missing_partner, 'partner'] = \
            df.loc[missing_partner, 'plate_id'].apply(extract_partner_from_rack)

    # Exclude BGE
    if exclude_bge:
        n_before = len(df)
        df = df[~df['plate_id'].apply(lambda x: is_bge_plate(str(x)) if x else False)]
        print(f"  Excluded {n_before - len(df):,} BGE rows")

    # Filter to partner
    if partner_filter:
        df = df[df['partner'] == partner_filter]
        print(f"  Filtered to partner {partner_filter}: {len(df):,} rows")

    # Keep only specimens with BOLD sequence
    df = df[df[_BOLD_NUC_COL].notna()]
    print(f"  {len(df):,} specimens with BOLD sequence")

    # Use specimen ID column
    df = df.rename(columns={_SPECIMEN_COL: 'specimen_id',
                             _BOLD_NUC_COL: 'bold_nuc',
                             _BOLD_UPLOAD_COL: 'bold_upload_date',
                             _BOLD_BIN_COL: 'bold_bin_uri'})

    return df[['specimen_id', 'plate_id', 'partner',
               'bold_nuc', 'bold_upload_date', 'bold_bin_uri']].copy()


# ── Main comparison ───────────────────────────────────────────────────────────

def run_comparison(portal_df, qc_files, verbose=False):
    """
    For each specimen in portal_df, find it in QC batch files and compare sequences.
    Returns results DataFrame.
    """
    total = len(portal_df)
    print(f"\nComparing {total:,} BOLD specimens against {len(qc_files)} QC batch files...")
    print("Loading QC sequences from batch files...")

    # Build index: specimen_id -> {batch: sequence}
    # Load all QC files into memory (filtered to relevant specimens)
    specimen_ids = set(portal_df['specimen_id'].dropna())

    # For efficiency, load all batches and index by specimen
    qc_index = defaultdict(dict)  # specimen_id -> {batch: seq}

    for i, (batch, filepath) in enumerate(sorted(qc_files.items())):
        seqs = load_qc_sequences(filepath)
        found = 0
        for pid, seq in seqs.items():
            if pid in specimen_ids:
                qc_index[pid][batch] = seq
                found += 1
        if verbose and found > 0:
            print(f"  {batch}: {found} matching specimens")
        elif i % 20 == 0:
            print(f"  Processed {i+1}/{len(qc_files)} batch files...")

    print(f"\n  {len(qc_index):,} unique specimens found in QC files")
    print(f"  {total - len(qc_index):,} specimens on BOLD but not in any QC file")
    print(f"\nRunning sequence comparisons...")

    results = []
    for _, row in portal_df.iterrows():
        sid = row['specimen_id']
        bold_seq = clean_seq(row['bold_nuc'])
        bold_len = len(bold_seq) if bold_seq else 0

        batch_data = qc_index.get(sid, {})
        n_batches = len(batch_data)

        if n_batches == 0:
            # On BOLD but not in any QC file
            results.append({
                'specimen_id':       sid,
                'plate_id':          row['plate_id'],
                'partner':           row['partner'],
                'bold_upload_date':  row.get('bold_upload_date'),
                'bold_bin_uri':      row.get('bold_bin_uri'),
                'bold_seq_length':   bold_len,
                'n_batches_in_qc':   0,
                'batches_in_qc':     '',
                'any_identical':     False,
                'identical_batches': '',
                'best_pct_identity': None,
                'best_batch':        '',
                'all_pct_identities':'{}',
                'status':            'BOLD_ONLY',
            })
            continue

        # Compare against each batch — both forward and reverse complement
        identities    = {}
        rc_identities = {}
        for batch, qc_seq in batch_data.items():
            qc_seq_clean = clean_seq(qc_seq)
            if bold_seq and qc_seq_clean:
                fwd = pct_identity(bold_seq, qc_seq_clean)
                rc  = pct_identity(bold_seq, reverse_complement(qc_seq_clean))
                identities[batch]    = fwd
                rc_identities[batch] = rc
            else:
                identities[batch]    = 0.0
                rc_identities[batch] = 0.0

        # Best forward match
        identical_batches = [b for b, p in identities.items() if p == 100.0]
        best_batch = max(identities, key=identities.get) if identities else ''
        best_pct   = identities[best_batch] if best_batch else None

        # Best reverse complement match
        rc_identical_batches = [b for b, p in rc_identities.items() if p == 100.0]
        rc_best_batch = max(rc_identities, key=rc_identities.get) if rc_identities else ''
        rc_best_pct   = rc_identities[rc_best_batch] if rc_best_batch else None

        # Status — forward takes priority over RC
        if identical_batches:
            status = 'IDENTICAL'
        elif rc_identical_batches:
            status = 'IDENTICAL_RC'
        elif best_pct is not None and best_pct >= 95:
            status = 'CLOSE'
        elif rc_best_pct is not None and rc_best_pct >= 95:
            status = 'CLOSE_RC'
        elif best_pct is not None and best_pct > 0:
            status = 'DIVERGENT'
        else:
            status = 'NO_SEQUENCE'

        results.append({
            'specimen_id':          sid,
            'plate_id':             row['plate_id'],
            'partner':              row['partner'],
            'bold_upload_date':     row.get('bold_upload_date'),
            'bold_bin_uri':         row.get('bold_bin_uri'),
            'bold_seq_length':      bold_len,
            'n_batches_in_qc':      n_batches,
            'batches_in_qc':        ','.join(sorted(batch_data.keys())),
            'any_identical':        bool(identical_batches),
            'identical_batches':    ','.join(sorted(identical_batches)),
            'best_pct_identity':    best_pct,
            'best_batch':           best_batch,
            'all_pct_identities':   json.dumps(identities),
            'rc_any_identical':     bool(rc_identical_batches),
            'rc_identical_batches': ','.join(sorted(rc_identical_batches)),
            'rc_best_pct_identity': rc_best_pct,
            'rc_best_batch':        rc_best_batch,
            'status':               status,
        })

    return pd.DataFrame(results)


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary(df, partner=None):
    label = f" ({partner})" if partner else ""
    print(f"\n{'='*60}")
    print(f"BOLD SEQUENCE CONCORDANCE SUMMARY{label}")
    print(f"{'='*60}")
    print(f"Total specimens on BOLD:      {len(df):,}")
    print()

    vc = df['status'].value_counts()
    for status, n in vc.items():
        pct = 100*n/len(df)
        print(f"  {status:20s}: {n:6,} ({pct:.1f}%)")

    print()
    not_identical = df[df['status'].isin(['CLOSE','CLOSE_RC','DIVERGENT'])]
    if len(not_identical) > 0:
        print(f"Non-identical sequences ({len(not_identical):,}):")
        print(f"  Best % identity distribution:")
        pcts = not_identical['best_pct_identity'].dropna()
        print(f"    Mean:   {pcts.mean():.2f}%")
        print(f"    Median: {pcts.median():.2f}%")
        print(f"    Min:    {pcts.min():.2f}%")
        print()
        print(f"  By partner:")
        print(not_identical.groupby('partner').size().sort_values(ascending=False).to_string())
        print()
        print(f"  Sample divergent specimens:")
        show = not_identical.nsmallest(10, 'best_pct_identity')
        print(show[['specimen_id','partner','best_pct_identity',
                    'best_batch','bold_seq_length']].to_string(index=False))


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Compare BOLD sequences against QC BOLDfiltered sequences'
    )
    parser.add_argument('--partner', default=None,
        help='Run for a single partner only (e.g. CAMP) — recommended for testing')
    parser.add_argument('--exclude-bge', action='store_true',
        help='Exclude BGE partners (BGEP, BGEG, BGKU, BGPT)')
    parser.add_argument('--input', default=None,
        help='Path to portal dump TSV (default: most recent in RESULTS_DIR)')
    parser.add_argument('--output', default=None,
        help='Output CSV path')
    parser.add_argument('--verbose', action='store_true',
        help='Print per-batch loading progress')
    args = parser.parse_args()

    today = datetime.datetime.now().strftime('%Y%m%d')
    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    # Find portal dump
    dump_path = args.input
    if not dump_path:
        dump_path = find_latest_dump(config.RESULTS_DIR)
    if not dump_path or not os.path.exists(dump_path):
        print("ERROR: No portal dump found. Run read_portal_dump.py --fetch first.")
        return

    # Output path
    if args.output is None:
        suffix = f"_{args.partner}" if args.partner else "_ALL"
        args.output = os.path.join(
            config.RESULTS_DIR,
            f'bold_sequence_concordance{suffix}_{today}.csv'
        )

    # Load portal sequences
    portal_df = load_portal_sequences(
        dump_path,
        partner_filter=args.partner,
        exclude_bge=args.exclude_bge,
    )

    if portal_df.empty:
        print("No specimens found matching criteria.")
        return

    # Find QC batch files
    print(f"\nScanning QC directory: {QC_DIR}")
    qc_files = find_bold_filtered_files(QC_DIR)
    print(f"  Found {len(qc_files)} BOLDfiltered batch files")

    # Run comparison
    results = run_comparison(portal_df, qc_files, verbose=args.verbose)

    # Print summary
    print_summary(results, partner=args.partner)

    # Save
    results.to_csv(args.output, index=False)
    print(f"\nOutput: {args.output}")
    print(f"  {len(results):,} rows saved")


if __name__ == '__main__':
    main()
