"""
extract_repeat_batch_sequences.py

Extracts QC-passed sequences from BOLD_filtered_sequences FASTA files for
specimens that passed QC in a repeat batch family member but have no sequence
on BOLD yet.

These are identified by batch_family_sequence_comparison.py and represent
genuine opportunities to increase BOLD coverage.

BGE partners (BGEP, BGEG, BGPT, BGKU) and TOL- prefixed specimens are
excluded — their sequences are tracked separately.

For specimens with multiple passing batch members, the sequence is taken
from the member with the highest QC category (lowest category number = best).
If categories are equal, the most recent batch member is preferred.

Input:
    batch_family_summary_YYYYMMDD_ALL.csv  — from batch_family_sequence_comparison.py
    batch_family_specimen_batch_YYYYMMDD_ALL.csv  — from batch_family_sequence_comparison.py
    BOLD_filtered_sequences_batch*.fasta files in QC directory

Output:
    repeat_batch_additional_sequences_YYYYMMDD.fasta  — ready for BOLD upload
    repeat_batch_additional_sequences_YYYYMMDD.csv    — metadata per specimen
    repeat_batch_additional_sequences_YYYYMMDD_summary.txt

Usage:
    conda activate bioscan-ops
    python3 extract_repeat_batch_sequences.py
    python3 extract_repeat_batch_sequences.py --summary-csv /path/to/batch_family_summary.csv
    python3 extract_repeat_batch_sequences.py --dry-run
"""

import argparse
import datetime
import glob
import os
import re
import pandas as pd
from collections import defaultdict

import config

TODAY   = datetime.datetime.now().strftime('%Y%m%d')
QC_DIR  = '/lustre/scratch126/tol/teams/lawniczak/projects/bioscan/bioscan_qc/qc_reports_rerun_Feb2026'

BGE_PARTNERS = {'BGEP', 'BGEG', 'BGPT', 'BGKU'}


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_partner(specimen_id):
    if not specimen_id:
        return None
    s = str(specimen_id).upper()
    if s.startswith('TOL-'):
        return None  # TOL- prefix = BGE
    m = re.match(r'^([A-Z]{4})[_-]', s)
    return m.group(1) if m else None


def is_bge(specimen_id):
    if not specimen_id:
        return False
    s = str(specimen_id).upper()
    if s.startswith('TOL-'):
        return True
    partner = get_partner(specimen_id)
    return partner in BGE_PARTNERS


def find_latest_summary(results_dir):
    """Find most recent batch_family_summary file."""
    candidates = sorted(
        glob.glob(os.path.join(results_dir, 'batch_family_summary_*_ALL.csv')) +
        glob.glob(os.path.join(results_dir, '*', 'batch_family_summary_*_ALL.csv'))
    )
    return max(candidates, key=os.path.getmtime) if candidates else None


def find_latest_batch(results_dir):
    """Find most recent batch_family_specimen_batch file."""
    candidates = sorted(
        glob.glob(os.path.join(results_dir, 'batch_family_specimen_batch_*_ALL.csv')) +
        glob.glob(os.path.join(results_dir, '*', 'batch_family_specimen_batch_*_ALL.csv'))
    )
    return max(candidates, key=os.path.getmtime) if candidates else None


# ── FASTA loading ─────────────────────────────────────────────────────────────

def find_fasta_file(qc_dir, batch_name):
    """Find BOLD_filtered_sequences FASTA for a batch."""
    path = os.path.join(qc_dir, batch_name, f'BOLD_filtered_sequences_{batch_name}.fasta')
    if os.path.exists(path):
        return path
    # Try glob
    candidates = glob.glob(
        os.path.join(qc_dir, batch_name, 'BOLD_filtered_sequences_*.fasta'))
    return candidates[0] if candidates else None


def parse_fasta_targeted(filepath, target_ids):
    """
    Parse FASTA file and return only sequences for target_ids.
    Returns dict: specimen_id -> sequence
    """
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
                    if current_id and current_id in target_ids:
                        result[current_id] = ''.join(current_seq).upper()
                    current_id = line[1:].split()[0]
                    current_seq = []
                else:
                    current_seq.append(line)
            if current_id and current_id in target_ids:
                result[current_id] = ''.join(current_seq).upper()
    except Exception as e:
        print(f"  Warning: could not read {filepath}: {e}")

    return result


def load_qc_categories(qc_dir, batch_name, target_ids):
    """
    Load QC category from filtered_metadata for target specimens.
    Returns dict: specimen_id -> category (int, lower = better)
    """
    pattern = os.path.join(qc_dir, batch_name, 'filtered_metadata_*.csv')
    files = glob.glob(pattern)
    if not files:
        return {}

    try:
        df = pd.read_csv(files[0], dtype=str)
    except Exception:
        return {}

    df.columns = [c.strip().strip('"') for c in df.columns]

    # Build full specimen ID
    pid_col = 'pid' if 'pid' in df.columns else None
    if not pid_col:
        return {}

    has_well = 'Well.Coordinate' in df.columns
    sample_pid = str(df[pid_col].iloc[0]) if len(df) > 0 else ''
    pid_has_well = bool(re.search(r'_[A-H]\d{1,2}$', sample_pid))

    result = {}
    for _, row in df.iterrows():
        pid = str(row[pid_col]).strip()
        if not pid or pid == 'nan':
            continue
        if pid_has_well:
            full_id = pid
        elif has_well:
            well = str(row.get('Well.Coordinate', '')).strip()
            full_id = f"{pid}_{well}" if well and well != 'nan' else pid
        else:
            full_id = pid

        if full_id in target_ids:
            cat = row.get('category', '99')
            try:
                result[full_id] = int(float(str(cat).strip()))
            except Exception:
                result[full_id] = 99

    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Extract QC-passed sequences from repeat batch members for BOLD upload'
    )
    parser.add_argument('--summary-csv', default=None,
        help='Path to batch_family_summary_ALL.csv')
    parser.add_argument('--batch-csv', default=None,
        help='Path to batch_family_specimen_batch_ALL.csv')
    parser.add_argument('--output-dir', default=None,
        help='Output directory (default: RESULTS_DIR)')
    parser.add_argument('--dry-run', action='store_true',
        help='Report what would be extracted without writing FASTA')
    args = parser.parse_args()

    out_dir = args.output_dir or os.environ.get('BIOSCAN_RUN_DIR') or config.RESULTS_DIR
    os.makedirs(out_dir, exist_ok=True)

    results_dir = config.RESULTS_DIR

    # Find input files
    summary_path = args.summary_csv or find_latest_summary(results_dir)
    batch_path   = args.batch_csv   or find_latest_batch(results_dir)

    if not summary_path or not os.path.exists(summary_path):
        print("ERROR: Could not find batch_family_summary_ALL.csv")
        print("Run batch_family_sequence_comparison.py first.")
        return
    if not batch_path or not os.path.exists(batch_path):
        print("ERROR: Could not find batch_family_specimen_batch_ALL.csv")
        return

    print("=" * 60)
    print("EXTRACT REPEAT BATCH SEQUENCES FOR BOLD UPLOAD")
    print(f"Run: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)
    print()
    print(f"Summary: {os.path.basename(summary_path)}")
    print(f"Batch:   {os.path.basename(batch_path)}")
    print()

    # Load files
    summary = pd.read_csv(summary_path, dtype=str)
    batch   = pd.read_csv(batch_path, dtype=str)

    # Filter to ADDITIONAL_PASS_AVAILABLE, not on BOLD, not BGE
    candidates = summary[
        (summary['specimen_status'] == 'ADDITIONAL_PASS_AVAILABLE') &
        (summary['is_on_bold'] == 'False') &
        (~summary['specimen_id'].apply(is_bge))
    ].copy()

    candidates['partner'] = candidates['specimen_id'].apply(get_partner)

    print(f"Target specimens (non-BGE, not on BOLD, QC passed in repeat): {len(candidates):,}")
    print()
    print("By partner:")
    print(candidates['partner'].value_counts().to_string())
    print()
    print("By batch family:")
    print(candidates['batch_family'].value_counts().to_string())
    print()

    if args.dry_run:
        print("DRY RUN — not extracting sequences.")
        return

    # Build lookup: specimen_id -> list of passing batch members
    # When multiple members pass, prefer lowest QC category (best quality)
    # then most recently sequenced (approximated by member name sort order)

    # Get QC decisions from batch file
    batch_pass = batch[
        (batch['specimen_id'].isin(set(candidates['specimen_id']))) &
        (batch['qc_decision'] == 'PASS')
    ][['specimen_id', 'batch_family', 'batch_member']].copy()

    # For each specimen, determine best batch member to use
    print("Determining best batch member per specimen...")
    specimen_to_member = {}  # specimen_id -> best batch_member

    for spec_id in candidates['specimen_id']:
        row = candidates[candidates['specimen_id'] == spec_id].iloc[0]
        passing = str(row.get('passed_not_on_bold', '')).split(',')
        passing = [p.strip() for p in passing if p.strip()]

        if not passing:
            continue

        if len(passing) == 1:
            specimen_to_member[spec_id] = passing[0]
            continue

        # Multiple passing members — load QC categories to pick best
        best_member = None
        best_cat    = 99
        for member in passing:
            cats = load_qc_categories(QC_DIR, member, {spec_id})
            cat = cats.get(spec_id, 99)
            if cat < best_cat:
                best_cat    = cat
                best_member = member
        specimen_to_member[spec_id] = best_member or passing[0]

    # Group by batch member for efficient FASTA loading
    member_to_specimens = defaultdict(set)
    for spec_id, member in specimen_to_member.items():
        if member:
            member_to_specimens[member].add(spec_id)

    print(f"Specimens to extract: {len(specimen_to_member):,}")
    print(f"From {len(member_to_specimens)} batch members:")
    for member, specs in sorted(member_to_specimens.items()):
        print(f"  {member}: {len(specs):,}")
    print()

    # Extract sequences from FASTA files
    print("Extracting sequences from FASTA files...")
    extracted = {}   # specimen_id -> sequence
    not_found = []

    for member, spec_ids in sorted(member_to_specimens.items()):
        fasta_path = find_fasta_file(QC_DIR, member)
        if not fasta_path:
            print(f"  WARNING: No FASTA found for {member} — {len(spec_ids)} specimens skipped")
            not_found.extend(spec_ids)
            continue

        seqs = parse_fasta_targeted(fasta_path, spec_ids)
        found    = len(seqs)
        missing  = len(spec_ids) - found
        extracted.update(seqs)
        print(f"  {member}: {found:,} extracted, {missing} not found in FASTA")
        if missing > 0:
            missing_ids = spec_ids - set(seqs.keys())
            not_found.extend(missing_ids)

    print()
    print(f"Total extracted: {len(extracted):,}")
    print(f"Not found in FASTA: {len(not_found):,}")
    print()

    # Write FASTA output
    fasta_out = os.path.join(out_dir, f'repeat_batch_additional_sequences_{TODAY}.fasta')
    csv_out   = os.path.join(out_dir, f'repeat_batch_additional_sequences_{TODAY}.csv')
    txt_out   = os.path.join(out_dir, f'repeat_batch_additional_sequences_{TODAY}_summary.txt')

    # FASTA
    with open(fasta_out, 'w') as f:
        for spec_id, seq in sorted(extracted.items()):
            member = specimen_to_member.get(spec_id, 'unknown')
            partner = get_partner(spec_id) or 'unknown'
            f.write(f'>{spec_id}|{member}|{partner}\n')
            f.write(f'{seq}\n')

    # CSV metadata
    meta_rows = []
    for spec_id, seq in sorted(extracted.items()):
        member  = specimen_to_member.get(spec_id, '')
        partner = get_partner(spec_id) or ''
        fam_row = candidates[candidates['specimen_id'] == spec_id]
        family  = fam_row['batch_family'].values[0] if len(fam_row) > 0 else ''
        meta_rows.append({
            'specimen_id':    spec_id,
            'partner':        partner,
            'batch_family':   family,
            'source_member':  member,
            'seq_length':     len(seq),
        })
    pd.DataFrame(meta_rows).to_csv(csv_out, index=False)

    # Summary text
    partner_counts = candidates[candidates['specimen_id'].isin(extracted)]['partner'].value_counts()
    family_counts  = candidates[candidates['specimen_id'].isin(extracted)]['batch_family'].value_counts()

    with open(txt_out, 'w') as f:
        f.write("REPEAT BATCH ADDITIONAL SEQUENCES — EXTRACTION SUMMARY\n")
        f.write(f"Run: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
        f.write(f"Total sequences extracted: {len(extracted):,}\n")
        f.write(f"Not found in FASTA:        {len(not_found):,}\n\n")
        f.write("By partner:\n")
        for p, n in partner_counts.items():
            f.write(f"  {p:8s}: {n:,}\n")
        f.write("\nBy batch family:\n")
        for fam, n in family_counts.items():
            f.write(f"  {fam:25s}: {n:,}\n")

    print(f"FASTA output:   {fasta_out}")
    print(f"CSV metadata:   {csv_out}")
    print(f"Summary:        {txt_out}")
    print()
    print("Ready for BOLD upload.")


if __name__ == '__main__':
    main()
