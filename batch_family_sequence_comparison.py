"""
batch_family_sequence_comparison.py

Compares sequences across batch family members for the same specimen ID.
Identifies:
  1. Which batch member was the source for the BOLD upload
  2. Whether other batch members have QC-passed sequences not yet on BOLD
  3. Whether the same specimen has genuinely different sequences across
     batch family members (conflicts)
  4. Whether the same specimen appears in more than one SPLIT of the
     same family (should never happen — flagged as anomaly)

Batch families (from QC rerun Feb 2026):
  batch10:  batch10, batchRnD10
  batch16:  batch16, batchRnD16, batchRnD16_2
  batch20:  batch20, batchRnDRevio20
  batch27:  batch27, batch27_1/2/3/4
  batch31:  batch31, batch31_0/1/2/3
  batch32:  batch32, batch32_0/1/2/3
  batch33:  batch33, batch33_0/1/2/3
  batch34:  batch34, batch34_0/1/2/3
  batch35:  batch35, batch35_repeat_batch0/1/2/3
  batch39:  batch39_0/1/2/3, batch39_rep_0/1/2/3

QC decision mapping (from filtered_metadata_batch*.csv):
  YES     -> PASS
  ON_HOLD -> ON_HOLD
  absent  -> FAIL (specimen not in file)

Sequence identity classification:
  IDENTICAL           - 100% match, same length
  IDENTICAL_TRIM_1_4  - 100% core match, <=4bp trimmed at 5' or 3' end (amber)
  IDENTICAL_TRIM_5_23 - 100% core match, 5-23bp trimmed at either end (amber, likely primer)
  IDENTICAL_TRIM_24_PLUS - 100% core, >23bp trim (amber, investigate)
  NEAR_IDENTICAL      - >99% match with offset
  CLOSE               - 95-99% match
  DIFFERENT           - <95% match (red — genuine conflict)
  NOT_IN_BATCH        - specimen not found in this batch member

Usage:
    conda activate bioscan-ops
    python3 batch_family_sequence_comparison.py
    python3 batch_family_sequence_comparison.py --family batch20
    python3 batch_family_sequence_comparison.py --verbose
"""

import argparse
import datetime
import glob
import os
import re
import json
import pandas as pd
from collections import defaultdict

import config

TODAY   = datetime.datetime.now().strftime('%Y%m%d')

# ── Batch family definitions ──────────────────────────────────────────────────

BATCH_FAMILIES = {
    'batch10': {
        'members': ['batch10', 'batchRnD10'],
        'splits':  [],
        'rnds':    ['batchRnD10'],
    },
    'batch16': {
        'members': ['batch16', 'batchRnD16', 'batchRnD16_2'],
        'splits':  [],
        'rnds':    ['batchRnD16', 'batchRnD16_2'],
    },
    'batch20': {
        'members': ['batch20', 'batchRnDRevio20'],
        'splits':  [],
        'rnds':    ['batchRnDRevio20'],
    },
    'batch27': {
        'members': ['batch27', 'batch27_1', 'batch27_2', 'batch27_3', 'batch27_4'],
        'splits':  ['batch27_1', 'batch27_2', 'batch27_3', 'batch27_4'],
        'rnds':    [],
    },
    'batch31': {
        'members': ['batch31', 'batch31_0', 'batch31_1', 'batch31_2', 'batch31_3'],
        'splits':  ['batch31_0', 'batch31_1', 'batch31_2', 'batch31_3'],
        'rnds':    [],
    },
    'batch32': {
        'members': ['batch32', 'batch32_0', 'batch32_1', 'batch32_2', 'batch32_3'],
        'splits':  ['batch32_0', 'batch32_1', 'batch32_2', 'batch32_3'],
        'rnds':    [],
    },
    'batch33': {
        'members': ['batch33', 'batch33_0', 'batch33_1', 'batch33_2', 'batch33_3'],
        'splits':  ['batch33_0', 'batch33_1', 'batch33_2', 'batch33_3'],
        'rnds':    [],
    },
    'batch34': {
        'members': ['batch34', 'batch34_0', 'batch34_1', 'batch34_2', 'batch34_3'],
        'splits':  ['batch34_0', 'batch34_1', 'batch34_2', 'batch34_3'],
        'rnds':    [],
    },
    'batch35': {
        'members': ['batch35', 'batch35_repeat_batch0', 'batch35_repeat_batch1',
                    'batch35_repeat_batch2', 'batch35_repeat_batch3'],
        'splits':  [],
        'rnds':    ['batch35_repeat_batch0', 'batch35_repeat_batch1',
                    'batch35_repeat_batch2', 'batch35_repeat_batch3'],
    },
    'batch39': {
        'members': ['batch39_0', 'batch39_1', 'batch39_2', 'batch39_3',
                    'batch39_rep_0', 'batch39_rep_1', 'batch39_rep_2', 'batch39_rep_3'],
        'splits':  ['batch39_0', 'batch39_1', 'batch39_2', 'batch39_3'],
        'rnds':    ['batch39_rep_0', 'batch39_rep_1', 'batch39_rep_2', 'batch39_rep_3'],
    },
}


# ── Sequence utilities ────────────────────────────────────────────────────────

def clean_seq(seq):
    if not seq or str(seq) in ('None', 'nan', ''):
        return None
    return re.sub(r'\s', '', str(seq).upper())


def compare_sequences(s1, s2, max_trim=60):
    """
    Compare two sequences allowing for end trimming.
    Returns (status, trim_5, trim_3, core_identity, best_offset)

    trim_5 > 0 means s1 has extra bases at 5' end vs s2
    trim_3 > 0 means s1 has extra bases at 3' end vs s2
    """
    if not s1 or not s2:
        return 'NO_SEQUENCE', None, None, None, None

    # Try all offsets to find best alignment
    best_pct  = 0.0
    best_off  = 0
    for offset in range(-max_trim, max_trim + 1):
        if offset >= 0:
            a, b = s1[offset:], s2
        else:
            a, b = s1, s2[-offset:]
        n = min(len(a), len(b))
        if n == 0:
            continue
        pct = 100 * sum(x == y for x, y in zip(a[:n], b[:n])) / n
        if pct > best_pct:
            best_pct = pct
            best_off = offset

    # Calculate trim amounts at best offset
    if best_off >= 0:
        # s1 starts later — s2 has extra at 5' end (or s1 is 5'-trimmed)
        trim_5 = best_off        # bases trimmed from 5' of s1
        trim_3 = max(0, len(s2) - (len(s1) - best_off))
    else:
        trim_5 = 0
        trim_3 = -best_off

    core_identity = round(best_pct, 4)

    # Classify
    total_trim = trim_5 + trim_3
    if core_identity == 100.0:
        if total_trim == 0:
            status = 'IDENTICAL'
        elif total_trim <= 4:
            status = 'IDENTICAL_TRIM_1_4'
        elif total_trim <= 23:
            status = 'IDENTICAL_TRIM_5_23'
        else:
            status = 'IDENTICAL_TRIM_24_PLUS'
    elif core_identity >= 99.0:
        status = 'NEAR_IDENTICAL'
    elif core_identity >= 95.0:
        status = 'CLOSE'
    else:
        status = 'DIFFERENT'

    return status, trim_5, trim_3, core_identity, best_off


def is_effectively_identical(status):
    return status in ('IDENTICAL', 'IDENTICAL_TRIM_1_4',
                      'IDENTICAL_TRIM_5_23', 'IDENTICAL_TRIM_24_PLUS')


# ── QC file loading ───────────────────────────────────────────────────────────

def load_qc_data(batch_dir, batch_name):
    """
    Load filtered_metadata_batch*.csv for QC decisions and sequences.
    Returns dict: full_specimen_id -> {'decision': PASS/ON_HOLD/FAIL, 'sequence': str}
    """
    pattern = os.path.join(batch_dir, 'filtered_metadata_*.csv')
    files = glob.glob(pattern)
    # Also try without subdirectory
    if not files:
        pattern2 = os.path.join(
            config.QC_DIR, batch_name, 'filtered_metadata_*.csv')
        files = glob.glob(pattern2)

    if not files:
        return {}

    filepath = files[0]
    try:
        df = pd.read_csv(filepath, dtype=str)
    except Exception as e:
        print(f"    Warning: could not read {filepath}: {e}")
        return {}

    result = {}

    # Determine pid format — newer batches have well in Well.Coordinate
    has_well_col = 'Well.Coordinate' in df.columns
    pid_has_well = False
    if 'pid' in df.columns and len(df) > 0:
        sample_pid = str(df['pid'].iloc[0])
        # Old format: CAMP_140_A1 (has well suffix)
        # New format: CAMP_140 (no well suffix)
        pid_has_well = bool(re.search(r'_[A-H]\d{1,2}$', sample_pid))

    for _, row in df.iterrows():
        pid = str(row.get('pid', '')).strip()
        if not pid or pid == 'nan':
            continue

        # Build full specimen ID
        if pid_has_well:
            full_id = pid
        elif has_well_col:
            well = str(row.get('Well.Coordinate', '')).strip()
            full_id = f"{pid}_{well}" if well and well != 'nan' else pid
        else:
            full_id = pid

        # QC decision
        dec = str(row.get('category_decision', '')).strip()
        if dec == 'YES':
            decision = 'PASS'
        elif dec == 'ON_HOLD':
            decision = 'ON_HOLD'
        else:
            decision = 'PASS' if dec else 'FAIL'

        # Sequence
        seq = clean_seq(row.get('sequence', ''))

        result[full_id] = {
            'decision': decision,
            'sequence': seq,
            'category': str(row.get('category', '')).strip(),
        }

    return result


# ── Portal dump loading ───────────────────────────────────────────────────────

def load_bold_sequences(dump_path=None, verbose=False):
    """Load BOLD sequences from the portal dump."""
    if dump_path is None:
        dump_path = config.PORTAL_DUMP_TSV
    print(f"  Loading BOLD sequences from: {os.path.basename(dump_path)}")

    df = pd.read_csv(dump_path, sep='\t', dtype=str,
                     usecols=['sts_specimen.id', 'bold_nuc'],
                     low_memory=False)
    df = df[df['bold_nuc'].notna() & (df['bold_nuc'] != 'None')]
    result = dict(zip(df['sts_specimen.id'], df['bold_nuc']))
    if verbose:
        print(f"  {len(result):,} BOLD sequences loaded")
    return result


# ── Main analysis ─────────────────────────────────────────────────────────────

def analyse_family(family_name, family_def, bold_seqs, qc_dir, verbose=False):
    """
    Analyse one batch family. Returns (specimen_batch_rows, specimen_summary_rows).
    """
    members  = family_def['members']
    splits   = set(family_def['splits'])
    rnds     = set(family_def['rnds'])

    if verbose:
        print(f"  Family: {family_name} — {len(members)} members: {members}")

    # Load QC data for each member
    member_data = {}
    for member in members:
        batch_dir = os.path.join(qc_dir, member)
        if not os.path.isdir(batch_dir):
            if verbose:
                print(f"    {member}: directory not found — skipping")
            continue
        data = load_qc_data(batch_dir, member)
        member_data[member] = data
        if verbose:
            print(f"    {member}: {len(data):,} specimens loaded")

    if not member_data:
        return [], []

    # Build specimen universe — all unique specimen IDs across all members
    all_specimens = set()
    for data in member_data.values():
        all_specimens.update(data.keys())

    if verbose:
        print(f"    Total unique specimens: {len(all_specimens):,}")

    # ── Per specimen-batch rows ───────────────────────────────────────────────
    specimen_batch_rows = []
    # ── Per specimen summary ──────────────────────────────────────────────────
    specimen_summary_rows = []

    for specimen_id in sorted(all_specimens):
        bold_seq = clean_seq(bold_seqs.get(specimen_id))
        bold_len = len(bold_seq) if bold_seq else None

        # Collect data from each member
        member_results = {}
        for member, data in member_data.items():
            info = data.get(specimen_id)
            if info is None:
                member_results[member] = {
                    'in_batch':   False,
                    'decision':   'NOT_IN_BATCH',
                    'sequence':   None,
                    'category':   None,
                    'vs_bold':    None,
                    'trim_5':     None,
                    'trim_3':     None,
                    'core_id':    None,
                }
                continue

            seq = info['sequence']
            seq_len = len(seq) if seq else None

            # Compare to BOLD
            if bold_seq and seq:
                status, t5, t3, core_id, _ = compare_sequences(bold_seq, seq)
            else:
                status, t5, t3, core_id = 'NO_BOLD_SEQ' if not bold_seq else 'NO_QC_SEQ', None, None, None

            member_results[member] = {
                'in_batch':   True,
                'decision':   info['decision'],
                'sequence':   seq,
                'seq_len':    seq_len,
                'category':   info['category'],
                'vs_bold':    status,
                'trim_5':     t5,
                'trim_3':     t3,
                'core_id':    core_id,
            }

            specimen_batch_rows.append({
                'specimen_id':    specimen_id,
                'batch_family':   family_name,
                'batch_member':   member,
                'member_type':    'RND' if member in rnds else 'SPLIT' if member in splits else 'PRIMARY',
                'qc_decision':    info['decision'],
                'qc_category':    info['category'],
                'seq_length':     seq_len,
                'bold_seq_length': bold_len,
                'vs_bold_status': status,
                'trim_5prime':    t5,
                'trim_3prime':    t3,
                'core_identity':  core_id,
                'is_on_bold':     bold_seq is not None,
            })

        # ── Check for split anomalies ─────────────────────────────────────────
        splits_present = [m for m in splits if member_results.get(m, {}).get('in_batch')]
        split_anomaly = len(splits_present) > 1

        # ── Identify BOLD source member ───────────────────────────────────────
        bold_source = None
        bold_source_status = None
        if bold_seq:
            best_core = -1
            for member, res in member_results.items():
                if not res.get('in_batch'):
                    continue
                cid = res.get('core_id')
                if cid is not None and cid > best_core:
                    best_core = cid
                    bold_source = member
                    bold_source_status = res.get('vs_bold')

        # Update is_bold_source flag in specimen_batch_rows
        for row in specimen_batch_rows:
            if (row['specimen_id'] == specimen_id and
                    row['batch_family'] == family_name):
                row['is_bold_source'] = (row['batch_member'] == bold_source)

        # ── QC-passed sequences not on BOLD ───────────────────────────────────
        passed_members = [m for m, res in member_results.items()
                          if res.get('in_batch') and res.get('decision') == 'PASS']
        passed_not_bold = []
        for m in passed_members:
            vs = member_results[m].get('vs_bold')
            if vs and not is_effectively_identical(vs) and vs != 'NO_BOLD_SEQ':
                passed_not_bold.append(m)
            elif vs == 'NO_BOLD_SEQ':
                passed_not_bold.append(m)

        # ── Cross-member sequence conflicts ───────────────────────────────────
        # Compare sequences between members that both have sequences
        seq_conflicts = []
        members_with_seq = [(m, res['sequence']) for m, res in member_results.items()
                            if res.get('in_batch') and res.get('sequence')]

        for i in range(len(members_with_seq)):
            for j in range(i + 1, len(members_with_seq)):
                m1, s1 = members_with_seq[i]
                m2, s2 = members_with_seq[j]
                status, t5, t3, core_id, _ = compare_sequences(s1, s2)
                if not is_effectively_identical(status):
                    seq_conflicts.append({
                        'member_1': m1, 'member_2': m2,
                        'status': status, 'core_identity': core_id
                    })

        # Overall specimen status
        if split_anomaly:
            specimen_status = 'SPLIT_ANOMALY'
        elif seq_conflicts:
            worst = min(seq_conflicts, key=lambda x: x['core_identity'] or 0)
            if worst['status'] == 'DIFFERENT':
                specimen_status = 'CONFLICT_DIFFERENT'
            else:
                specimen_status = 'CONFLICT_CLOSE'
        elif passed_not_bold:
            specimen_status = 'ADDITIONAL_PASS_AVAILABLE'
        else:
            specimen_status = 'OK'

        specimen_summary_rows.append({
            'specimen_id':            specimen_id,
            'batch_family':           family_name,
            'n_members_present':      sum(1 for r in member_results.values() if r.get('in_batch')),
            'members_present':        ','.join(m for m, r in member_results.items() if r.get('in_batch')),
            'splits_present':         ','.join(splits_present),
            'split_anomaly':          split_anomaly,
            'is_on_bold':             bold_seq is not None,
            'bold_source_member':     bold_source,
            'bold_source_status':     bold_source_status,
            'passed_members':         ','.join(passed_members),
            'n_passed_members':       len(passed_members),
            'passed_not_on_bold':     ','.join(passed_not_bold),
            'n_passed_not_on_bold':   len(passed_not_bold),
            'n_seq_conflicts':        len(seq_conflicts),
            'conflict_details':       json.dumps(seq_conflicts) if seq_conflicts else '',
            'specimen_status':        specimen_status,
        })

    return specimen_batch_rows, specimen_summary_rows


# ── Summary printing ──────────────────────────────────────────────────────────

def print_family_summary(summary_df, batch_df):
    print()
    print("=" * 70)
    print("BATCH FAMILY SEQUENCE COMPARISON — SUMMARY")
    print("=" * 70)

    total = len(summary_df)
    print(f"\nTotal unique specimens across all families: {total:,}")
    print()

    print("Overall specimen status:")
    for status, n in summary_df['specimen_status'].value_counts().items():
        pct = 100 * n / total
        flag = "⚠" if 'CONFLICT' in status or 'ANOMALY' in status else \
               "→" if 'ADDITIONAL' in status else "✓"
        print(f"  {flag} {status:35s}: {n:6,} ({pct:.1f}%)")
    print()

    # By family
    print("By batch family:")
    print(summary_df.groupby(['batch_family', 'specimen_status'])
          .size().to_string())
    print()

    # Split anomalies
    anomalies = summary_df[summary_df['split_anomaly']]
    if len(anomalies) > 0:
        print(f"⚠ SPLIT ANOMALIES — same specimen in >1 split ({len(anomalies):,}):")
        print(anomalies[['specimen_id', 'batch_family', 'splits_present']].to_string(index=False))
        print()

    # Additional passes available
    extra = summary_df[summary_df['specimen_status'] == 'ADDITIONAL_PASS_AVAILABLE']
    if len(extra) > 0:
        print(f"→ Additional QC-passed sequences not yet on BOLD: {len(extra):,}")
        print("  By family:")
        print(extra.groupby('batch_family').size().to_string())
        print()

    # Conflicts
    conflicts = summary_df[summary_df['n_seq_conflicts'] > 0]
    if len(conflicts) > 0:
        print(f"⚠ Specimens with sequence conflicts across batch members: {len(conflicts):,}")
        print("  By family:")
        print(conflicts.groupby('batch_family').size().to_string())
        print()

    # vs BOLD status
    print("Vs BOLD status (specimen-batch level):")
    for status, n in batch_df['vs_bold_status'].value_counts().items():
        pct = 100 * n / len(batch_df)
        print(f"  {status:30s}: {n:6,} ({pct:.1f}%)")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Compare sequences across batch family members'
    )
    parser.add_argument('--family', default=None,
        help='Run for one family only e.g. batch20')
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--output-dir', default=None,
        help='Output directory (default: RESULTS_DIR)')
    args = parser.parse_args()

    out_dir = args.output_dir or os.environ.get('BIOSCAN_RUN_DIR') or config.RESULTS_DIR
    os.makedirs(out_dir, exist_ok=True)

    print("=" * 70)
    print("BATCH FAMILY SEQUENCE COMPARISON")
    print(f"Run: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)
    print()

    # Load BOLD sequences
    print("Loading BOLD sequences from portal dump...")
    bold_seqs = load_bold_sequences(verbose=True)
    print()

    # Select families to run
    families = BATCH_FAMILIES
    if args.family:
        if args.family not in BATCH_FAMILIES:
            print(f"ERROR: Unknown family '{args.family}'. "
                  f"Choose from: {list(BATCH_FAMILIES.keys())}")
            return
        families = {args.family: BATCH_FAMILIES[args.family]}

    # Run analysis
    all_batch_rows   = []
    all_summary_rows = []

    for family_name, family_def in families.items():
        print(f"Analysing {family_name}...")
        batch_rows, summary_rows = analyse_family(
            family_name, family_def, bold_seqs, config.QC_DIR,
            verbose=args.verbose
        )
        all_batch_rows.extend(batch_rows)
        all_summary_rows.extend(summary_rows)
        print(f"  {len(summary_rows):,} specimens, {len(batch_rows):,} specimen-batch records")

    if not all_batch_rows:
        print("No data found.")
        return

    batch_df   = pd.DataFrame(all_batch_rows)
    summary_df = pd.DataFrame(all_summary_rows)

    # Print summary
    print_family_summary(summary_df, batch_df)

    # Save outputs
    suffix = f"_{args.family}" if args.family else "_ALL"
    batch_csv   = os.path.join(out_dir, f'batch_family_specimen_batch_{TODAY}{suffix}.csv')
    summary_csv = os.path.join(out_dir, f'batch_family_summary_{TODAY}{suffix}.csv')

    batch_df.to_csv(batch_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)

    print(f"\nOutputs written to: {out_dir}")
    print(f"  {batch_csv.split('/')[-1]}   — {len(batch_df):,} specimen-batch rows")
    print(f"  {summary_csv.split('/')[-1]} — {len(summary_df):,} specimen summary rows")


if __name__ == '__main__':
    main()
