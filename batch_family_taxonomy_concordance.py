"""
batch_family_taxonomy_concordance.py

Compares mBRAVE taxonomy calls across batch family members for the same
specimen ID. Used to check concordance when the same plates were sequenced
in multiple batches (RnD runs, splits, repeats).

This is a standalone script — NOT part of the monthly pipeline run.
Run occasionally when batch families need checking.

For each specimen appearing in multiple batch family members:
  - Compares taxonomy at each level (phylum, class, order, family, genus, species, BIN)
  - Reports the lowest level of conflict (e.g. "CONFLICT_AT_FAMILY")
  - Flags where one member has a call and another has None/null (not a conflict
    if the filled levels above agree)
  - Distinguishes between primary taxonomy conflicts and secondary

Taxonomy file used (in order of preference):
  1. renamed_samples_stop_codon_*_consensusseq_network.csv  (has correct specimen IDs)
  2. *consensusseq_network.tsv                              (fallback)

Batch families defined (edit BATCH_FAMILIES to add new ones):
  batch10, batch16, batch20, batch27, batch31, batch32,
  batch33, batch34, batch35, batch39

PCR1_volume_test batches are excluded.

Usage:
    conda activate bioscan-ops
    python3 batch_family_taxonomy_concordance.py
    python3 batch_family_taxonomy_concordance.py --family batch20
    python3 batch_family_taxonomy_concordance.py --family batch20 --verbose
"""

import argparse
import datetime
import glob
import os
import re
import pandas as pd
from collections import defaultdict

import config

TODAY    = datetime.datetime.now().strftime('%Y%m%d')
MBRAVE_DIR = '/lustre/scratch126/tol/teams/lawniczak/projects/bioscan/bioscan_qc/mbrave_batch_data'

# ── Batch family definitions ──────────────────────────────────────────────────

BATCH_FAMILIES = {
    'batch10': ['batch10', 'batchRnD10'],
    'batch16': ['batch16', 'batchRnD16', 'batchRnD16_2'],
    'batch20': ['batch20', 'batchRnDRevio20'],
    'batch27': ['batch27', 'batch27_1', 'batch27_2', 'batch27_3', 'batch27_4'],
    'batch31': ['batch31', 'batch31_0', 'batch31_1', 'batch31_2', 'batch31_3'],
    'batch32': ['batch32', 'batch32_0', 'batch32_1', 'batch32_2', 'batch32_3'],
    'batch33': ['batch33', 'batch33_0', 'batch33_1', 'batch33_2', 'batch33_3'],
    'batch34': ['batch34', 'batch34_0', 'batch34_1', 'batch34_2', 'batch34_3'],
    'batch35': ['batch35', 'batch35_repeat_batch0', 'batch35_repeat_batch1',
                'batch35_repeat_batch2', 'batch35_repeat_batch3'],
    'batch39': ['batch39_0', 'batch39_1', 'batch39_2', 'batch39_3',
                'batch39_rep_0', 'batch39_rep_1', 'batch39_rep_2', 'batch39_rep_3'],
}

# Taxonomy levels in order (lowest to highest resolution)
TAX_LEVELS = ['p_primary', 'c_primary', 'o_primary', 'f_primary',
              'g_primary', 's_primary', 'otu_primary']

TAX_LABELS = {
    'p_primary':   'phylum',
    'c_primary':   'class',
    'o_primary':   'order',
    'f_primary':   'family',
    'g_primary':   'genus',
    's_primary':   'species',
    'otu_primary': 'BIN',
}

# Splits — specimen should appear in only ONE of these per family
FAMILY_SPLITS = {
    'batch27': {'batch27_1', 'batch27_2', 'batch27_3', 'batch27_4'},
    'batch31': {'batch31_0', 'batch31_1', 'batch31_2', 'batch31_3'},
    'batch32': {'batch32_0', 'batch32_1', 'batch32_2', 'batch32_3'},
    'batch33': {'batch33_0', 'batch33_1', 'batch33_2', 'batch33_3'},
    'batch34': {'batch34_0', 'batch34_1', 'batch34_2', 'batch34_3'},
    'batch39': {'batch39_0', 'batch39_1', 'batch39_2', 'batch39_3'},
}


# ── File finding ──────────────────────────────────────────────────────────────

def find_consensusseq_file(batch_dir):
    """
    Find the best consensusseq file for a batch directory.
    Prefers renamed_samples_stop_codon_*_consensusseq_network.csv
    (has correct specimen IDs) over raw *consensusseq_network.tsv.
    Returns (filepath, separator) or (None, None).
    """
    # Prefer renamed CSV (correct specimen IDs)
    renamed = glob.glob(os.path.join(
        batch_dir, 'renamed_samples_stop_codon_*_consensusseq_network.csv'))
    if renamed:
        return renamed[0], ','

    # Fall back to TSV
    tsv = glob.glob(os.path.join(batch_dir, '*consensusseq_network.tsv'))
    if tsv:
        return tsv[0], '\t'

    return None, None


# ── Data loading ──────────────────────────────────────────────────────────────

def load_taxonomy(filepath, sep, batch_name, verbose=False):
    """
    Load taxonomy data from a consensusseq file.
    Returns dict: specimen_id -> {tax_level: value, ...}
    Multiple rows per specimen (multiple sequence calls) — use primary assignment.
    """
    try:
        df = pd.read_csv(filepath, sep=sep, dtype=str, low_memory=False)
    except Exception as e:
        print(f"    WARNING: could not read {filepath}: {e}")
        return {}

    # Normalise column names — strip quotes
    df.columns = [c.strip().strip('"') for c in df.columns]

    # Find pid column
    pid_col = None
    for col in ['pid', 'pid2', 'specimen_id']:
        if col in df.columns:
            pid_col = col
            break
    if not pid_col:
        print(f"    WARNING: no pid column found in {filepath}")
        return {}

    # Replace None/nan strings
    df = df.replace({'None': None, 'nan': None, '': None, 'EXCLUDED': None})

    # Check which tax levels are present
    available_levels = [l for l in TAX_LEVELS if l in df.columns]
    if not available_levels:
        print(f"    WARNING: no taxonomy columns found in {filepath}")
        return {}

    result = {}
    for _, row in df.iterrows():
        pid = str(row[pid_col]).strip().strip('"') if row[pid_col] else None
        if not pid or pid in ('nan', 'None'):
            continue

        # Skip if already seen (multiple rows per specimen — keep first/primary)
        if pid in result:
            continue

        tax = {}
        for level in TAX_LEVELS:
            if level in df.columns:
                val = row.get(level)
                tax[level] = str(val).strip() if val and str(val) not in ('nan','None','') else None
            else:
                tax[level] = None

        result[pid] = tax

    if verbose:
        print(f"    {batch_name}: {len(result):,} specimens loaded from {os.path.basename(filepath)}")

    return result


# ── Taxonomy comparison ───────────────────────────────────────────────────────

def compare_taxonomy(tax_dict_a, tax_dict_b):
    """
    Compare taxonomy between two specimens.
    Returns dict with conflict info per level.

    Rules:
    - If both have a value and they differ -> CONFLICT
      Exception: if species differ but BIN is the same -> BOLD_NAME_AMBIGUITY
      (multiple species names assigned to one BIN in BOLD database — not a
      real biological conflict between sequencing runs)
    - If one is None and other has a value -> PARTIAL
    - If both None -> BOTH_NULL
    - If both same -> IDENTICAL
    """
    # Check BIN first — used to reclassify species conflicts
    bin_a = tax_dict_a.get('otu_primary')
    bin_b = tax_dict_b.get('otu_primary')
    same_bin = (bin_a is not None and bin_b is not None and bin_a == bin_b)

    level_results = {}
    for level in TAX_LEVELS:
        val_a = tax_dict_a.get(level)
        val_b = tax_dict_b.get(level)

        if val_a is None and val_b is None:
            level_results[level] = 'BOTH_NULL'
        elif val_a is None:
            level_results[level] = 'PARTIAL_B_ONLY'
        elif val_b is None:
            level_results[level] = 'PARTIAL_A_ONLY'
        elif val_a == val_b:
            level_results[level] = 'IDENTICAL'
        else:
            # Values differ — check if this is a BOLD name ambiguity
            if same_bin and level in ('s_primary', 'g_primary'):
                # Same BIN, different species/genus name — BOLD database has
                # multiple names for this BIN, not a real biological conflict
                level_results[level] = 'BOLD_NAME_AMBIGUITY'
            else:
                level_results[level] = 'CONFLICT'

    return level_results


def get_conflict_level(level_results):
    """
    Return the highest-resolution level at which a genuine conflict exists.
    A PARTIAL is only flagged if all non-null levels are identical
    (i.e. one member has more resolution).
    Returns (conflict_level, conflict_type) or ('NONE', 'IDENTICAL/PARTIAL')
    """
    has_conflict = False
    has_partial  = False
    conflict_level = None

    for level in TAX_LEVELS:
        result = level_results.get(level)
        if result == 'CONFLICT':
            has_conflict = True
            conflict_level = level
            break  # Stop at first conflict — higher levels are implied
        elif result in ('PARTIAL_A_ONLY', 'PARTIAL_B_ONLY'):
            has_partial = True

    if has_conflict:
        return conflict_level, 'CONFLICT'
    elif has_partial:
        return None, 'PARTIAL'
    else:
        return None, 'IDENTICAL'


# ── Main analysis ─────────────────────────────────────────────────────────────

def analyse_family(family_name, members, mbrave_dir, verbose=False):
    """
    Analyse taxonomy concordance for one batch family.
    Returns (specimen_batch_rows, specimen_comparison_rows, summary_stats)
    """
    splits = FAMILY_SPLITS.get(family_name, set())

    # Load taxonomy for each member
    member_tax = {}
    for member in members:
        batch_dir = os.path.join(mbrave_dir, member)
        if not os.path.isdir(batch_dir):
            if verbose:
                print(f"    {member}: directory not found — skipping")
            continue
        filepath, sep = find_consensusseq_file(batch_dir)
        if not filepath:
            if verbose:
                print(f"    {member}: no consensusseq file found — skipping")
            continue
        tax_data = load_taxonomy(filepath, sep, member, verbose=verbose)
        if tax_data:
            member_tax[member] = tax_data

    if len(member_tax) < 2:
        print(f"  {family_name}: fewer than 2 members with data — skipping")
        return [], [], {}

    # All unique specimens
    all_specimens = set()
    for data in member_tax.values():
        all_specimens.update(data.keys())

    if verbose:
        print(f"  {family_name}: {len(all_specimens):,} unique specimens across "
              f"{len(member_tax)} members")

    # Per-specimen flat rows (one per member)
    specimen_batch_rows = []
    # Per-specimen pairwise comparison rows
    comparison_rows = []

    members_present_per_spec = defaultdict(list)

    for specimen_id in sorted(all_specimens):
        # Collect which members have this specimen
        present_in = {m: member_tax[m][specimen_id]
                      for m in member_tax if specimen_id in member_tax[m]}
        members_present_per_spec[specimen_id] = list(present_in.keys())

        # Check split anomaly
        splits_present = [m for m in present_in if m in splits]
        split_anomaly = len(splits_present) > 1

        # Flat rows per member
        for member, tax in present_in.items():
            row = {
                'specimen_id':  specimen_id,
                'batch_family': family_name,
                'batch_member': member,
                'split_anomaly': split_anomaly,
            }
            for level in TAX_LEVELS:
                row[TAX_LABELS[level]] = tax.get(level)
            specimen_batch_rows.append(row)

        # Pairwise comparisons
        member_list = sorted(present_in.keys())
        for i in range(len(member_list)):
            for j in range(i + 1, len(member_list)):
                m1, m2 = member_list[i], member_list[j]
                t1, t2 = present_in[m1], present_in[m2]

                level_results = compare_taxonomy(t1, t2)
                conflict_level, conflict_type = get_conflict_level(level_results)

                row = {
                    'specimen_id':      specimen_id,
                    'batch_family':     family_name,
                    'member_a':         m1,
                    'member_b':         m2,
                    'split_anomaly':    split_anomaly,
                    'conflict_type':    conflict_type,
                    'conflict_level':   TAX_LABELS.get(conflict_level, 'none')
                                        if conflict_level else 'none',
                }
                # Add per-level status and values
                for level in TAX_LEVELS:
                    label = TAX_LABELS[level]
                    row[f'{label}_a']      = t1.get(level)
                    row[f'{label}_b']      = t2.get(level)
                    row[f'{label}_status'] = level_results.get(level, 'MISSING')

                comparison_rows.append(row)

    # Summary stats
    comp_df = pd.DataFrame(comparison_rows) if comparison_rows else pd.DataFrame()
    stats = {}
    if not comp_df.empty:
        stats['n_specimens']     = len(all_specimens)
        stats['n_comparisons']   = len(comp_df)
        stats['n_identical']     = int((comp_df['conflict_type'] == 'IDENTICAL').sum())
        stats['n_partial']       = int((comp_df['conflict_type'] == 'PARTIAL').sum())
        stats['n_ambiguity']     = int((comp_df['conflict_type'] == 'BOLD_NAME_AMBIGUITY').sum())
        stats['n_conflict']      = int((comp_df['conflict_type'] == 'CONFLICT').sum())
        stats['n_split_anomaly'] = int(comp_df['split_anomaly'].sum())
        if stats['n_conflict'] > 0:
            conflict_levels = comp_df[comp_df['conflict_type']=='CONFLICT']['conflict_level']
            stats['conflict_level_breakdown'] = conflict_levels.value_counts().to_dict()

    return specimen_batch_rows, comparison_rows, stats


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary(all_comparison_rows, all_stats):
    print()
    print("=" * 70)
    print("TAXONOMY CONCORDANCE SUMMARY — ALL FAMILIES")
    print("=" * 70)

    total_specs   = sum(s.get('n_specimens', 0) for s in all_stats.values())
    total_comp    = sum(s.get('n_comparisons', 0) for s in all_stats.values())
    total_id      = sum(s.get('n_identical', 0) for s in all_stats.values())
    total_partial = sum(s.get('n_partial', 0) for s in all_stats.values())
    total_conf    = sum(s.get('n_conflict', 0) for s in all_stats.values())
    total_split   = sum(s.get('n_split_anomaly', 0) for s in all_stats.values())

    print(f"\nTotal unique specimens:    {total_specs:,}")
    print(f"Total pairwise comparisons: {total_comp:,}")
    print()
    print(f"  IDENTICAL  (all levels match):        {total_id:,} ({100*total_id/max(total_comp,1):.1f}%)")
    total_amb = sum(s.get('n_ambiguity', 0) for s in all_stats.values())
    print(f"  PARTIAL    (one member has more info): {total_partial:,} ({100*total_partial/max(total_comp,1):.1f}%)")
    print(f"  BOLD_NAME_AMBIGUITY (same BIN, diff name): {total_amb:,} ({100*total_amb/max(total_comp,1):.1f}%)")
    print(f"  CONFLICT   (genuine disagreement):     {total_conf:,} ({100*total_conf/max(total_comp,1):.1f}%)")
    if total_split:
        print(f"  SPLIT ANOMALY (specimen in >1 split):  {total_split:,} ⚠")
    print()

    # Conflict level breakdown across all families
    if all_comparison_rows:
        comp_df = pd.DataFrame(all_comparison_rows)
        conflicts = comp_df[comp_df['conflict_type'] == 'CONFLICT']
        if len(conflicts) > 0:
            print("Conflicts by taxonomy level:")
            for level, n in conflicts['conflict_level'].value_counts().items():
                print(f"  {level:10s}: {n:,}")
            print()
            print("Conflicts by batch family:")
            for fam, n in conflicts['batch_family'].value_counts().items():
                print(f"  {fam:20s}: {n:,}")
            print()

    # Per-family summary
    print("Per-family breakdown:")
    print(f"  {'Family':20s} {'Specimens':>10} {'Identical':>10} "
          f"{'Partial':>10} {'BIN_Ambig':>10} {'Conflict':>10}")
    print("  " + "-" * 72)
    for fam, stats in sorted(all_stats.items()):
        print(f"  {fam:20s} {stats.get('n_specimens',0):>10,} "
              f"{stats.get('n_identical',0):>10,} "
              f"{stats.get('n_partial',0):>10,} "
              f"{stats.get('n_ambiguity',0):>10,} "
              f"{stats.get('n_conflict',0):>10,}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Compare mBRAVE taxonomy across batch family members'
    )
    parser.add_argument('--family', default=None,
        help='Run for one family only e.g. batch20')
    parser.add_argument('--verbose', action='store_true',
        help='Print per-batch loading details')
    parser.add_argument('--output-dir', default=None,
        help='Output directory (default: RESULTS_DIR)')
    args = parser.parse_args()

    out_dir = args.output_dir or os.environ.get('BIOSCAN_RUN_DIR') or config.RESULTS_DIR
    os.makedirs(out_dir, exist_ok=True)

    print("=" * 70)
    print("BATCH FAMILY TAXONOMY CONCORDANCE")
    print(f"Run: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"mBRAVE dir: {MBRAVE_DIR}")
    print("=" * 70)
    print()

    # Select families
    families = BATCH_FAMILIES
    if args.family:
        if args.family not in BATCH_FAMILIES:
            print(f"ERROR: Unknown family '{args.family}'. "
                  f"Choose from: {list(BATCH_FAMILIES.keys())}")
            return
        families = {args.family: BATCH_FAMILIES[args.family]}

    all_batch_rows   = []
    all_comp_rows    = []
    all_stats        = {}

    for family_name, members in families.items():
        print(f"Analysing {family_name} ({len(members)} members)...")
        batch_rows, comp_rows, stats = analyse_family(
            family_name, members, MBRAVE_DIR, verbose=args.verbose)
        all_batch_rows.extend(batch_rows)
        all_comp_rows.extend(comp_rows)
        all_stats[family_name] = stats
        if stats:
            print(f"  {stats.get('n_specimens',0):,} specimens | "
                  f"{stats.get('n_identical',0):,} identical | "
                  f"{stats.get('n_partial',0):,} partial | "
                  f"{stats.get('n_conflict',0):,} conflicts")

    if not all_comp_rows:
        print("No comparison data generated.")
        return

    print_summary(all_comp_rows, all_stats)

    # Save outputs
    suffix = f"_{args.family}" if args.family else "_ALL"
    batch_csv = os.path.join(
        out_dir, f'batch_family_taxonomy_per_member_{TODAY}{suffix}.csv')
    comp_csv  = os.path.join(
        out_dir, f'batch_family_taxonomy_comparisons_{TODAY}{suffix}.csv')
    conf_csv  = os.path.join(
        out_dir, f'batch_family_taxonomy_conflicts_{TODAY}{suffix}.csv')

    pd.DataFrame(all_batch_rows).to_csv(batch_csv, index=False)

    comp_df = pd.DataFrame(all_comp_rows)
    comp_df.to_csv(comp_csv, index=False)

    # Conflicts only — most actionable
    conflicts = comp_df[comp_df['conflict_type'] == 'CONFLICT'].copy()
    conflicts.to_csv(conf_csv, index=False)

    print(f"\nOutputs written to: {out_dir}")
    print(f"  {os.path.basename(batch_csv)}  — taxonomy per specimen per member")
    print(f"  {os.path.basename(comp_csv)}   — all pairwise comparisons")
    print(f"  {os.path.basename(conf_csv)}   — conflicts only ({len(conflicts):,} rows)")


if __name__ == '__main__':
    main()
