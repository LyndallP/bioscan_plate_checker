"""
plate_summary_all.py

Generates a comprehensive plate-level summary for ALL plates across all
partners and batches. Uses the best QC result per specimen across all
repeat sequencings (PASS > ON_HOLD > FAILED).

Two output files:
  plate_summary_all_YYYYMMDD.csv      - PASS / ON_HOLD / FAIL counts
  plate_summary_categories_YYYYMMDD.csv - categories 1-12 counts

One row per plate. Plates never sequenced appear with null sequencing columns.

Best result logic per specimen:
  Across all batches a specimen appeared in, take the best decision:
  PASS > ON_HOLD > FAILED
  So if a specimen passed in batch1 and failed in batch2, it counts as PASS.
  If it failed in batch1 and passed in batch2, it counts as PASS.

Expected wells per plate = sample_stats rows + control_neg rows + control_pos rows
for that plate. Controls are listed separately (not counted in specimen totals).

Sources:
  - Portal dump   : plate existence, partner, submission date
  - UMI stats     : expected wells, control info (pos/neg/random neg SQPP)
  - qc_portal     : QC decisions per specimen (PASS/ON_HOLD/FAILED + description)

Usage:
    python3 plate_summary_all.py
    python3 plate_summary_all.py --partner BGEP
    python3 plate_summary_all.py --verbose
"""

import argparse
import datetime
import glob
import os
import re
import pandas as pd
from collections import defaultdict

import config
from utils import (resolve_batches, build_batch_cross_map,
                   batch_sort_key, matches_partner, safe_read_csv,
                   extract_plate_from_pid)


# ── Decision ranking ──────────────────────────────────────────────────────────
_DECISION_RANK = {'PASS': 3, 'ON_HOLD': 2, 'FAILED': 1}

# QC category → decision mapping
_CAT_DECISION = {
    '1': 'PASS', '2': 'PASS', '3': 'PASS', '4': 'PASS',
    '5': 'PASS', '6': 'PASS', '7': 'PASS', '8': 'PASS',
    '9': 'ON_HOLD', '10': 'ON_HOLD', '11': 'ON_HOLD', '12': 'ON_HOLD',
}

# All 12 categories
_ALL_CATS = [str(i) for i in range(1, 13)]

# UMI file patterns
_SAMPLE_PATTERN   = "umi.*_sample_stats.txt"
_NEG_PATTERN      = "umi.*_control_neg_stats.txt"
_POS_PATTERN      = "umi.*_control_pos_stats.txt"

# qc_portal column detection
_PID_RE = re.compile(r'^[A-Z]')


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


def _normalise_plate(plate_id):
    """Strip TOL- prefix and normalise for matching."""
    if not plate_id:
        return plate_id
    return str(plate_id).strip()


# ── Step 1: Load portal plate list ────────────────────────────────────────────

def load_portal_plates(portal_csv=None):
    """Load plate list from portal dump cache."""
    if portal_csv is None:
        portal_csv = config.PORTAL_PLATES_CSV
    df = pd.read_csv(portal_csv, dtype=str)
    df['partner'] = df['plate_id'].apply(_extract_partner)
    return df.set_index('plate_id').to_dict('index')


# ── Step 2: Load UMI stats for all batches ────────────────────────────────────

def _read_umi_file(path, dtype=str):
    try:
        df = safe_read_csv(path, sep='\t', dtype=dtype)
        return df
    except Exception as e:
        return pd.DataFrame()


def load_umi_data(mbrave_dir, resolved_batches, verbose=False):
    """
    Load sample_stats, control_neg_stats, control_pos_stats for all batches.

    Returns:
      specimens   : dict plate_id -> batch -> list of specimen labels
      controls_neg: dict plate_id -> batch -> list of (label, type, well, sqpp_id, count)
      controls_pos: dict plate_id -> batch -> list of (label, well, sqpp_id, count)
    """
    specimens    = defaultdict(lambda: defaultdict(list))
    controls_neg = defaultdict(lambda: defaultdict(list))
    controls_pos = defaultdict(lambda: defaultdict(list))

    for batch_folder in resolved_batches:
        batch_path = os.path.join(mbrave_dir, batch_folder)

        # Sample stats
        for f in glob.glob(os.path.join(batch_path, _SAMPLE_PATTERN)):
            if '_control_' in f:
                continue
            df = _read_umi_file(f)
            if df.empty or 'Label' not in df.columns:
                continue
            for _, row in df.iterrows():
                label    = str(row.get('Label', '')).strip()
                plate_id = str(row.get('Sample Plate ID', '')).strip()
                if label and plate_id and plate_id != 'nan':
                    specimens[plate_id][batch_folder].append(label)

        # Negative controls
        for f in glob.glob(os.path.join(batch_path, _NEG_PATTERN)):
            df = _read_umi_file(f)
            if df.empty or 'Label' not in df.columns:
                continue
            for _, row in df.iterrows():
                label    = str(row.get('Label', '')).strip()
                plate_id = str(row.get('Sample Plate ID', '')).strip()
                count    = str(row.get('Count', '0')).strip()
                if not label or not plate_id or plate_id == 'nan':
                    continue

                # Parse control type and well
                if 'SQPP' in label:
                    # CONTROL_NEG_SQPP-XXXXX-X_WELLPOS
                    m = re.match(r'CONTROL_NEG_(SQPP-[\w-]+)_([A-H]\d{1,2})$', label)
                    sqpp_id = m.group(1) if m else None
                    well    = m.group(2) if m else None
                    ctrl_type = 'random_neg_sqpp'
                else:
                    # CONTROL_NEG_LYSATE_PLATEID_WELLPOS
                    m = re.match(r'CONTROL_NEG_LYSATE_(.+)_([A-H]\d{1,2})$', label)
                    sqpp_id = None
                    well    = m.group(2) if m else None
                    ctrl_type = 'neg_lysate'

                controls_neg[plate_id][batch_folder].append({
                    'label':     label,
                    'type':      ctrl_type,
                    'well':      well,
                    'sqpp_id':   sqpp_id,
                    'count':     count,
                })

        # Positive controls
        for f in glob.glob(os.path.join(batch_path, _POS_PATTERN)):
            df = _read_umi_file(f)
            if df.empty or 'Label' not in df.columns:
                continue
            for _, row in df.iterrows():
                label    = str(row.get('Label', '')).strip()
                plate_id = str(row.get('Sample Plate ID', '')).strip()
                count    = str(row.get('Count', '0')).strip()
                if not label or not plate_id or plate_id == 'nan':
                    continue

                # CONTROL_POS_PLATEID_WELLPOS or CONTROL_POS_SQPP-XXX_WELLPOS
                if 'SQPP' in label:
                    m = re.match(r'CONTROL_POS_(SQPP-[\w-]+)_([A-H]\d{1,2})$', label)
                    sqpp_id = m.group(1) if m else None
                    well    = m.group(2) if m else None
                else:
                    m = re.match(r'CONTROL_POS_(.+)_([A-H]\d{1,2})$', label)
                    sqpp_id = None
                    well    = m.group(2) if m else None

                controls_pos[plate_id][batch_folder].append({
                    'label':   label,
                    'well':    well,
                    'sqpp_id': sqpp_id,
                    'count':   count,
                })

        if verbose:
            n_plates = len(set(list(specimens.keys()) +
                               list(controls_neg.keys()) +
                               list(controls_pos.keys())))
            print(f"  {batch_folder}: processed")

    return dict(specimens), dict(controls_neg), dict(controls_pos)


# ── Step 3: Load QC portal decisions ─────────────────────────────────────────

def read_qc_portal(batch_folder, batch_path):
    """Read qc_portal file. Returns DataFrame: pid | decision | category | description"""
    files = glob.glob(os.path.join(batch_path, 'qc_portal_batch*.csv'))
    if not files:
        return pd.DataFrame()
    try:
        peek = safe_read_csv(files[0], nrows=1, header=None, dtype=str)
        first_val = str(peek.iloc[0, 0]).strip().strip('"')
        if _PID_RE.match(first_val) and first_val.lower() != 'pid':
            df = safe_read_csv(files[0], header=None, dtype=str)
            df = df.iloc[:, :3].copy()
            df.columns = ['pid', 'decision', 'description']
        else:
            df = safe_read_csv(files[0], dtype=str)
            df.columns = [c.strip().strip('"') for c in df.columns]
            df = df.rename(columns={'category_decision': 'decision',
                                    'category_explanation': 'description'})
            df = df[['pid', 'decision', 'description']].copy()
        for col in df.columns:
            df[col] = df[col].str.strip().str.strip('"')
        df = df[df['pid'].notna() & (df['pid'] != 'pid')]

        # Extract category number from description
        # e.g. "4, non-conflicting secondary sequences..."
        df['category'] = df['description'].str.extract(r'^(\d+),').fillna('')

        return df
    except Exception as e:
        return pd.DataFrame()


def load_all_qc_decisions(qc_dir, qc_resolved, verbose=False):
    """
    Load all QC decisions from all batches.
    Returns dict: specimen_id -> list of (batch, decision, category)
    sorted by batch order.
    """
    all_decisions = defaultdict(list)

    for batch_folder in qc_resolved:
        batch_path = os.path.join(qc_dir, batch_folder)
        df = read_qc_portal(batch_folder, batch_path)
        if df.empty:
            continue
        for _, row in df.iterrows():
            pid = str(row['pid']).strip()
            if pid:
                all_decisions[pid].append({
                    'batch':    batch_folder,
                    'decision': str(row['decision']).strip(),
                    'category': str(row['category']).strip(),
                })
        if verbose:
            print(f"  {batch_folder}: {len(df)} QC records")

    return dict(all_decisions)


# ── Step 4: Best result per specimen ─────────────────────────────────────────

def best_result(decisions_list):
    """
    Given list of {batch, decision, category} dicts for one specimen,
    return the entry with the best decision (PASS > ON_HOLD > FAILED).
    """
    if not decisions_list:
        return None
    return max(decisions_list,
               key=lambda d: _DECISION_RANK.get(d['decision'], 0))


# ── Step 5: Build plate summary ───────────────────────────────────────────────

def build_plate_summary(portal_plates, specimens, controls_neg, controls_pos,
                        all_qc_decisions, partner_filter=None, verbose=False):
    """
    Build one row per plate with best-result specimen counts and control info.
    """
    # All plate IDs from portal + UMI
    all_plates = set(portal_plates.keys()) | set(specimens.keys())

    rows_summary    = []
    rows_categories = []

    for plate_id in sorted(all_plates):
        partner = _extract_partner(plate_id)

        # Partner filter
        if partner_filter and partner_filter.upper() != 'ALL':
            if partner != partner_filter.upper():
                continue

        portal_info  = portal_plates.get(plate_id, {})
        submit_date  = portal_info.get('submit_date')
        portal_wells = portal_info.get('n_wells')
        in_portal    = plate_id in portal_plates

        # All batches this plate appeared in (from UMI)
        plate_batches = sorted(specimens.get(plate_id, {}).keys(),
                               key=batch_sort_key)
        n_batches = len(plate_batches)

        if n_batches == 0:
            # Never sequenced
            rows_summary.append({
                'plate_id':           plate_id,
                'partner':            partner,
                'in_portal':          in_portal,
                'submit_date':        submit_date,
                'portal_n_wells':     portal_wells,
                'n_batches_sequenced':0,
                'batches':            None,
                'expected_wells':     None,
                'n_specimens':        None,
                'pass_count':         None,
                'on_hold_count':      None,
                'fail_count':         None,
                'combined_count':     None,
                'pass_rate':          None,
                'on_hold_rate':       None,
                'fail_rate':          None,
                'combined_rate':      None,
                'pos_control_well':   None,
                'pos_control_reads':  None,
                'neg_lysate_well':    None,
                'neg_lysate_reads':   None,
                'random_neg_sqpp_id': None,
                'random_neg_well':    None,
                'random_neg_reads':   None,
            })
            rows_categories.append({
                'plate_id':           plate_id,
                'partner':            partner,
                'in_portal':          in_portal,
                'submit_date':        submit_date,
                'n_batches_sequenced':0,
                'batches':            None,
                'expected_wells':     None,
                'n_specimens':        None,
                **{f'cat{c}_count': None for c in _ALL_CATS},
                'failed_count':       None,
            })
            continue

        # All specimens across all batches for this plate
        all_specimen_ids = set()
        for batch in plate_batches:
            all_specimen_ids.update(specimens[plate_id][batch])

        # Best result per specimen
        n_pass    = 0
        n_on_hold = 0
        n_failed  = 0
        cat_counts = defaultdict(int)
        batches_used = defaultdict(int)  # batch -> how many specimens used from it

        for specimen_id in all_specimen_ids:
            decisions = all_qc_decisions.get(specimen_id, [])
            best = best_result(decisions)
            if best is None:
                n_failed += 1  # in UMI but no QC record = failed
                continue

            dec = best['decision']
            cat = best['category']
            batches_used[best['batch']] += 1

            if dec == 'PASS':
                n_pass += 1
            elif dec == 'ON_HOLD':
                n_on_hold += 1
            else:
                n_failed += 1

            if cat in _ALL_CATS:
                cat_counts[cat] += 1
            elif dec == 'FAILED':
                cat_counts['failed'] = cat_counts.get('failed', 0) + 1

        n_specimens = len(all_specimen_ids)
        n_combined  = n_pass + n_on_hold

        # Expected wells = specimens + all controls (any batch)
        # Use the most recent batch for control info
        last_batch = plate_batches[-1]

        # Count all unique control entries across all batches
        all_ctrl_neg = []
        all_ctrl_pos = []
        for b in plate_batches:
            all_ctrl_neg.extend(controls_neg.get(plate_id, {}).get(b, []))
            all_ctrl_pos.extend(controls_pos.get(plate_id, {}).get(b, []))

        n_ctrl_neg = len(set(c['well'] for c in all_ctrl_neg if c['well']))
        n_ctrl_pos = len(set(c['well'] for c in all_ctrl_pos if c['well']))

        # Use last batch controls for reporting
        last_neg = controls_neg.get(plate_id, {}).get(last_batch, [])
        last_pos = controls_pos.get(plate_id, {}).get(last_batch, [])

        # Parse controls for last batch
        pos_ctrl      = next((c for c in last_pos), None)
        neg_lysate    = next((c for c in last_neg if c['type'] == 'neg_lysate'), None)
        random_neg    = next((c for c in last_neg if c['type'] == 'random_neg_sqpp'), None)

        # Expected wells in last batch
        last_batch_specimens = len(specimens.get(plate_id, {}).get(last_batch, []))
        last_batch_neg = len(last_neg)
        last_batch_pos = len(last_pos)
        expected_wells = last_batch_specimens + last_batch_neg + last_batch_pos

        # Most common batch used
        primary_batch = (max(batches_used, key=batches_used.get)
                         if batches_used else last_batch)

        pct = lambda n: round(100 * n / n_specimens, 1) if n_specimens > 0 else 0

        rows_summary.append({
            'plate_id':           plate_id,
            'partner':            partner,
            'in_portal':          in_portal,
            'submit_date':        submit_date,
            'portal_n_wells':     portal_wells,
            'n_batches_sequenced':n_batches,
            'batches':            ','.join(plate_batches),
            'primary_batch':      primary_batch,
            'expected_wells':     expected_wells,
            'n_specimens':        n_specimens,
            'pass_count':         n_pass,
            'on_hold_count':      n_on_hold,
            'fail_count':         n_failed,
            'combined_count':     n_combined,
            'pass_rate':          pct(n_pass),
            'on_hold_rate':       pct(n_on_hold),
            'fail_rate':          pct(n_failed),
            'combined_rate':      pct(n_combined),
            'pos_control_well':   pos_ctrl['well'] if pos_ctrl else None,
            'pos_control_reads':  pos_ctrl['count'] if pos_ctrl else None,
            'pos_control_sqpp':   pos_ctrl['sqpp_id'] if pos_ctrl else None,
            'neg_lysate_well':    neg_lysate['well'] if neg_lysate else None,
            'neg_lysate_reads':   neg_lysate['count'] if neg_lysate else None,
            'random_neg_sqpp_id': random_neg['sqpp_id'] if random_neg else None,
            'random_neg_well':    random_neg['well'] if random_neg else None,
            'random_neg_reads':   random_neg['count'] if random_neg else None,
        })

        rows_categories.append({
            'plate_id':           plate_id,
            'partner':            partner,
            'in_portal':          in_portal,
            'submit_date':        submit_date,
            'n_batches_sequenced':n_batches,
            'batches':            ','.join(plate_batches),
            'primary_batch':      primary_batch,
            'expected_wells':     expected_wells,
            'n_specimens':        n_specimens,
            **{f'cat{c}_count': cat_counts.get(c, 0) for c in _ALL_CATS},
            'failed_count':       n_failed,
            'pos_control_well':   pos_ctrl['well'] if pos_ctrl else None,
            'pos_control_reads':  pos_ctrl['count'] if pos_ctrl else None,
            'pos_control_sqpp':   pos_ctrl['sqpp_id'] if pos_ctrl else None,
            'neg_lysate_well':    neg_lysate['well'] if neg_lysate else None,
            'neg_lysate_reads':   neg_lysate['count'] if neg_lysate else None,
            'random_neg_sqpp_id': random_neg['sqpp_id'] if random_neg else None,
            'random_neg_well':    random_neg['well'] if random_neg else None,
            'random_neg_reads':   random_neg['count'] if random_neg else None,
        })

    return pd.DataFrame(rows_summary), pd.DataFrame(rows_categories)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='All-plate summary with best QC result per specimen'
    )
    parser.add_argument('--partner', default='ALL')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    today = datetime.datetime.now().strftime('%Y%m%d')
    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    # ── Load portal plate list ────────────────────────────────────────────────
    print("Loading portal plate list...")
    portal_plates = load_portal_plates()
    print(f"  {len(portal_plates)} plates in portal")

    # ── Resolve batches ───────────────────────────────────────────────────────
    print("Resolving batches...")
    (mbrave_to_qc, qc_to_mbrave, issues,
     mbrave_resolved, qc_resolved,
     mbrave_skipped, qc_skipped) = build_batch_cross_map(
        config.MBRAVE_DIR, config.QC_DIR)
    print(f"  {len(mbrave_resolved)} mBRAVE batches | "
          f"{len(qc_resolved)} QC batches")

    # ── Load UMI data ─────────────────────────────────────────────────────────
    print("Loading UMI stats (samples + controls)...")
    specimens, controls_neg, controls_pos = load_umi_data(
        config.MBRAVE_DIR, mbrave_resolved, verbose=args.verbose)
    print(f"  {len(specimens)} plates with specimen data")
    print(f"  {len(controls_neg)} plates with negative control data")
    print(f"  {len(controls_pos)} plates with positive control data")

    # ── Load QC decisions ─────────────────────────────────────────────────────
    print("Loading QC portal decisions...")
    all_qc_decisions = load_all_qc_decisions(
        config.QC_DIR, qc_resolved, verbose=args.verbose)
    print(f"  {len(all_qc_decisions)} unique specimens with QC decisions")

    # ── Build summary ─────────────────────────────────────────────────────────
    print("Building plate summary...")
    df_summary, df_categories = build_plate_summary(
        portal_plates, specimens, controls_neg, controls_pos,
        all_qc_decisions,
        partner_filter=args.partner,
        verbose=args.verbose,
    )

    # ── Print quick summary ───────────────────────────────────────────────────
    print(f"\nPlate summary:")
    print(f"  Total plates         : {len(df_summary)}")
    seq = df_summary[df_summary['n_batches_sequenced'] > 0]
    not_seq = df_summary[df_summary['n_batches_sequenced'] == 0]
    print(f"  Sequenced plates     : {len(seq)}")
    print(f"  Never sequenced      : {len(not_seq)}")
    if len(seq) > 0:
        print(f"  Avg pass rate        : "
              f"{seq['pass_rate'].mean():.1f}%")
        print(f"  Avg combined rate    : "
              f"{seq['combined_rate'].mean():.1f}%")
        print(f"  Plates with 100% pass: "
              f"{(seq['pass_rate'] == 100).sum()}")
        print(f"  Plates with 0% pass  : "
              f"{(seq['pass_rate'] == 0).sum()}")

    # ── Save outputs ──────────────────────────────────────────────────────────
    partner_tag = args.partner if args.partner != 'ALL' else 'ALL'
    summary_path = os.path.join(
        config.RESULTS_DIR,
        f'plate_summary_all_{partner_tag}_{today}.csv')
    cats_path = os.path.join(
        config.RESULTS_DIR,
        f'plate_summary_categories_{partner_tag}_{today}.csv')

    df_summary.to_csv(summary_path, index=False)
    df_categories.to_csv(cats_path, index=False)

    print(f"\nOutputs written:")
    print(f"  {summary_path}")
    print(f"       (PASS / ON_HOLD / FAIL counts + control info per plate)")
    print(f"  {cats_path}")
    print(f"       (categories 1-12 counts + control info per plate)")


if __name__ == '__main__':
    main()
