"""
plate_summary_all.py

Generates a comprehensive plate-level summary for ALL plates across all
partners and batches. Uses the best QC result per specimen across all
repeat sequencings (PASS > ON_HOLD > FAILED).

Two output files:
  plate_summary_all_YYYYMMDD.csv          - PASS / ON_HOLD / FAIL counts
  plate_summary_categories_YYYYMMDD.csv   - categories 1-12 counts

One row per plate. Plates never sequenced appear with null sequencing columns.

Best result logic per specimen:
  PASS > ON_HOLD > FAILED across all batches the specimen appeared in.

Control naming conventions (from UMI control files):
  CONTROL_NEG_LYSATE_PLATEID_WELL  -> lysate negative (fixed well G12 or H12)
  CONTROL_NEG_SQPP-XXXXX_WELL      -> random negative with SQPP specimen ID
  CONTROL_NEG_PLATEID_WELL         -> random negative, early batch (no SQPP ID)
  CONTROL_POS_PLATEID_WELL         -> positive control (H12 for BGEP, G12 others)
  CONTROL_POS_SQPP-XXXXX_WELL      -> positive control via SQPP specimen

n_controls = 96 - n_specimens (sense check: should be 3 for full plates,
higher for partial plates where empty wells are assigned control barcodes).

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
                   batch_sort_key, matches_partner, safe_read_csv)


# ── Constants ─────────────────────────────────────────────────────────────────
_DECISION_RANK = {'PASS': 3, 'ON_HOLD': 2, 'FAILED': 1}
_ALL_CATS      = [str(i) for i in range(1, 13)]
_FIXED_WELLS   = {'G12', 'H12'}
_WELLS_PER_PLATE = 96

_SAMPLE_PATTERN = "umi.*_sample_stats.txt"
_NEG_PATTERN    = "umi.*_control_neg_stats.txt"
_POS_PATTERN    = "umi.*_control_pos_stats.txt"
_PID_RE         = re.compile(r'^[A-Z]')


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


def _normalise_plate_id(plate_id):
    """
    Strip TOL- prefix so TOL-BGEP-111 and BGEP-111 are treated as the same plate.
    Some batches use TOL-BGEP-XXX in UMI stats, others use BGEP-XXX for the same plate.
    """
    if not plate_id:
        return plate_id
    s = str(plate_id).strip()
    if s.upper().startswith('TOL-'):
        s = s[4:]  # strip TOL-
    return s


# ── Portal plate list ─────────────────────────────────────────────────────────

def load_portal_plates(portal_csv=None):
    if portal_csv is None:
        portal_csv = config.PORTAL_PLATES_CSV
    df = pd.read_csv(portal_csv, dtype=str)
    # Normalise plate IDs to strip TOL- prefix for consistent matching
    df['plate_id'] = df['plate_id'].apply(_normalise_plate_id)
    df['partner'] = df['plate_id'].apply(_extract_partner)
    # Deduplicate in case stripping creates duplicates (keep first)
    df = df.drop_duplicates(subset='plate_id', keep='first')
    return df.set_index('plate_id').to_dict('index')


# ── UMI data ──────────────────────────────────────────────────────────────────

def _parse_neg_control(label):
    """
    Parse a negative control label into (type, well, sqpp_id).

    Naming conventions:
      CONTROL_NEG_LYSATE_PLATEID_WELL  -> ('neg_lysate', well, None)
      CONTROL_NEG_SQPP-XXXXX_WELL      -> ('random_neg_sqpp', well, sqpp_id)
      CONTROL_NEG_PLATEID_WELL         -> ('random_neg_early', well, None)
                                          if well not in G12/H12, else 'neg_lysate'
    """
    if 'SQPP' in label:
        m = re.match(r'CONTROL_NEG_(SQPP-[\w-]+)_([A-H]\d{1,2})$', label)
        sqpp_id = m.group(1) if m else None
        well    = m.group(2) if m else None
        return 'random_neg_sqpp', well, sqpp_id

    if 'LYSATE' in label:
        m = re.match(r'CONTROL_NEG_LYSATE_.+_([A-H]\d{1,2})$', label)
        well = m.group(1) if m else None
        return 'neg_lysate', well, None

    # Early batch: CONTROL_NEG_PLATEID_WELL — no LYSATE or SQPP marker
    m = re.match(r'CONTROL_NEG_.+_([A-H]\d{1,2})$', label)
    well = m.group(1) if m else None
    if well and well not in _FIXED_WELLS:
        return 'random_neg_early', well, None
    return 'neg_lysate', well, None


def _parse_pos_control(label):
    """Parse a positive control label into (well, sqpp_id)."""
    if 'SQPP' in label:
        m = re.match(r'CONTROL_POS_(SQPP-[\w-]+)_([A-H]\d{1,2})$', label)
        return (m.group(2) if m else None), (m.group(1) if m else None)
    m = re.match(r'CONTROL_POS_.+_([A-H]\d{1,2})$', label)
    return (m.group(1) if m else None), None


def load_umi_data(mbrave_dir, resolved_batches, verbose=False):
    """
    Load sample and control stats from all mBRAVE batches.

    Returns:
      specimens   : plate_id -> batch -> [specimen labels]
      controls_neg: plate_id -> batch -> [{'type','well','sqpp_id','count','label'}]
      controls_pos: plate_id -> batch -> [{'well','sqpp_id','count','label'}]
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
            try:
                df = safe_read_csv(f, sep='\t', dtype=str)
                if 'Label' not in df.columns:
                    continue
                for _, row in df.iterrows():
                    label    = str(row.get('Label', '')).strip()
                    plate_id = _normalise_plate_id(str(row.get('Sample Plate ID', '')).strip())
                    if label and plate_id and plate_id not in ('nan', ''):
                        specimens[plate_id][batch_folder].append(label)
            except Exception:
                pass

        # Negative controls
        for f in glob.glob(os.path.join(batch_path, _NEG_PATTERN)):
            try:
                df = safe_read_csv(f, sep='\t', dtype=str)
                if 'Label' not in df.columns:
                    continue
                for _, row in df.iterrows():
                    label    = str(row.get('Label', '')).strip()
                    plate_id = _normalise_plate_id(str(row.get('Sample Plate ID', '')).strip())
                    count    = str(row.get('Count', '0')).strip()
                    if not label or not plate_id or plate_id in ('nan', ''):
                        continue
                    ctrl_type, well, sqpp_id = _parse_neg_control(label)
                    controls_neg[plate_id][batch_folder].append({
                        'label':    label,
                        'type':     ctrl_type,
                        'well':     well,
                        'sqpp_id':  sqpp_id,
                        'count':    count,
                    })
            except Exception:
                pass

        # Positive controls
        for f in glob.glob(os.path.join(batch_path, _POS_PATTERN)):
            try:
                df = safe_read_csv(f, sep='\t', dtype=str)
                if 'Label' not in df.columns:
                    continue
                for _, row in df.iterrows():
                    label    = str(row.get('Label', '')).strip()
                    plate_id = _normalise_plate_id(str(row.get('Sample Plate ID', '')).strip())
                    count    = str(row.get('Count', '0')).strip()
                    if not label or not plate_id or plate_id in ('nan', ''):
                        continue
                    well, sqpp_id = _parse_pos_control(label)
                    controls_pos[plate_id][batch_folder].append({
                        'label':   label,
                        'well':    well,
                        'sqpp_id': sqpp_id,
                        'count':   count,
                    })
            except Exception:
                pass

        if verbose:
            print(f"  {batch_folder}: processed")

    return dict(specimens), dict(controls_neg), dict(controls_pos)


# ── QC decisions ──────────────────────────────────────────────────────────────

def read_qc_portal(batch_folder, batch_path):
    """
    Read qc_portal for PASS/ON_HOLD/FAILED decisions (includes FAILed specimens).
    Returns DataFrame: pid | decision
    """
    files = glob.glob(os.path.join(batch_path, 'qc_portal_batch*.csv'))
    if not files:
        return pd.DataFrame()
    try:
        peek = safe_read_csv(files[0], nrows=1, header=None, dtype=str)
        first_val = str(peek.iloc[0, 0]).strip().strip('"')
        if _PID_RE.match(first_val) and first_val.lower() != 'pid':
            df = safe_read_csv(files[0], header=None, dtype=str)
            df = df.iloc[:, :2].copy()
            df.columns = ['pid', 'decision']
        else:
            df = safe_read_csv(files[0], dtype=str)
            df.columns = [c.strip().strip('"') for c in df.columns]
            df = df.rename(columns={'category_decision': 'decision'})
            df = df[['pid', 'decision']].copy()
        for col in df.columns:
            df[col] = df[col].str.strip().str.strip('"')
        df = df[df['pid'].notna() & (df['pid'] != 'pid')]
        return df
    except Exception:
        return pd.DataFrame()


def read_filtered_metadata(batch_folder, batch_path):
    """
    Read filtered_metadata for category numbers.
    Uses the 'category' column directly (integer 1-12).
    FAILed specimens are not present in filtered_metadata.

    Two pid formats exist:
      Older batches (batch30): pid = full specimen ID e.g. CAMP_131_A1
      Newer batches (batch54): pid = plate ID only e.g. CAMP_211
    We return both the raw pid AND a well-stripped version so the
    caller can match against qc_portal pids which always include the well.
    Returns DataFrame: pid_meta | category | well_from_meta
    """
    files = glob.glob(os.path.join(batch_path, 'filtered_metadata_batch*.csv'))
    if not files:
        return pd.DataFrame()
    try:
        df = safe_read_csv(files[0], dtype=str)
        if 'pid' not in df.columns:
            df.columns = [c.strip().strip('"') for c in df.columns]
        if 'category' not in df.columns:
            return pd.DataFrame()
        df['pid_meta'] = df['pid'].str.strip().str.strip('"')
        df['category']  = df['category'].str.strip()

        # Detect format: newer batches have pid = plate ID (no well)
        # Well coordinate is in a separate column 'Well.Coordinate'
        well_col = next((c for c in ['Well.Coordinate', 'Well Coordinate',
                                      'well_coordinate'] if c in df.columns), None)
        if well_col:
            # Check whether pid already contains the well coordinate
            # Old format: pid = 'CAMP_131_A1' (already has well)
            # New format: pid = 'CAMP_211'    (plate only, well is separate)
            # Detect by checking if pid ends with _[A-H][0-9]{1,2}
            sample_pid = df['pid_meta'].iloc[0] if len(df) > 0 else ''
            pid_has_well = bool(re.match(r'.+_[A-H]\d{1,2}$', sample_pid))
            if pid_has_well:
                # Old format: pid already includes well, use as-is
                df['full_pid'] = df['pid_meta']
            else:
                # New format: construct full pid from plate pid + well
                df['full_pid'] = df['pid_meta'] + '_' + df[well_col].str.strip()
        else:
            # No well column at all: use pid as-is
            df['full_pid'] = df['pid_meta']

        return df[['full_pid', 'category']].copy()
    except Exception:
        return pd.DataFrame()


def load_all_qc_decisions(qc_dir, qc_resolved, verbose=False):
    """
    Load QC decisions from qc_portal (all specimens including FAILED)
    and category numbers from filtered_metadata (PASS/ON_HOLD only but
    has reliable category numbers in all batch formats).
    Returns dict: specimen_id -> list of {batch, decision, category}
    """
    all_decisions = defaultdict(list)

    for batch_folder in qc_resolved:
        batch_path = os.path.join(qc_dir, batch_folder)

        # Decisions from qc_portal (includes FAILED)
        portal_df = read_qc_portal(batch_folder, batch_path)
        if portal_df.empty:
            continue

        # Categories from filtered_metadata (reliable category numbers)
        meta_df = read_filtered_metadata(batch_folder, batch_path)
        cat_lookup = {}
        if not meta_df.empty:
            cat_lookup = dict(zip(meta_df['full_pid'], meta_df['category']))

        for _, row in portal_df.iterrows():
            pid = str(row['pid']).strip()
            dec = str(row['decision']).strip()
            if pid:
                all_decisions[pid].append({
                    'batch':    batch_folder,
                    'decision': dec,
                    'category': cat_lookup.get(pid, ''),
                })
                # Also store under normalised pid (TOL- stripped) so
                # UMI specimen labels that kept TOL- prefix can still match
                norm_pid = re.sub(r'^TOL-', '', pid) if pid.upper().startswith('TOL-') else pid
                if norm_pid != pid:
                    all_decisions[norm_pid].append({
                        'batch':    batch_folder,
                        'decision': dec,
                        'category': cat_lookup.get(pid, ''),
                    })

        if verbose:
            print(f"  {batch_folder}: {len(portal_df)} QC records, "
                  f"{len(cat_lookup)} with category")

    return dict(all_decisions)


def best_result(decisions_list):
    """Return entry with best decision: PASS > ON_HOLD > FAILED."""
    if not decisions_list:
        return None
    return max(decisions_list,
               key=lambda d: _DECISION_RANK.get(d['decision'], 0))


# ── Build plate summary ───────────────────────────────────────────────────────

def build_plate_summary(portal_plates, specimens, controls_neg, controls_pos,
                        all_qc_decisions, partner_filter=None, verbose=False):

    all_plates = set(portal_plates.keys()) | set(specimens.keys())
    rows_summary    = []
    rows_categories = []

    for plate_id in sorted(all_plates):
        partner = _extract_partner(plate_id)

        if partner_filter and partner_filter.upper() != 'ALL':
            if partner != partner_filter.upper():
                continue

        portal_info  = portal_plates.get(plate_id, {})
        submit_date  = portal_info.get('submit_date')
        in_portal    = plate_id in portal_plates

        plate_batches = sorted(specimens.get(plate_id, {}).keys(),
                               key=batch_sort_key)
        n_batches = len(plate_batches)

        # ── Never sequenced ───────────────────────────────────────────────────
        if n_batches == 0:
            null_row = {
                'plate_id':           plate_id,
                'partner':            partner,
                'in_portal':          in_portal,
                'submit_date':        submit_date,
                'n_batches_sequenced':0,
                'batches':            None,
                'primary_batch':      None,
                'n_specimens':        None,
                'n_controls':         None,
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
                'pos_control_sqpp':   None,
                'neg_lysate_well':    None,
                'neg_lysate_reads':   None,
                'random_neg_sqpp_id': None,
                'random_neg_well':    None,
                'random_neg_reads':   None,
            }
            rows_summary.append(null_row)
            rows_categories.append({
                **{k: null_row[k] for k in
                   ['plate_id','partner','in_portal','submit_date',
                    'n_batches_sequenced','batches','primary_batch',
                    'n_specimens','n_controls',
                    'pos_control_well','pos_control_reads','pos_control_sqpp',
                    'neg_lysate_well','neg_lysate_reads',
                    'random_neg_sqpp_id','random_neg_well','random_neg_reads']},
                **{f'cat{c}_count': None for c in _ALL_CATS},
                'failed_count': None,
            })
            continue

        # ── Sequenced — best result per specimen ──────────────────────────────
        all_specimen_ids = set()
        for batch in plate_batches:
            all_specimen_ids.update(specimens[plate_id][batch])

        n_pass    = 0
        n_on_hold = 0
        n_failed  = 0
        cat_counts    = defaultdict(int)
        batches_used  = defaultdict(int)

        for specimen_id in all_specimen_ids:
            decisions = all_qc_decisions.get(specimen_id, [])
            best = best_result(decisions)
            if best is None:
                n_failed += 1
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

        n_specimens = len(all_specimen_ids)
        n_controls  = _WELLS_PER_PLATE - n_specimens
        n_combined  = n_pass + n_on_hold
        pct = lambda n: round(100 * n / n_specimens, 1) if n_specimens > 0 else 0

        primary_batch = (max(batches_used, key=batches_used.get)
                         if batches_used else plate_batches[-1])

        # Controls — use last batch only for reporting
        last_batch = plate_batches[-1]
        last_neg   = controls_neg.get(plate_id, {}).get(last_batch, [])
        last_pos   = controls_pos.get(plate_id, {}).get(last_batch, [])

        pos_ctrl   = next((c for c in last_pos), None)
        neg_lysate = next((c for c in last_neg
                           if c['type'] == 'neg_lysate'), None)
        random_neg = next((c for c in last_neg
                           if c['type'] in ('random_neg_sqpp',
                                            'random_neg_early')), None)

        ctrl_common = {
            'pos_control_well':   pos_ctrl['well']    if pos_ctrl   else None,
            'pos_control_reads':  pos_ctrl['count']   if pos_ctrl   else None,
            'pos_control_sqpp':   pos_ctrl['sqpp_id'] if pos_ctrl   else None,
            'neg_lysate_well':    neg_lysate['well']  if neg_lysate else None,
            'neg_lysate_reads':   neg_lysate['count'] if neg_lysate else None,
            'random_neg_sqpp_id': random_neg['sqpp_id'] if random_neg else None,
            'random_neg_well':    random_neg['well']    if random_neg else None,
            'random_neg_reads':   random_neg['count']   if random_neg else None,
        }

        base = {
            'plate_id':           plate_id,
            'partner':            partner,
            'in_portal':          in_portal,
            'submit_date':        submit_date,
            'n_batches_sequenced':n_batches,
            'batches':            ','.join(plate_batches),
            'primary_batch':      primary_batch,
            'n_specimens':        n_specimens,
            'n_controls':         n_controls,
        }

        rows_summary.append({
            **base,
            'pass_count':    n_pass,
            'on_hold_count': n_on_hold,
            'fail_count':    n_failed,
            'combined_count':n_combined,
            'pass_rate':     pct(n_pass),
            'on_hold_rate':  pct(n_on_hold),
            'fail_rate':     pct(n_failed),
            'combined_rate': pct(n_combined),
            **ctrl_common,
        })

        rows_categories.append({
            **base,
            **{f'cat{c}_count': cat_counts.get(c, 0) for c in _ALL_CATS},
            'failed_count': n_failed,
            **ctrl_common,
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

    print("Loading portal plate list...")
    portal_plates = load_portal_plates()
    print(f"  {len(portal_plates)} plates in portal")

    print("Resolving batches...")
    (mbrave_to_qc, qc_to_mbrave, issues,
     mbrave_resolved, qc_resolved,
     mbrave_skipped, qc_skipped) = build_batch_cross_map(
        config.MBRAVE_DIR, config.QC_DIR)
    print(f"  {len(mbrave_resolved)} mBRAVE | {len(qc_resolved)} QC batches")

    print("Loading UMI stats...")
    specimens, controls_neg, controls_pos = load_umi_data(
        config.MBRAVE_DIR, mbrave_resolved, verbose=args.verbose)
    print(f"  {len(specimens)} plates with specimen data")

    print("Loading QC decisions...")
    all_qc_decisions = load_all_qc_decisions(
        config.QC_DIR, qc_resolved, verbose=args.verbose)
    print(f"  {len(all_qc_decisions)} unique specimens with QC decisions")

    print("Building plate summary...")
    df_summary, df_categories = build_plate_summary(
        portal_plates, specimens, controls_neg, controls_pos,
        all_qc_decisions, partner_filter=args.partner,
        verbose=args.verbose)


    seq   = df_summary[df_summary['n_batches_sequenced'] > 0]
    notseq= df_summary[df_summary['n_batches_sequenced'] == 0]

    print(f"\nPlate summary:")
    print(f"  Total plates         : {len(df_summary)}")
    print(f"  Sequenced            : {len(seq)}")
    print(f"  Never sequenced      : {len(notseq)}")
    if len(seq) > 0:
        print(f"  Avg pass rate        : {seq['pass_rate'].mean():.1f}%")
        print(f"  Avg combined rate    : {seq['combined_rate'].mean():.1f}%")
        print(f"  Full plates (n=93)   : "
              f"{(seq['n_specimens']==93).sum()}")
        print(f"  Partial plates (<93) : "
              f"{(seq['n_specimens']<93).sum()}")
        print(f"  Plates 100% pass     : "
              f"{(seq['pass_rate']==100).sum()}")
        print(f"  Plates 0% pass       : "
              f"{(seq['pass_rate']==0).sum()}")

    partner_tag = args.partner if args.partner != 'ALL' else 'ALL'
    summary_path = os.path.join(config.RESULTS_DIR,
        f'plate_summary_all_{partner_tag}_{today}.csv')
    cats_path = os.path.join(config.RESULTS_DIR,
        f'plate_summary_categories_{partner_tag}_{today}.csv')

    df_summary.to_csv(summary_path, index=False)
    df_categories.to_csv(cats_path, index=False)

    print(f"\nOutputs:")
    print(f"  {summary_path}")
    print(f"  {cats_path}")


if __name__ == '__main__':
    main()
