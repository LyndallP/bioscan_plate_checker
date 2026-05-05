"""
Shared utilities for bioscan_plate_checker.

Batch resolution rules (applied independently per directory):
  - ALWAYS prefer splits (batchN_0, batchN_1...) over plain (batchN)
  - ALWAYS prefer splits over merged (batchN_merged)
  - If only plain exists -> use plain
  - If only merged exists (no splits, no plain) -> use merged as fallback

Cross-directory mapping:
  - mBRAVE may have splits (batch51_0..3) where QC has only plain (batch51)
    because QC was auto-merged. We map splits -> plain for QC lookup.

Special folders (not standard production batches):
  - batchRnD*, PCR1_volume_test_batch*   (R&D / test)
  - batch35_repeat_batch*, batch39_rep_* (repeat sub-batches)
"""

import os
import re
import glob
import pandas as pd
from collections import defaultdict


# ── Folder classification ─────────────────────────────────────────────────────

_SPLIT_RE   = re.compile(r'^batch(\d+)_(\d+)$')
_MERGED_RE  = re.compile(r'^batch(\d+)_merged$')
_PLAIN_RE   = re.compile(r'^batch(\d+)$')
_SPECIAL_RE = re.compile(r'^(batchRnD\w+|PCR1_volume_test_batch\d+|batch\d+_repeat_batch\d+|batch\d+_rep_\d+)$')


def classify_folder(name):
    """
    Classify a batch folder name into a dict:
        type:      'plain' | 'split' | 'merged' | 'special' | 'unknown'
        base_num:  int or None
        split_num: int or None
        name:      original folder name
    """
    m = _SPLIT_RE.match(name)
    if m:
        return {'type': 'split',   'base_num': int(m.group(1)),
                'split_num': int(m.group(2)), 'name': name}
    m = _MERGED_RE.match(name)
    if m:
        return {'type': 'merged',  'base_num': int(m.group(1)),
                'split_num': None,  'name': name}
    m = _PLAIN_RE.match(name)
    if m:
        return {'type': 'plain',   'base_num': int(m.group(1)),
                'split_num': None,  'name': name}
    if _SPECIAL_RE.match(name):
        return {'type': 'special', 'base_num': None,
                'split_num': None,  'name': name}
    return {'type': 'unknown', 'base_num': None, 'split_num': None, 'name': name}


def resolve_batches(data_dir, include_special=False):
    """
    Return (resolved_list, skipped_list) for data_dir.

    Priority per base_num group:
      1. Splits  (always preferred)
      2. Plain   (fallback if no splits)
      3. Merged  (last resort, only if nothing else)
    """
    entries = [classify_folder(f)
               for f in os.listdir(data_dir)
               if os.path.isdir(os.path.join(data_dir, f))]

    by_base  = defaultdict(list)
    specials = []
    unknowns = []

    for e in entries:
        if e['type'] == 'special':
            specials.append(e)
        elif e['type'] == 'unknown':
            unknowns.append(e)
        else:
            by_base[e['base_num']].append(e)

    if unknowns:
        print(f"  NOTE: unrecognised folders in {data_dir}: "
              f"{[u['name'] for u in unknowns]}")

    result  = []
    skipped = []

    for base_num in sorted(by_base.keys()):
        group = by_base[base_num]
        types = {e['type'] for e in group}

        if 'split' in types:
            use  = sorted([e for e in group if e['type'] == 'split'],
                          key=lambda e: e['split_num'])
            skip = [e for e in group if e['type'] in ('plain', 'merged')]
        elif 'plain' in types:
            use  = [e for e in group if e['type'] == 'plain']
            skip = [e for e in group if e['type'] == 'merged']
        else:
            # Only merged — use as fallback
            use  = [e for e in group if e['type'] == 'merged']
            skip = []

        result.extend([e['name'] for e in use])
        skipped.extend([e['name'] for e in skip])

    if include_special:
        result.extend(sorted([e['name'] for e in specials]))

    return result, skipped


def build_batch_cross_map(mbrave_dir, qc_dir):
    """
    Build cross-directory mapping: each resolved mBRAVE folder -> QC folder.

    Key case: mBRAVE splits (batch51_0..3) -> QC plain (batch51) [auto-merged QC]

    Returns:
        mbrave_to_qc:    dict  mbrave_folder -> qc_folder (or None)
        qc_to_mbrave:    dict  qc_folder -> [mbrave_folder, ...]
        issues:          list of warning strings
        mbrave_resolved: list
        qc_resolved:     list
        mbrave_skipped:  list
        qc_skipped:      list
    """
    mbrave_resolved, mbrave_skipped = resolve_batches(mbrave_dir)
    qc_resolved,     qc_skipped     = resolve_batches(qc_dir)

    # Index QC by (base_num, split_num)
    qc_index = {}
    qc_plain  = {}  # base_num -> folder (plain only, for auto-merge fallback)
    for folder in qc_resolved:
        c = classify_folder(folder)
        if c['base_num'] is not None:
            qc_index[(c['base_num'], c['split_num'])] = folder
            if c['type'] == 'plain':
                qc_plain[c['base_num']] = folder

    mbrave_to_qc = {}
    qc_to_mbrave = defaultdict(list)
    issues       = []

    for mb_folder in mbrave_resolved:
        c = classify_folder(mb_folder)
        if c['base_num'] is None:
            mbrave_to_qc[mb_folder] = None
            continue

        exact_key = (c['base_num'], c['split_num'])

        if exact_key in qc_index:
            # Exact match (split->split or plain->plain)
            qc_folder = qc_index[exact_key]
            mbrave_to_qc[mb_folder] = qc_folder
            qc_to_mbrave[qc_folder].append(mb_folder)

        elif c['type'] == 'split' and c['base_num'] in qc_plain:
            # Auto-merged QC: mBRAVE split -> QC plain
            qc_folder = qc_plain[c['base_num']]
            mbrave_to_qc[mb_folder] = qc_folder
            qc_to_mbrave[qc_folder].append(mb_folder)

        else:
            mbrave_to_qc[mb_folder] = None
            issues.append(
                f"mBRAVE '{mb_folder}' has no matching QC folder"
            )

    # QC folders with no mBRAVE counterpart
    for qc_folder in qc_resolved:
        if qc_folder not in qc_to_mbrave:
            c = classify_folder(qc_folder)
            if c['base_num'] is not None:
                issues.append(f"QC '{qc_folder}' has no corresponding mBRAVE folder")

    return (mbrave_to_qc, dict(qc_to_mbrave), issues,
            mbrave_resolved, qc_resolved, mbrave_skipped, qc_skipped)


# ── Plate ID utilities ────────────────────────────────────────────────────────

def extract_plate_from_pid(pid):
    """
    Strip trailing well coordinate from a pid string.
        "HIRW_001_A01"   -> "HIRW_001"
        "TOL-BGEP-008_H12" -> "TOL-BGEP-008"
    """
    if pd.isna(pid):
        return None
    pid = str(pid).strip()
    plate = re.sub(r'_[A-H]\d{1,2}$', '', pid)
    return plate


def normalise_plate_id(plate_id):
    if pd.isna(plate_id):
        return None
    return str(plate_id).strip()


def matches_partner(plate_id, partner):
    """True if plate_id belongs to partner, or partner is ALL/None."""
    if partner is None or str(partner).upper() == 'ALL':
        return True
    p   = str(partner).upper()
    pid = str(plate_id).upper()
    return (pid.startswith(p + '_') or pid.startswith(p + '-') or
            pid.startswith('TOL-' + p + '-'))


def is_bge_plate(plate_id):
    """
    Return True if plate_id belongs to a BGE partner (BGEP, BGEG, BGKU, BGPT).

    Handles both plain format (BGEP-161) and TOL-prefixed (TOL-BGEP-161).
    Importing config here (not at module top) avoids circular imports.
    """
    if not plate_id:
        return False
    from config import BGE_PARTNER_CODES
    pid = str(plate_id).upper()
    # Strip TOL- prefix for consistent matching
    if pid.startswith('TOL-'):
        pid = pid[4:]
    for code in BGE_PARTNER_CODES:
        if pid.startswith(code + '-') or pid.startswith(code + '_'):
            return True
    return False


# ── Safe file reading ─────────────────────────────────────────────────────────

def safe_read_csv(filepath, **kwargs):
    """Read CSV/TSV with encoding fallback."""
    for enc in ('utf-8', 'latin1', 'iso-8859-1', 'cp1252'):
        try:
            return pd.read_csv(filepath, encoding=enc, **kwargs)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode {filepath}")


# ── Glob helpers ──────────────────────────────────────────────────────────────

def find_file_in_batch(batch_path, pattern):
    """Find file matching glob pattern in batch_path. Returns path or None."""
    matches = glob.glob(os.path.join(batch_path, pattern))
    if not matches:
        return None
    if len(matches) > 1:
        print(f"  WARNING: multiple matches for '{pattern}' in "
              f"{os.path.basename(batch_path)}, using {os.path.basename(matches[0])}")
    return matches[0]


def batch_sort_key(folder_name):
    """Numeric sort: (base_num, split_num) so batch9 < batch10 < batch10_0."""
    c = classify_folder(folder_name)
    return (c['base_num'] if c['base_num'] is not None else 999999,
            c['split_num'] if c['split_num'] is not None else -1)


# ── Discovery / audit ─────────────────────────────────────────────────────────

def audit_batch_structure(mbrave_dir, qc_dir, verbose=True):
    """
    Print full audit of batch resolution and cross-mapping. Run this first
    on a new system to verify folder structure before running analysis.
    """
    (mbrave_to_qc, qc_to_mbrave, issues,
     mbrave_resolved, qc_resolved,
     mbrave_skipped, qc_skipped) = build_batch_cross_map(mbrave_dir, qc_dir)

    print("\n" + "=" * 70)
    print("BATCH STRUCTURE AUDIT")
    print("=" * 70)
    print(f"\nmBRAVE : {len(mbrave_resolved)} folders used, "
          f"{len(mbrave_skipped)} skipped")
    if mbrave_skipped and verbose:
        print(f"  Skipped: {sorted(mbrave_skipped, key=batch_sort_key)}")

    print(f"QC     : {len(qc_resolved)} folders used, "
          f"{len(qc_skipped)} skipped")
    if qc_skipped and verbose:
        print(f"  Skipped: {sorted(qc_skipped, key=batch_sort_key)}")

    # Categorise mappings
    exact      = {k: v for k, v in mbrave_to_qc.items()
                  if v is not None and classify_folder(k)['split_num'] ==
                  classify_folder(v)['split_num']}
    auto_merge = {k: v for k, v in mbrave_to_qc.items()
                  if v is not None and classify_folder(k)['type'] == 'split'
                  and classify_folder(v)['type'] == 'plain'}
    no_qc      = {k: v for k, v in mbrave_to_qc.items() if v is None}

    print(f"\nCross-mapping:")
    print(f"  Exact (split->split / plain->plain) : {len(exact)}")
    print(f"  Split->Plain (auto-merged QC)       : {len(auto_merge)}")
    if auto_merge and verbose:
        by_qc = defaultdict(list)
        for mb, qc in auto_merge.items():
            by_qc[qc].append(mb)
        for qc_f, mb_list in sorted(by_qc.items()):
            print(f"    {', '.join(sorted(mb_list, key=batch_sort_key))} -> {qc_f}")
    print(f"  No QC match                         : {len(no_qc)}")
    if no_qc and verbose:
        print(f"    {sorted(no_qc.keys(), key=batch_sort_key)}")

    if issues:
        print(f"\nISSUES ({len(issues)}):")
        for issue in issues:
            print(f"  WARNING: {issue}")
    else:
        print("\n  No cross-mapping issues detected")

    return mbrave_to_qc, qc_to_mbrave, issues


if __name__ == '__main__':
    import config
    audit_batch_structure(config.MBRAVE_DIR, config.QC_DIR, verbose=True)
