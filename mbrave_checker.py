"""
mbrave_checker.py

Scans mbrave_batch_data and returns, for each plate ID, which batches
it appeared in (based on consensusseq_network.tsv files).

Uses resolve_batches() — splits always preferred over plain/merged.
"""

import os
import pandas as pd
from collections import defaultdict

import config
from utils import (resolve_batches, find_file_in_batch, extract_plate_from_pid,
                   safe_read_csv, batch_sort_key, matches_partner, classify_folder)


def get_plates_from_batch(batch_folder, batch_path, verbose=False):
    """
    Extract unique plate IDs from consensusseq_network file in batch_path.
    Returns set of plate ID strings.
    """
    plates = set()

    tsv_file = find_file_in_batch(batch_path, config.CONSENSUSSEQ_NETWORK_PATTERN)
    if tsv_file:
        try:
            df = safe_read_csv(tsv_file, sep='\t',
                               usecols=[config.MBRAVE_PID_COL], dtype=str)
            for pid in df[config.MBRAVE_PID_COL].dropna():
                plate = extract_plate_from_pid(pid)
                if plate:
                    plates.add(plate)
            if verbose:
                print(f"  {batch_folder}: {len(plates)} plates "
                      f"({os.path.basename(tsv_file)})")
            return plates
        except Exception as e:
            print(f"  WARNING: failed reading {tsv_file}: {e}")

    # Fallback to CSV
    csv_file = find_file_in_batch(batch_path, config.CONSENSUSSEQ_NETWORK_CSV_PATTERN)
    if csv_file:
        try:
            df = safe_read_csv(csv_file, usecols=[config.MBRAVE_PID_COL], dtype=str)
            for pid in df[config.MBRAVE_PID_COL].dropna():
                plate = extract_plate_from_pid(pid)
                if plate:
                    plates.add(plate)
            if verbose:
                print(f"  {batch_folder}: {len(plates)} plates "
                      f"({os.path.basename(csv_file)}, CSV fallback)")
            return plates
        except Exception as e:
            print(f"  WARNING: failed reading {csv_file}: {e}")

    print(f"  WARNING: no consensusseq_network file in {batch_path}")
    return plates


def build_mbrave_plate_index(mbrave_dir=None, partner=None, verbose=False):
    """
    Scan all resolved mBRAVE batches and return:
        plate_to_batches: dict  plate_id -> [batch_folder, ...]  (sorted)
        resolved:         list of batch folders used
        skipped:          list of batch folders excluded by dedup
    """
    if mbrave_dir is None:
        mbrave_dir = config.MBRAVE_DIR

    resolved, skipped = resolve_batches(mbrave_dir)

    if verbose:
        print(f"\nmBRAVE: using {len(resolved)} batches, "
              f"skipped {len(skipped)}: {skipped}")

    plate_to_batches = defaultdict(list)

    for batch_folder in resolved:
        batch_path = os.path.join(mbrave_dir, batch_folder)
        plates = get_plates_from_batch(batch_folder, batch_path, verbose=verbose)
        for plate in plates:
            if matches_partner(plate, partner):
                plate_to_batches[plate].append(batch_folder)

    # Sort each plate's batch list
    for plate in plate_to_batches:
        plate_to_batches[plate].sort(key=batch_sort_key)

    return dict(plate_to_batches), resolved, skipped


def summarise_mbrave(plate_to_batches):
    n_plates   = len(plate_to_batches)
    n_repeated = sum(1 for v in plate_to_batches.values() if len(v) > 1)
    n_batches  = len({b for batches in plate_to_batches.values() for b in batches})
    print(f"\nmBRAVE: {n_plates} unique plates across {n_batches} batches "
          f"({n_repeated} plates sequenced more than once)")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--partner', default='ALL')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    plate_to_batches, resolved, skipped = build_mbrave_plate_index(
        partner=args.partner, verbose=args.verbose)
    summarise_mbrave(plate_to_batches)
    if skipped:
        print(f"Skipped: {skipped}")
