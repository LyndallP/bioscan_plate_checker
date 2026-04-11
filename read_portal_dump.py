"""
read_portal_dump.py

Reads the pre-exported portal dump TSV (sts_manifests_YYYYMMDD.tsv) and
builds a plate-level summary CSV used by plate_status_report.py.

This replaces live portal queries which take ~2 hours for 350k+ samples.

Input columns used:
    col 1  : sts_specimen.id  — specimen+well ID e.g. "BGEP-161_A1"
    col 25 : bold_nuc         — sequence uploaded to BOLD (non-empty = uploaded)
    col 59 : sts_submit_date  — submission date

Partner is extracted from plate ID prefix:
    HIRW_001     -> HIRW
    BGEP-161     -> BGEP
    TOL-BGEP-001 -> BGEP
    MOZZ00000609A -> MOZZ

Usage:
    # Build the plate summary from the dump (run once)
    python3 read_portal_dump.py --input /path/to/sts_manifests_20260408.tsv

    # Specify output path
    python3 read_portal_dump.py --input /path/to/sts_manifests_20260408.tsv \\
                                --output /path/to/portal_plates.csv
"""

import argparse
import os
import re
import datetime
import pandas as pd
from collections import defaultdict

import config
from utils import extract_plate_from_pid

# Default path to the portal dump
DEFAULT_DUMP = "/lustre/scratch126/tol/teams/lawniczak/projects/bioscan/100k_paper/output/sts_manifests_20260408.tsv"

# Column names we need
_SPECIMEN_COL  = 'sts_specimen.id'
_BOLD_COL      = 'bold_nuc'
_SUBMIT_COL    = 'sts_submit_date'


def extract_partner_from_plate(plate_id):
    """
    Extract 4-letter partner code from plate ID.
        HIRW_001     -> HIRW
        BGEP-161     -> BGEP
        TOL-BGEP-001 -> BGEP
        MOZZ00000609A -> MOZZ
        BSN_001      -> BSN
        CAMP_001     -> CAMP
    """
    if not plate_id:
        return None
    pid = str(plate_id).upper()
    # TOL-XXXX- format
    m = re.match(r'^TOL-([A-Z]{4})-', pid)
    if m:
        return m.group(1)
    # XXXX- or XXXX_ format (4 letters)
    m = re.match(r'^([A-Z]{4})[-_]', pid)
    if m:
        return m.group(1)
    # MOZZ format (longer prefix)
    m = re.match(r'^(MOZZ)', pid)
    if m:
        return 'MOZZ'
    return None


def is_control_plate(plate_id):
    """Return True if plate_id is a control, not a real plate."""
    if not plate_id:
        return True
    pid = str(plate_id).upper()
    return (pid.startswith('CONTROL_') or
            pid.startswith('CONTROL-') or
            'CONTROL_NEG' in pid or
            'CONTROL_POS' in pid)


def build_portal_plate_summary(dump_path, output_path, verbose=True):
    """
    Read portal dump TSV and write plate-level summary CSV.

    Output columns:
        plate_id | partner | submit_date | bold_uploaded | n_wells_portal
    """
    print(f"Reading portal dump: {dump_path}")

    # Read only the columns we need
    df = pd.read_csv(dump_path, sep='\t', dtype=str,
                     usecols=[_SPECIMEN_COL, _BOLD_COL, _SUBMIT_COL],
                     low_memory=False)

    print(f"  {len(df)} rows loaded")

    # Extract plate ID from specimen ID
    df['plate_id'] = df[_SPECIMEN_COL].apply(extract_plate_from_pid)

    # Remove controls and blanks
    df = df[~df['plate_id'].apply(is_control_plate)]
    df = df[df['plate_id'].notna()]
    df = df[df['plate_id'] != 'NA']
    df = df[df['plate_id'] != 'None']

    # Extract partner
    df['partner'] = df['plate_id'].apply(extract_partner_from_plate)

    # BOLD: True if bold_nuc is non-empty and not 'None'
    df['bold_uploaded'] = (
        df[_BOLD_COL].notna() &
        (df[_BOLD_COL] != 'None') &
        (df[_BOLD_COL].str.strip() != '')
    )

    # Submit date: keep date part only
    df['submit_date'] = df[_SUBMIT_COL].str[:10].replace('None', None)

    # Aggregate to plate level
    print("  Aggregating to plate level...")
    plate_rows = []
    for plate_id, grp in df.groupby('plate_id'):
        partner = grp['partner'].dropna().iloc[0] if grp['partner'].notna().any() else None
        dates = grp['submit_date'].dropna()
        dates = dates[dates != 'None']
        submit_date = sorted(dates)[0] if len(dates) > 0 else None
        bold_uploaded = bool(grp['bold_uploaded'].any())
        plate_rows.append({
            'plate_id':       plate_id,
            'partner':        partner,
            'submit_date':    submit_date,
            'bold_uploaded':  bold_uploaded,
            'n_wells_portal': len(grp),
        })

    result = pd.DataFrame(plate_rows).sort_values('plate_id').reset_index(drop=True)

    # Summary
    print(f"\nPortal plate summary:")
    print(f"  Total plates       : {len(result)}")
    print(f"  With submit date   : {result['submit_date'].notna().sum()}")
    print(f"  BOLD uploaded      : {result['bold_uploaded'].sum()}")
    print(f"  Unique partners    : {result['partner'].nunique()}")
    print(f"  Partners           : {sorted(result['partner'].dropna().unique())}")

    # Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    result.to_csv(output_path, index=False)
    print(f"\nSaved to: {output_path}")

    return result


def load_portal_plate_summary(csv_path):
    """Load the pre-built plate summary CSV."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"Portal plate summary not found: {csv_path}\n"
            f"Run: python3 read_portal_dump.py --input {DEFAULT_DUMP}"
        )
    df = pd.read_csv(csv_path, dtype=str)
    df['bold_uploaded'] = df['bold_uploaded'].map(
        {'True': True, 'False': False, True: True, False: False}
    ).fillna(False)
    df['n_wells_portal'] = pd.to_numeric(df['n_wells_portal'], errors='coerce')
    return df


def main():
    parser = argparse.ArgumentParser(
        description='Build plate summary from portal dump TSV'
    )
    parser.add_argument('--input', default=DEFAULT_DUMP,
        help=f'Path to portal dump TSV (default: {DEFAULT_DUMP})')
    parser.add_argument('--output', default=None,
        help='Output CSV path (default: RESULTS_DIR/portal_plates_from_dump.csv)')
    args = parser.parse_args()

    if args.output is None:
        os.makedirs(config.RESULTS_DIR, exist_ok=True)
        args.output = os.path.join(config.RESULTS_DIR, 'portal_plates_from_dump.csv')

    build_portal_plate_summary(args.input, args.output)


if __name__ == '__main__':
    main()
