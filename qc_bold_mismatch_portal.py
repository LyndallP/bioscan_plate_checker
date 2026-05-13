"""
qc_bold_mismatch_portal.py

Flags specimens that have QC result = FAILED but still have a sequence
on BOLD. Uses the portal dump directly — no need to read QC batch files.

This should never happen: only QC-passed specimens should be on BOLD.
Any match is a data integrity issue requiring investigation.

Checks per flagged specimen:
  1. Is there a sequence on BOLD?          (bold_nuc non-empty)
  2. Is there a BIN assigned?              (bold_bin_uri non-empty)
  3. Is there BOLD taxonomy?               (bold_species, bold_family etc)

Usage:
    python3 qc_bold_mismatch_portal.py
    python3 qc_bold_mismatch_portal.py --exclude-bge
    python3 qc_bold_mismatch_portal.py --input /path/to/sts_manifests.tsv
"""

import argparse
import datetime
import glob
import os
import pandas as pd

import config
from utils import is_bge_plate

# Portal column names
_QC_RESULT_COL   = 'bioscan_qc_sanger_qc_result'
_QC_DESC_COL     = 'bioscan_qc_sanger_qc_description'
_SPECIMEN_COL    = 'sts_specimen.id'
_RACK_COL        = 'sts_rackid'
_BOLD_NUC_COL    = 'bold_nuc'
_BOLD_BIN_COL    = 'bold_bin_uri'
_BOLD_SPECIES    = 'bold_species'
_BOLD_FAMILY     = 'bold_family'
_BOLD_ORDER      = 'bold_order'
_BOLD_UPLOAD     = 'bold_sequence_upload_date'
_PARTNER_COL     = 'bold_bold_recordset_code_arr'
_SUBMIT_COL      = 'sts_submit_date'


def find_latest_dump(results_dir):
    candidates = sorted(glob.glob(os.path.join(results_dir, 'sts_manifests_*.tsv')))
    return candidates[-1] if candidates else None


def clean_partner(val):
    """Strip list brackets from portal partner field e.g. ['FACE'] -> FACE."""
    if not val or str(val) in ('None', 'nan', ''):
        return None
    val = str(val).strip().strip('[]').strip().strip("'\"")
    import re
    # Strip leading non-standard letter (SCAMP->CAMP, BNHMG->NHMG)
    if re.match(r'[A-Z][A-Z]{4}$', val):
        val = val[1:]
    return val or None


def extract_partner_from_rack(rack_id):
    """Fallback: extract 4-letter partner code from rack ID."""
    import re
    if not rack_id:
        return None
    m = re.match(r'^(?:TOL-)?([A-Z]{4})[_-]', str(rack_id))
    return m.group(1) if m else None


def main():
    parser = argparse.ArgumentParser(
        description='Find QC-FAILED specimens that have sequences on BOLD'
    )
    parser.add_argument('--input', default=None,
        help='Path to portal dump TSV (default: most recent in RESULTS_DIR)')
    parser.add_argument('--exclude-bge', action='store_true',
        help='Exclude BGE partners (BGEP, BGEG, BGKU, BGPT)')
    parser.add_argument('--output', default=None,
        help='Output CSV path (default: RESULTS_DIR/qc_bold_mismatch_YYYYMMDD.csv)')
    args = parser.parse_args()

    today = datetime.datetime.now().strftime('%Y%m%d')
    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    # Find dump
    dump_path = args.input or find_latest_dump(config.RESULTS_DIR)
    if not dump_path or not os.path.exists(dump_path):
        print("ERROR: No portal dump found. Run read_portal_dump.py --fetch first.")
        return

    print(f"Reading portal dump: {os.path.basename(dump_path)}")

    # Load only columns we need
    all_cols = [
        _SPECIMEN_COL, _RACK_COL, _QC_RESULT_COL, _QC_DESC_COL,
        _BOLD_NUC_COL, _BOLD_BIN_COL, _BOLD_SPECIES, _BOLD_FAMILY,
        _BOLD_ORDER, _BOLD_UPLOAD, _PARTNER_COL, _SUBMIT_COL,
    ]
    # Read header to find available columns
    peek = pd.read_csv(dump_path, sep='\t', dtype=str, nrows=0)
    use_cols = [c for c in all_cols if c in peek.columns]
    missing = [c for c in all_cols if c not in peek.columns]
    if missing:
        print(f"  Note: {len(missing)} columns not in dump: {missing}")

    df = pd.read_csv(dump_path, sep='\t', dtype=str, usecols=use_cols,
                     low_memory=False)
    print(f"  {len(df):,} rows loaded")

    # Clean up None strings
    for col in df.columns:
        df[col] = df[col].replace({'None': None, 'nan': None, '': None})

    # Extract plate ID and partner
    if _RACK_COL in df.columns:
        df['plate_id'] = df[_RACK_COL]
    elif _SPECIMEN_COL in df.columns:
        df['plate_id'] = df[_SPECIMEN_COL].str.extract(r'^(.+)_[A-H]\d{1,2}$')[0]

    df['partner'] = df[_PARTNER_COL].apply(clean_partner) if _PARTNER_COL in df.columns else None
    mask_no_partner = df['partner'].isna()
    if mask_no_partner.any() and 'plate_id' in df.columns:
        df.loc[mask_no_partner, 'partner'] = \
            df.loc[mask_no_partner, 'plate_id'].apply(extract_partner_from_rack)

    # Exclude BGE if requested
    if args.exclude_bge and 'plate_id' in df.columns:
        n_before = len(df)
        df = df[~df['plate_id'].apply(lambda x: is_bge_plate(str(x)) if x else False)]
        print(f"  Excluded {n_before - len(df):,} BGE partner rows")

    # QC result distribution
    if _QC_RESULT_COL in df.columns:
        print(f"\nQC result distribution:")
        for val, n in df[_QC_RESULT_COL].value_counts(dropna=False).items():
            print(f"  {val}: {n:,}")
    else:
        print(f"ERROR: {_QC_RESULT_COL} not found in dump.")
        return

    # Find FAILED specimens
    failed = df[df[_QC_RESULT_COL] == 'FAILED'].copy()
    print(f"\nQC-FAILED specimens: {len(failed):,}")

    # Check 1: sequence on BOLD
    has_seq = failed[_BOLD_NUC_COL].notna() if _BOLD_NUC_COL in failed.columns \
              else pd.Series(False, index=failed.index)

    # Check 2: BIN assigned
    has_bin = failed[_BOLD_BIN_COL].notna() if _BOLD_BIN_COL in failed.columns \
              else pd.Series(False, index=failed.index)

    # Check 3: any BOLD taxonomy
    tax_cols = [c for c in [_BOLD_SPECIES, _BOLD_FAMILY, _BOLD_ORDER] if c in failed.columns]
    has_tax = failed[tax_cols].notna().any(axis=1) if tax_cols \
              else pd.Series(False, index=failed.index)

    # Mismatch = FAILED but has sequence on BOLD
    mismatch = failed[has_seq].copy()
    mismatch['has_bin']      = has_bin[has_seq]
    mismatch['has_taxonomy'] = has_tax[has_seq]

    print(f"\n=== MISMATCH RESULTS ===")
    print(f"QC-FAILED specimens with sequence on BOLD: {len(mismatch):,}")
    if len(mismatch) > 0:
        print(f"  Of these, with BIN assigned:   {mismatch['has_bin'].sum():,}")
        print(f"  Of these, with BOLD taxonomy:  {mismatch['has_taxonomy'].sum():,}")
        print(f"\nBy partner:")
        print(mismatch['partner'].value_counts().to_string())
        print(f"\nSample rows:")
        show_cols = [c for c in [_SPECIMEN_COL, 'plate_id', 'partner',
                                  _QC_RESULT_COL, _QC_DESC_COL,
                                  _BOLD_BIN_COL, _BOLD_SPECIES,
                                  _BOLD_UPLOAD] if c in mismatch.columns]
        print(mismatch[show_cols].head(10).to_string(index=False))
    else:
        print("  ✅ No mismatches found — clean data integrity.")

    # Also report: FAILED with no sequence (expected, for completeness)
    no_seq = failed[~has_seq]
    print(f"\nQC-FAILED specimens with NO sequence on BOLD: {len(no_seq):,} (expected)")

    # Save output
    if args.output is None:
        args.output = os.path.join(
            config.RESULTS_DIR, f'qc_bold_mismatch_{today}.csv')

    # Save full mismatch table
    out_cols = [c for c in [_SPECIMEN_COL, 'plate_id', 'partner',
                              _QC_RESULT_COL, _QC_DESC_COL,
                              _BOLD_NUC_COL, _BOLD_BIN_COL,
                              _BOLD_SPECIES, _BOLD_FAMILY, _BOLD_ORDER,
                              _BOLD_UPLOAD, _SUBMIT_COL,
                              'has_bin', 'has_taxonomy'] if c in mismatch.columns]

    if len(mismatch) > 0:
        mismatch[out_cols].to_csv(args.output, index=False)
        print(f"\nOutput: {args.output}")
        print(f"  {len(mismatch):,} mismatch records saved")
    else:
        # Write empty file with headers so downstream scripts don't break
        pd.DataFrame(columns=out_cols).to_csv(args.output, index=False)
        print(f"\nOutput: {args.output} (empty — no mismatches)")


if __name__ == '__main__':
    main()
