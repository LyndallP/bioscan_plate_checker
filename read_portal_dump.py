"""
read_portal_dump.py

Fetches a fresh BIOSCAN specimen dump from the ToL portal using the `tol`
CLI, saves it as a dated TSV, then builds a plate-level summary CSV used
by plate_status_report.py and the rest of the pipeline.

The dated dump is saved alongside all other pipeline outputs so that every
reporting run has a fully reproducible, auditable input file.

Fetch command used:
    tol data --source=portal --operation=list --type=sample \\
        --filter='{"and_":{"sts_project":{"in_list":{"value":["BIOSCAN"],"negate":false}}}}' \\
        --fields=sts_rackid,sts_specimen.id,bold_nuc,sts_submit_date,\\
                 bold_bold_recordset_code_arr,bold_bin_uri,sts_species.sts_scientific_name \\
        --output=tsv

Input columns used from dump:
    sts_specimen.id               — specimen+well ID e.g. "BGEP-161_A1"
    bold_nuc                      — sequence uploaded to BOLD (non-empty = uploaded)
    sts_submit_date               — plate submission date
    bold_bin_uri                  — BIN URI if assigned
    bold_bold_recordset_code_arr  — partner code (supplementary cross-check)
    sts_species.sts_scientific_name — "unidentified" = specimen present,
                                      "blank" = empty well (partial plate / control)

Partner is extracted from plate ID prefix:
    HIRW_001      -> HIRW
    BGEP-161      -> BGEP
    TOL-BGEP-001  -> BGEP
    MOZZ00000609A -> MOZZ

Usage:
    # Fetch fresh dump from portal AND build plate summary (recommended)
    python3 read_portal_dump.py --fetch

    # Build plate summary from existing dump (faster, no portal query)
    python3 read_portal_dump.py --input /path/to/sts_manifests_20260501.tsv

    # Fetch and save to custom path
    python3 read_portal_dump.py --fetch --output /path/to/portal_plates.csv
"""

import argparse
import os
import re
import subprocess
import datetime
import pandas as pd

import config
from utils import extract_plate_from_pid

# Column names from portal dump
_SPECIMEN_COL  = 'sts_specimen.id'
_BOLD_COL      = 'bold_nuc'
_SUBMIT_COL    = 'sts_submit_date'
_BIN_COL       = 'bold_bin_uri'
_PARTNER_COL   = 'bold_bold_recordset_code_arr'  # supplementary only
_SPECIES_COL   = 'sts_species.sts_scientific_name'

# Portal fetch settings
_PORTAL_FILTER = (
    '{"and_":{"sts_project":{"in_list":{"value":["BIOSCAN"],"negate":false}}}}'
)
_PORTAL_FIELDS = (
    'bioscan_extra_adult_feeding_guild,bioscan_extra_associations,bioscan_extra_broad_biotope_habitat_resources,bioscan_extra_current_conservation_status,bioscan_extra_larval_feeding_guild,bioscan_extra_link_to_assemblage,bioscan_extra_specific_assemblage_type,bioscan_extra_vernacular,bioscan_qc_sanger_qc_description,bioscan_qc_sanger_qc_result,bioscan_qc_uksi_name_status,bold_associated_specimens,bold_associated_taxa,bold_bin_created_date,bold_bin_uri,bold_bold_recordset_code_arr,bold_class,bold_collection_code,bold_collection_date_end,bold_collection_date_start,bold_collection_event_id,bold_collection_notes,bold_collection_time,bold_collectors,bold_coord,bold_coord_accuracy,bold_coord_source,bold_country/ocean,bold_country_iso,bold_elev,bold_elev_accuracy,bold_family,bold_fieldid,bold_genus,bold_geoid,bold_habitat,bold_identification_method,bold_identified_by,bold_inst,bold_insdc_acs,bold_kingdom,bold_life_stage,bold_marker_code,bold_museumid,bold_notes,bold_nuc,bold_nuc_basecount,bold_order,bold_phylum,bold_processid,bold_processid_minted_date,bold_province/state,bold_record_id,bold_region,bold_reproduction,bold_sampling_protocol,bold_sector,bold_sequence_run_site,bold_sequence_upload_date,bold_sex,bold_short_note,bold_site,bold_site_code,bold_species,bold_species_reference,bold_specimenid,bold_subfamily,bold_taxid,bold_taxonomy_notes,bold_tissue_type,bold_tribe,bold_voucher_type,broad_biotope_habitat_resources,current_conservation_status,id,larval_feeding_guild,link_to_assemblage,mlwh_sequencing_request_count,mlwh_sequencing_request_mlwh_order_date_max,mlwh_sequencing_request_mlwh_order_date_min,specific_assemblage_type,sts_AMOUNT_OF_CATCH_PLATED,sts_BOLD_ACCESSION_NUMBER,sts_BOTTLE_DIRECTION,sts_CATCH_BOTTLE_TEMPERATURE_STORAGE,sts_CATCH_LOT,sts_CATCH_SOLUTION,sts_COLLECTION_METHOD,sts_CONTRIBUTORS,sts_COUNTRY_OF_COLLECTION,sts_DATE_OF_PLATING,sts_DURATION_OF_COLLECTION,sts_MORPHOSPECIES_DESCRIPTION,sts_PLATE_TEMPERATURE_STORAGE,sts_PREDICTED_FAMILY,sts_PREDICTED_GENUS,sts_PREDICTED_ORDER_OR_GROUP,sts_PREDICTED_SCIENTIFIC_NAME,sts_SORTING_SOLUTION_USED,sts_WHAT_3_WORDS,sts_col_date,sts_col_time,sts_collection_country,sts_collection_locality,sts_collection_method_desc,sts_created_on,sts_depth,sts_elevation,sts_gal_abbreviation,sts_gal_name,sts_habitat,sts_identify_name,sts_labwhere_name,sts_latitude,sts_lifestage,sts_location,sts_longitude,sts_organism_part,sts_other_info,sts_preservation_approach,sts_preservative_solution,sts_rackid,sts_receive_date,sts_sex,sts_voucherid,uksi_name_status,vernacular,sts_sampleset.id,sts_manifest.id,adult_feeding_guild,associations,sts_specimen.id,sts_submit_date,sts_species.sts_scientific_name,bioscan_image_url,sts_specimen_risk'
)



def fetch_portal_dump(output_path=None, verbose=True):
    """
    Fetch a fresh BIOSCAN specimen dump from the ToL portal using the
    tol CLI and save as a dated TSV in the results directory.

    Takes ~2 hours. Run inside tmux so it survives connection drops:
        tmux new -s portal_dump
        python3 read_portal_dump.py --fetch
        Ctrl+B then D to detach; tmux attach -s portal_dump to reattach
    """
    today = datetime.datetime.now().strftime('%Y%m%d')
    if output_path is None:
        os.makedirs(config.RESULTS_DIR, exist_ok=True)
        output_path = os.path.join(config.RESULTS_DIR, f'sts_manifests_{today}.tsv')

    tol_exe = '/software/treeoflife/conda/users/envs/team222/lp20/bioscan-ops/bin/tol'

    cmd = [
        tol_exe, 'data',
        '--source=portal',
        '--operation=list',
        '--type=sample',
        f'--filter={_PORTAL_FILTER}',
        f'--fields={_PORTAL_FIELDS}',
        '--output=tsv',
    ]

    if verbose:
        print(f"Fetching BIOSCAN portal dump...")
        print(f"  Output: {output_path}")
        print(f"  Command: {' '.join(cmd)}")
        print()

    with open(output_path, 'w') as fout:
        result = subprocess.run(cmd, stdout=fout, stderr=subprocess.PIPE, text=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"Portal fetch failed (exit code {result.returncode}):\n{result.stderr}"
        )

    size = os.path.getsize(output_path)
    if size < 1000:
        raise RuntimeError(f"Portal dump looks empty ({size} bytes): {output_path}")

    if verbose:
        print(f"  Done. {size / 1024 / 1024:.1f} MB saved to: {output_path}")
        if result.stderr:
            print(f"  Warnings: {result.stderr[:300]}")

    return output_path

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

    # Read columns we need — handle missing columns gracefully
    # (older dumps may not have bin_uri or species columns)
    all_cols = [_SPECIMEN_COL, _BOLD_COL, _SUBMIT_COL, _BIN_COL, _SPECIES_COL]
    peek = pd.read_csv(dump_path, sep='\t', dtype=str, nrows=0)
    available = [c for c in all_cols if c in peek.columns]
    df = pd.read_csv(dump_path, sep='\t', dtype=str,
                     usecols=available, low_memory=False)
    # Add missing optional columns as empty
    for col in all_cols:
        if col not in df.columns:
            df[col] = None

    print(f"  {len(df)} rows loaded")

    # Extract plate ID from specimen ID
    df['plate_id'] = df[_SPECIMEN_COL].apply(extract_plate_from_pid)

    # Remove controls and blank wells
    # "blank" in sts_species.sts_scientific_name = empty well (partial plate,
    # positive/negative control well). These should not count as specimens.
    # Note: the random negative SQPP specimen is assigned AFTER portal submission
    # so it may appear as "unidentified" here — that is correct behaviour.
    df = df[~df['plate_id'].apply(is_control_plate)]
    df = df[df['plate_id'].notna()]
    df = df[df['plate_id'] != 'NA']
    df = df[df['plate_id'] != 'None']
    if _SPECIES_COL in df.columns:
        n_blank_total = (df[_SPECIES_COL].str.lower() == 'blank').sum()
        df = df[df[_SPECIES_COL].str.lower() != 'blank']
        print(f"  Removed {n_blank_total} blank wells (empty/control wells from portal)")

    # Partner: use bold_bold_recordset_code_arr from portal if available.
    # The field comes through as a stringified list e.g. "['FACE']" or "['SCAMP']"
    # so we need to strip brackets and quotes.
    # Fall back to plate ID parsing for rows where the field is None/empty.
    if _PARTNER_COL in df.columns:
        def clean_partner(val):
            if not val or val in ('None', 'nan', ''):
                return None
            # Strip list brackets and quotes: "['FACE']" -> "FACE"
            val = str(val).strip()
            val = val.strip('[]').strip().strip("'\"")
            # Some partners have a prefix letter added (SCAMP->CAMP, BNHMG->NHMG)
            # These are BIOSCAN-specific recordset codes with a leading letter
            # Strip leading non-standard letter if result is 5 chars and
            # last 4 are uppercase letters
            import re
            if re.match(r'[A-Z][A-Z]{4}$', val) and not re.match(r'[A-Z]{4}$', val):
                val = val[1:]  # strip leading letter e.g. SCAMP->CAMP, BNHMG->NHMG
            return val if val else None

        df['partner'] = df[_PARTNER_COL].apply(clean_partner)
        # Fill remaining None from plate ID
        missing = df['partner'].isna()
        if missing.any():
            df.loc[missing, 'partner'] = (
                df.loc[missing, 'plate_id'].apply(extract_partner_from_plate)
            )
    else:
        df['partner'] = df['plate_id'].apply(extract_partner_from_plate)

    # BOLD: True if bold_nuc is non-empty and not 'None'
    df['bold_uploaded'] = (
        df[_BOLD_COL].notna() &
        (df[_BOLD_COL] != 'None') &
        (df[_BOLD_COL].str.strip() != '')
    )
    # BIN: True if bold_bin_uri is populated
    if _BIN_COL in df.columns:
        df['has_bin'] = (
            df[_BIN_COL].notna() &
            (df[_BIN_COL] != 'None') &
            (df[_BIN_COL].str.strip() != '')
        )
    else:
        df['has_bin'] = False

    # Submit date: keep date part only
    df['submit_date'] = df[_SUBMIT_COL].str[:10].replace('None', None)

    # Aggregate to plate level
    print("  Aggregating to plate level...")
    plate_rows = []
    for plate_id, grp in df.groupby('plate_id'):
        # Use most common partner value — portal field may have slight variation
        partner_vals = grp['partner'].dropna()
        partner = partner_vals.mode().iloc[0] if len(partner_vals) > 0 else None
        dates = grp['submit_date'].dropna()
        dates = dates[dates != 'None']
        submit_date = sorted(dates)[0] if len(dates) > 0 else None
        bold_uploaded  = bool(grp['bold_uploaded'].any())
        n_with_bin     = int(grp['has_bin'].sum()) if 'has_bin' in grp else 0
        # Species: "blank" = empty well, "unidentified" = specimen present
        n_blank = 0
        n_specimen = 0
        if _SPECIES_COL in grp.columns:
            n_blank    = int((grp[_SPECIES_COL].str.lower() == 'blank').sum())
            n_specimen = int((grp[_SPECIES_COL].str.lower() == 'unidentified').sum())
        plate_rows.append({
            'plate_id':       plate_id,
            'partner':        partner,
            'submit_date':    submit_date,
            'bold_uploaded':  bold_uploaded,
            'n_wells_portal': len(grp),
            'n_with_bin':     n_with_bin,
            'n_blank_wells':  n_blank,
            'n_specimen_wells': n_specimen,
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
            f"Run: python3 read_portal_dump.py --fetch"
        )
    df = pd.read_csv(csv_path, dtype=str)
    df['bold_uploaded'] = df['bold_uploaded'].map(
        {'True': True, 'False': False, True: True, False: False}
    ).fillna(False)
    df['n_wells_portal'] = pd.to_numeric(df['n_wells_portal'], errors='coerce')
    return df


def main():
    parser = argparse.ArgumentParser(
        description='Fetch portal dump and build plate summary CSV'
    )
    parser.add_argument('--fetch', action='store_true',
        help='Fetch a fresh dump from the portal (takes ~2 hours, run in tmux)')
    parser.add_argument('--input', default=None,
        help='Path to existing portal dump TSV (skip fetch; default: config.py PORTAL_DUMP_TSV)')
    parser.add_argument('--output', default=None,
        help='Output CSV path (default: RESULTS_DIR/portal_plates_from_dump.csv)')
    args = parser.parse_args()

    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    # Step 1: get the dump TSV
    if args.fetch:
        dump_path = fetch_portal_dump(output_path=args.input)
    elif args.input:
        dump_path = args.input
    else:
        # Defaults to the pipeline's single source of truth in config.py, same
        # as plate_status_report.py / bold_summary_from_portal.py / bold_check.R
        dump_path = config.PORTAL_DUMP_TSV
        if not dump_path or not os.path.exists(dump_path):
            parser.error(
                f'Portal dump not found: {dump_path}\n'
                f'Check PORTAL_DUMP_TSV in config.py, run with --fetch to download '
                f'a new one, or provide --input /path/to/sts_manifests_YYYYMMDD.tsv'
            )
        print(f"Using dump from config.py PORTAL_DUMP_TSV: {dump_path}")

    # Step 2: build plate summary
    # --fetch runs synchronously so the file exists now

    if args.output is None:
        args.output = os.path.join(config.RESULTS_DIR, 'portal_plates_from_dump.csv')

    build_portal_plate_summary(dump_path, args.output)


if __name__ == '__main__':
    main()
