"""
bold_summary_from_portal.py

Generates BOLD upload and BIN assignment summaries directly from the
portal dump TSV — no API call, no R, no BOLDconnectR needed.

Fields used from portal dump:
    sts_specimen.id         -> specimen ID (plate extracted from this)
    bold_nuc                -> non-null = sequence uploaded to BOLD
    bold_sequence_upload_date -> when uploaded
    bold_bin_uri            -> BIN assignment (null = no BIN yet)
    bold_bin_created_date   -> when BIN was assigned

Produces:
    bold_summary_report_YYYYMMDD.txt   - human-readable report
    bold_missing_bin_YYYYMMDD.csv      - specimens with seq but no BIN
    bold_plate_summary_YYYYMMDD.csv    - plate-level BOLD status

Usage:
    python3 bold_summary_from_portal.py
    python3 bold_summary_from_portal.py --partner FACE
    python3 bold_summary_from_portal.py --input /path/to/sts_manifests.tsv
"""

import argparse
import datetime
import os
import re
import pandas as pd

import config
from utils import extract_plate_from_pid, matches_partner, is_bge_plate


# ── Column names in portal dump ───────────────────────────────────────────────
_SPECIMEN_COL    = 'sts_specimen.id'
_BOLD_NUC_COL    = 'bold_nuc'
_UPLOAD_DATE_COL = 'bold_sequence_upload_date'
_BIN_URI_COL     = 'bold_bin_uri'
_BIN_DATE_COL    = 'bold_bin_created_date'
_SUBMIT_DATE_COL = 'sts_submit_date'

_USECOLS = [_SPECIMEN_COL, _BOLD_NUC_COL, _UPLOAD_DATE_COL,
            _BIN_URI_COL, _BIN_DATE_COL, _SUBMIT_DATE_COL]


def extract_partner_from_plate(plate_id):
    if not plate_id:
        return None
    pid = str(plate_id)
    m = re.match(r'^TOL-([A-Z]{4})-', pid)
    if m:
        return m.group(1)
    m = re.match(r'^([A-Z]{4})[-_]', pid)
    if m:
        return m.group(1)
    if pid.upper().startswith('MOZZ'):
        return 'MOZZ'
    return None


def is_control(specimen_id):
    sid = str(specimen_id).upper()
    return (sid.startswith('CONTROL_NEG') or
            sid.startswith('CONTROL_POS') or
            sid.startswith('CONTROL-'))


def load_portal_dump(dump_path, partner=None, exclude_bge=False):
    """Load portal dump and return specimen-level DataFrame."""
    print(f"Reading portal dump: {dump_path}")
    df = pd.read_csv(dump_path, sep='\t', dtype=str,
                     usecols=_USECOLS, low_memory=False)
    print(f"  {len(df)} rows loaded")

    # Extract plate ID
    df['plate_id'] = df[_SPECIMEN_COL].apply(extract_plate_from_pid)
    df['partner']  = df['plate_id'].apply(extract_partner_from_plate)

    # Remove controls and blanks
    df = df[~df[_SPECIMEN_COL].apply(is_control)]
    df = df[df['plate_id'].notna()]
    df = df[~df['plate_id'].isin(['NA', 'None', ''])]

    if exclude_bge:
        n_before = len(df)
        df = df[~df['plate_id'].apply(is_bge_plate)]
        n_removed = n_before - len(df)
        if n_removed > 0:
            print(f"  Excluded {n_removed} BGE partner rows (BGEP/BGEG/BGKU/BGPT)")

    # Fix None strings
    for col in [_BOLD_NUC_COL, _UPLOAD_DATE_COL, _BIN_URI_COL, _BIN_DATE_COL]:
        df[col] = df[col].replace({'None': None, 'nan': None, '': None})

    # Derived boolean fields
    df['bold_uploaded']  = df[_BOLD_NUC_COL].notna() & (df[_BOLD_NUC_COL].str.len() > 10)
    df['has_bin']        = df[_BIN_URI_COL].notna() & (df[_BIN_URI_COL] != 'None')
    df['upload_date']    = df[_UPLOAD_DATE_COL].str[:10]
    df['bin_date']       = df[_BIN_DATE_COL].str[:10]
    df['submit_date']    = df[_SUBMIT_DATE_COL].str[:10]

    # Partner filter
    if partner and partner.upper() != 'ALL':
        df = df[df['partner'] == partner.upper()]
        print(f"  Filtered to partner '{partner}': {len(df)} rows")

    print(f"  {df['plate_id'].nunique()} unique plates")
    print(f"  {df['bold_uploaded'].sum()} specimens uploaded to BOLD")
    return df


def build_plate_summary(df):
    """Aggregate to plate level."""
    rows = []
    for plate_id, grp in df.groupby('plate_id'):
        partner    = grp['partner'].dropna().iloc[0] if grp['partner'].notna().any() else None
        n_total    = len(grp)
        n_uploaded = grp['bold_uploaded'].sum()
        n_with_bin = grp['has_bin'].sum()
        n_no_bin   = (grp['bold_uploaded'] & ~grp['has_bin']).sum()

        upload_dates = grp.loc[grp['bold_uploaded'], 'upload_date'].dropna()
        earliest_upload = sorted(upload_dates)[0] if len(upload_dates) > 0 else None
        latest_upload   = sorted(upload_dates)[-1] if len(upload_dates) > 0 else None

        rows.append({
            'plate_id':        plate_id,
            'partner':         partner,
            'n_specimens':     n_total,
            'n_bold_uploaded': int(n_uploaded),
            'n_with_bin':      int(n_with_bin),
            'n_no_bin':        int(n_no_bin),
            'pct_uploaded':    round(100 * n_uploaded / n_total, 1) if n_total > 0 else 0,
            'earliest_upload': earliest_upload,
            'latest_upload':   latest_upload,
        })
    return pd.DataFrame(rows).sort_values('plate_id').reset_index(drop=True)


def generate_report(df, plate_df, partner, output_path):
    """Write human-readable report."""
    today_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    lines = []

    def h(t): lines.extend(['', '=' * 65, t, '=' * 65])
    def s(t): lines.extend(['', f'--- {t} ---'])

    h("BOLD UPLOAD SUMMARY — FROM PORTAL DUMP")
    lines.append(f"Generated : {today_str}")
    lines.append(f"Partner   : {partner}")
    lines.append(f"Source    : {config.PORTAL_DUMP_TSV}")

    h("OVERALL")
    n_total    = len(df)
    n_uploaded = df['bold_uploaded'].sum()
    n_with_bin = df['has_bin'].sum()
    n_no_bin   = (df['bold_uploaded'] & ~df['has_bin']).sum()
    n_not_uploaded = (~df['bold_uploaded']).sum()

    lines.append(f"  Total specimens in portal        : {n_total}")
    lines.append(f"  Uploaded to BOLD                 : {n_uploaded} "
                f"({100*n_uploaded//n_total if n_total else 0}%)")
    lines.append(f"  With BIN URI                     : {n_with_bin}")
    lines.append(f"  Uploaded but NO BIN URI          : {n_no_bin}  ← needs follow-up")
    lines.append(f"  Not yet uploaded to BOLD         : {n_not_uploaded}")
    lines.append(f"  Plates with missing BINs         : "
                f"{(plate_df['n_no_bin'] > 0).sum()}")

    h("BOLD UPLOAD STATUS BY PARTNER")
    partner_grp = df.groupby('partner').agg(
        n_specimens  = ('bold_uploaded', 'count'),
        n_uploaded   = ('bold_uploaded', 'sum'),
        n_with_bin   = ('has_bin', 'sum'),
        n_no_bin     = ('bold_uploaded', lambda x: (x & ~df.loc[x.index, 'has_bin']).sum()),
    ).reset_index()
    partner_grp['pct_uploaded'] = (100 * partner_grp['n_uploaded'] /
                                   partner_grp['n_specimens']).round(1)
    partner_grp = partner_grp.sort_values('n_no_bin', ascending=False)

    lines.append(f"  {'Partner':<8} {'Specimens':>10} {'Uploaded':>9} "
                f"{'With BIN':>9} {'No BIN':>7} {'% Up':>6}")
    lines.append(f"  {'-'*8} {'-'*10} {'-'*9} {'-'*9} {'-'*7} {'-'*6}")
    for _, row in partner_grp.iterrows():
        lines.append(f"  {str(row['partner']):<8} {int(row['n_specimens']):>10} "
                    f"{int(row['n_uploaded']):>9} {int(row['n_with_bin']):>9} "
                    f"{int(row['n_no_bin']):>7} {float(row['pct_uploaded']):>6.1f}")

    h("MISSING BIN URI — BY UPLOAD DATE")
    no_bin_df = df[df['bold_uploaded'] & ~df['has_bin']]
    date_grp = no_bin_df.groupby('upload_date').size().reset_index(name='n')
    date_grp = date_grp.sort_values('upload_date')
    lines.append(f"  {'Upload Date':<15} {'N Specimens':>12}")
    lines.append(f"  {'-'*15} {'-'*12}")
    for _, row in date_grp.iterrows():
        lines.append(f"  {str(row['upload_date']):<15} {int(row['n']):>12}")

    h("MISSING BIN URI — BY PARTNER")
    no_bin_partner = no_bin_df.groupby('partner').size().reset_index(name='n_no_bin')
    no_bin_partner = no_bin_partner.sort_values('n_no_bin', ascending=False)
    lines.append(f"  {'Partner':<10} {'No BIN':>8}")
    lines.append(f"  {'-'*10} {'-'*8}")
    for _, row in no_bin_partner.iterrows():
        lines.append(f"  {str(row['partner']):<10} {int(row['n_no_bin']):>8}")

    h("TOP 30 PLATES WITH MOST MISSING BINs")
    top = plate_df[plate_df['n_no_bin'] > 0].nlargest(30, 'n_no_bin')
    lines.append(f"  {'Plate ID':<25} {'Partner':<8} {'Uploaded':>9} "
                f"{'No BIN':>7} {'Earliest Upload':>16}")
    lines.append(f"  {'-'*25} {'-'*8} {'-'*9} {'-'*7} {'-'*16}")
    for _, row in top.iterrows():
        lines.append(f"  {str(row['plate_id']):<25} "
                    f"{str(row.get('partner','?')):<8} "
                    f"{int(row['n_bold_uploaded']):>9} "
                    f"{int(row['n_no_bin']):>7} "
                    f"{str(row['earliest_upload'] or '?'):>16}")

    report = '\n'.join(lines)
    print(report)
    with open(output_path, 'w') as f:
        f.write(report)
    print(f"\nReport written to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='BOLD upload summary from portal dump — no API needed'
    )
    parser.add_argument('--input', default=config.PORTAL_DUMP_TSV,
        help='Path to portal dump TSV')
    parser.add_argument('--partner', default='ALL')
    parser.add_argument('--exclude-bge', action='store_true',
        help='Exclude BGE partner plates (BGEP, BGEG, BGKU, BGPT) from output')
    args = parser.parse_args()

    today = datetime.datetime.now().strftime('%Y%m%d')
    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    df       = load_portal_dump(args.input, partner=args.partner,
                                exclude_bge=args.exclude_bge)
    plate_df = build_plate_summary(df)

    # Save outputs
    report_path  = os.path.join(config.RESULTS_DIR,
                                f'bold_summary_report_{today}.txt')
    no_bin_path  = os.path.join(config.RESULTS_DIR,
                                f'bold_missing_bin_{today}.csv')
    plate_path   = os.path.join(config.RESULTS_DIR,
                                f'bold_plate_summary_{today}.csv')

    no_bin_df = df[df['bold_uploaded'] & ~df['has_bin']][[
        _SPECIMEN_COL, 'plate_id', 'partner', 'upload_date', 'bin_date',
        _BIN_URI_COL, 'submit_date'
    ]].copy()

    no_bin_df.to_csv(no_bin_path, index=False)
    plate_df.to_csv(plate_path, index=False)

    generate_report(df, plate_df, args.partner, report_path)

    print(f"\nAll outputs written to {config.RESULTS_DIR}:")
    print(f"  {report_path}")
    print(f"  {no_bin_path}")
    print(f"  {plate_path}")


if __name__ == '__main__':
    main()
