"""
portal_query.py

Query the ToL portal for BIOSCAN plate data.

For each plate (sts_rackid):
  - sts_submit_date   : when the plate was submitted
  - sts_gal_abbreviation : partner code (disambiguates MOZZ plates)
  - bold_nuc          : non-None means sequence uploaded to BOLD

Queries in batches of PORTAL_BATCH_SIZE plates with rate limiting.
"""

import time
import pandas as pd
from collections import defaultdict

import config

try:
    from tol.sources.portal import portal
    from tol.core import DataSourceFilter
    TOL_AVAILABLE = True
except ImportError:
    TOL_AVAILABLE = False
    print("WARNING: tol library not available — portal queries will be skipped")


def query_portal_for_plates(plate_list, verbose=False):
    """
    Query portal for a list of plate IDs (sts_rackid).
    Returns a DataFrame with one row per PLATE (not per well):
        plate_id | partner | submit_date | bold_uploaded | n_wells
    
    bold_uploaded = True if ANY well in the plate has bold_nuc set.
    submit_date   = earliest sts_submit_date across wells in the plate.
    """
    if not TOL_AVAILABLE:
        print("ERROR: tol library not available")
        return pd.DataFrame()

    prtl = portal()
    plate_list = list(plate_list)
    batch_size = config.PORTAL_BATCH_SIZE
    all_records = []

    for i in range(0, len(plate_list), batch_size):
        batch = plate_list[i:i + batch_size]
        print(f"  Querying portal: plates {i+1}–{min(i+batch_size, len(plate_list))} "
              f"of {len(plate_list)}")
        try:
            f = DataSourceFilter()
            f.and_ = {'sts_rackid': {'in_list': {'value': batch}}}
            samples = prtl.get_list('sample', object_filters=f)

            for sample in samples:
                bold_nuc = getattr(sample, 'bold_nuc', None)
                submit_date = getattr(sample, 'sts_submit_date', None)
                all_records.append({
                    'plate_id':     sample.sts_rackid,
                    'partner':      getattr(sample, 'sts_gal_abbreviation', None),
                    'submit_date':  str(submit_date)[:10] if submit_date else None,
                    'bold_nuc':     bold_nuc,
                    'bold_uploaded': bool(bold_nuc),
                })
        except Exception as e:
            print(f"  ERROR querying batch {i}–{i+batch_size}: {e}")

        if i + batch_size < len(plate_list):
            time.sleep(config.PORTAL_RATE_LIMIT_SLEEP)

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)

    # Aggregate to plate level
    plate_summary = []
    for plate_id, grp in df.groupby('plate_id'):
        # Partner: take first non-null
        partner = grp['partner'].dropna().iloc[0] if grp['partner'].dropna().any() else None
        # Submit date: earliest
        dates = grp['submit_date'].dropna()
        submit_date = sorted(dates)[0] if len(dates) > 0 else None
        # BOLD: True if any well has bold_nuc
        bold_uploaded = grp['bold_uploaded'].any()

        plate_summary.append({
            'plate_id':     plate_id,
            'partner':      partner,
            'submit_date':  submit_date,
            'bold_uploaded': bold_uploaded,
            'n_wells_portal': len(grp),
        })

    return pd.DataFrame(plate_summary)


def get_all_bioscan_plates_from_portal(verbose=False):
    """
    Query portal for ALL plates ever submitted to BIOSCAN.
    Uses the cohort filter if available, otherwise pulls all samples.
    
    Returns plate-level DataFrame:
        plate_id | partner | submit_date | bold_uploaded | n_wells_portal
    """
    if not TOL_AVAILABLE:
        print("ERROR: tol library not available")
        return pd.DataFrame()

    print("Querying portal for all BIOSCAN plates...")
    prtl = portal()

    # Query all samples — the portal is scoped to BIOSCAN project
    # This may be slow for large datasets
    f = DataSourceFilter()
    samples = prtl.get_list('sample', object_filters=f)

    all_records = []
    for sample in samples:
        bold_nuc    = getattr(sample, 'bold_nuc', None)
        submit_date = getattr(sample, 'sts_submit_date', None)
        all_records.append({
            'plate_id':      sample.sts_rackid,
            'partner':       getattr(sample, 'sts_gal_abbreviation', None),
            'submit_date':   str(submit_date)[:10] if submit_date else None,
            'bold_nuc':      bold_nuc,
            'bold_uploaded': bool(bold_nuc),
        })
        if verbose and len(all_records) % 1000 == 0:
            print(f"  ...{len(all_records)} samples retrieved")

    if not all_records:
        return pd.DataFrame()

    print(f"  Retrieved {len(all_records)} samples from portal")
    df = pd.DataFrame(all_records)

    # Aggregate to plate level
    plate_summary = []
    for plate_id, grp in df.groupby('plate_id'):
        partner     = grp['partner'].dropna().iloc[0] if grp['partner'].dropna().any() else None
        dates       = grp['submit_date'].dropna()
        submit_date = sorted(dates)[0] if len(dates) > 0 else None
        bold_uploaded = grp['bold_uploaded'].any()

        plate_summary.append({
            'plate_id':       plate_id,
            'partner':        partner,
            'submit_date':    submit_date,
            'bold_uploaded':  bold_uploaded,
            'n_wells_portal': len(grp),
        })

    result = pd.DataFrame(plate_summary)
    print(f"  {len(result)} unique plates found on portal")
    return result
