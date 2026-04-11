"""
portal_query.py

Query the ToL portal for BIOSCAN sample data.
Adapted from bioscan_sciops.py, extended to:
  - query by partner (via sts_gal_abbreviation) or all BIOSCAN plates
  - extract bold_nuc field for BOLD upload status
  - batch queries to avoid 414 errors
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


def _query_portal_batch(prtl, plates, verbose=False):
    """Query portal for a batch of plate IDs. Returns list of sample dicts."""
    f = DataSourceFilter()
    f.and_ = {'sts_rackid': {'in_list': {'value': plates}}}
    samples = prtl.get_list('sample', object_filters=f)

    records = []
    for sample in samples:
        uid = sample.id
        rec = {
            'uid':           uid,
            'plate_id':      sample.sts_rackid,
            'well_id':       sample.sts_tubeid,
            'specimen_id':   sample.sts_specimen.id if hasattr(sample, 'sts_specimen') else None,
            'partner':       sample.sts_gal_abbreviation,
        }

        # BOLD upload status
        bold_nuc = getattr(sample, config.PORTAL_BOLD_FIELD, None)
        rec['bold_nuc'] = bold_nuc if bold_nuc else None
        rec['bold_uploaded'] = bool(bold_nuc)

        # Taxonomy (minimal — just for control identification)
        if hasattr(sample.sts_species, 'id'):
            rec['taxon_id'] = sample.sts_species.id
        else:
            rec['taxon_id'] = None

        if verbose:
            print(f"  {uid} | {rec['plate_id']} | {rec['well_id']} | partner={rec['partner']}")

        records.append(rec)

    return records


def query_plates_from_portal(plates, verbose=False):
    """
    Query portal for a list of plate IDs.
    Batches requests (PORTAL_BATCH_SIZE plates per call) with rate limiting.

    Returns:
        df: DataFrame with columns [uid, plate_id, well_id, specimen_id, partner,
                                    bold_nuc, bold_uploaded, taxon_id]
        missing_plates: set of plate IDs not found
    """
    if not TOL_AVAILABLE:
        print("ERROR: tol library not available")
        return pd.DataFrame(), set(plates)

    prtl = portal()
    all_records = []
    plates = list(plates)
    batch_size = config.PORTAL_BATCH_SIZE

    for i in range(0, len(plates), batch_size):
        batch = plates[i:i + batch_size]
        if verbose or True:  # always show progress for portal queries
            print(f"  Querying portal: plates {i+1}–{min(i+batch_size, len(plates))} of {len(plates)}")
        try:
            records = _query_portal_batch(prtl, batch, verbose=verbose)
            all_records.extend(records)
        except Exception as e:
            print(f"  ERROR querying batch {i}–{i+batch_size}: {e}")
        if i + batch_size < len(plates):
            time.sleep(config.PORTAL_RATE_LIMIT_SLEEP)

    if not all_records:
        df = pd.DataFrame(columns=['uid','plate_id','well_id','specimen_id',
                                   'partner','bold_nuc','bold_uploaded','taxon_id'])
        return df, set(plates)

    df = pd.DataFrame(all_records)
    found_plates = set(df['plate_id'].unique())
    missing_plates = set(plates) - found_plates

    return df, missing_plates


def get_portal_plate_summary(plates, verbose=False):
    """
    For a list of plate IDs, return a per-plate portal summary:

        { plate_id: {
            'portal_found':   True/False,
            'partner':        '4-letter code or None',
            'n_wells':        int,
            'bold_uploaded':  True/False  (True if ANY well has bold_nuc)
          }
        }
    """
    df, missing = query_plates_from_portal(plates, verbose=verbose)

    summary = {}

    if not df.empty:
        for plate_id, grp in df.groupby('plate_id'):
            summary[plate_id] = {
                'portal_found':  True,
                'partner':       grp['partner'].dropna().iloc[0] if not grp['partner'].dropna().empty else None,
                'n_wells':       len(grp),
                'bold_uploaded': grp['bold_uploaded'].any(),
            }

    for plate_id in missing:
        summary[plate_id] = {
            'portal_found':  False,
            'partner':       None,
            'n_wells':       0,
            'bold_uploaded': False,
        }

    return summary, df


def get_all_plates_for_partner(partner, verbose=False):
    """
    Query portal for ALL plates associated with a partner code
    (via sts_gal_abbreviation field). Returns DataFrame of all samples.

    This is the recommended way to get the full plate list from the portal
    without needing a pre-existing plate list.
    """
    if not TOL_AVAILABLE:
        print("ERROR: tol library not available")
        return pd.DataFrame()

    prtl = portal()
    f = DataSourceFilter()

    if partner.upper() == 'ALL':
        # Query all BIOSCAN samples — use cohort filter if available,
        # otherwise pull all and filter client-side
        # NOTE: for very large datasets this may be slow; consider partner-by-partner
        print("WARNING: querying ALL BIOSCAN samples from portal — this may be slow")
        f.and_ = {}  # no filter — relies on project scoping in portal config
    else:
        f.and_ = {'sts_gal_abbreviation': {'eq': {'value': partner}}}

    print(f"Querying portal for partner='{partner}'...")
    samples = prtl.get_list('sample', object_filters=f)

    records = []
    for sample in samples:
        bold_nuc = getattr(sample, config.PORTAL_BOLD_FIELD, None)
        records.append({
            'uid':          sample.id,
            'plate_id':     sample.sts_rackid,
            'well_id':      sample.sts_tubeid,
            'specimen_id':  sample.sts_specimen.id if hasattr(sample, 'sts_specimen') else None,
            'partner':      sample.sts_gal_abbreviation,
            'bold_nuc':     bold_nuc if bold_nuc else None,
            'bold_uploaded': bool(bold_nuc),
        })
        if verbose:
            print(f"  {sample.id} | {sample.sts_rackid} | {sample.sts_tubeid}")

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records)
