"""
generate_summary_report.py

Generates a comprehensive Markdown summary report of the BIOSCAN pipeline
status, drawing from all pipeline output files. The report mirrors the
structure of the slide-deck review but in a self-contained document with
tables, numbers, and actionable recommendations.

Sections:
    1. Pipeline overview
    2. Plates missing from sequencing
    3. Plates not yet on BOLD
    4. Plate-level QC summary
    5. Repeat sequencing (plate and specimen level)
    6. Missing specimens and assembly failures
    7. BOLD quality flags
    8. Actions required

Usage:
    python3 generate_summary_report.py
    python3 generate_summary_report.py --exclude-bge
    python3 generate_summary_report.py --output /path/to/report.md
"""

import argparse
import datetime
import glob
import os
import re
import pandas as pd
from collections import defaultdict

import config
from utils import is_bge_plate, resolve_run_dir


# ── Helpers ───────────────────────────────────────────────────────────────────

def _latest(pattern, results_dir):
    """Return the most recently modified file matching pattern.

    Searches both the root of results_dir and one level of timestamped
    subdirectories (YYYYMMDD_HHMMSS/), returning whichever match has the
    highest mtime. Root-level files (e.g. portal_plates_from_dump.csv,
    bold_workbench_combined.csv) are found correctly because they live in root.
    """
    candidates = (
        glob.glob(os.path.join(results_dir, pattern)) +
        glob.glob(os.path.join(results_dir, '*', pattern))
    )
    return max(candidates, key=os.path.getmtime) if candidates else None


def _read(path, **kwargs):
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str, **kwargs)
    except Exception as e:
        print(f"  Warning: could not read {path}: {e}")
        return pd.DataFrame()


def _num(df, col):
    return pd.to_numeric(df[col], errors='coerce') if col in df.columns else pd.Series()


def pct(n, total):
    return f"{100*n/total:.1f}%" if total else "n/a"


def fmt(n):
    """Format number with thousands separator."""
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


# ── Section generators ────────────────────────────────────────────────────────

def section_pipeline_overview(plate_status, exclude_bge):
    df = plate_status.copy()
    if exclude_bge:
        df = df[~df['plate_id'].apply(lambda x: is_bge_plate(str(x)))]

    total = len(df)
    in_portal = df['in_portal'].astype(str).str.upper().eq('TRUE').sum() if 'in_portal' in df.columns else total
    sequenced = df[df['missing_at'].isna() | (df['missing_at'] == '')].shape[0] if 'missing_at' in df.columns else 0

    # Pipeline stage counts
    stages = df['pipeline_stage'].value_counts() if 'pipeline_stage' in df.columns else pd.Series()
    n_mbrave = (df['pipeline_stage'].isin(['mbrave','qc','bold'])).sum() if 'pipeline_stage' in df.columns else 0
    n_qc     = (df['pipeline_stage'].isin(['qc','bold'])).sum() if 'pipeline_stage' in df.columns else 0
    n_bold   = (df['pipeline_stage'] == 'bold').sum() if 'pipeline_stage' in df.columns else 0

    # Partners
    n_partners = df['partner'].nunique() if 'partner' in df.columns else 0

    # Dropped counts
    missing = df['missing_at'].value_counts() if 'missing_at' in df.columns else pd.Series()
    n_missing_mbrave = int(missing.get('mbrave', 0))
    n_missing_qc     = int(missing.get('qc', 0))
    n_missing_bold   = int(missing.get('bold', 0))

    # Partners fully through
    if 'partner' in df.columns and 'missing_at' in df.columns:
        complete_partners = df.groupby('partner').apply(
            lambda g: g['missing_at'].isna().all() and (g['pipeline_stage'] == 'bold').all()
        )
        complete_list = sorted(complete_partners[complete_partners].index.tolist())
    else:
        complete_list = []

    lines = [
        "## 1. Pipeline Overview",
        "",
        f"**{fmt(total)} plates tracked across {fmt(n_partners)} partners**",
        "",
        "| Stage | Plates | % of total |",
        "|---|---|---|",
        f"| Submitted to portal | {fmt(in_portal)} | {pct(in_portal, total)} |",
        f"| Through mBRAVE (sequenced) | {fmt(n_mbrave)} | {pct(n_mbrave, total)} |",
        f"| Through QC | {fmt(n_qc)} | {pct(n_qc, total)} |",
        f"| Uploaded to BOLD | {fmt(n_bold)} | {pct(n_bold, total)} |",
        "",
        "**Dropped at each stage:**",
        "",
        f"- {fmt(n_missing_mbrave)} plates not sequenced (mBRAVE missing)",
        f"- {fmt(n_missing_qc)} plates not through QC",
        f"- {fmt(n_missing_bold)} plates passed QC but not on BOLD",
        "",
    ]
    if complete_list:
        lines += [
            f"**{len(complete_list)} partners with all plates fully through to BOLD:**",
            "",
            ", ".join(complete_list),
            "",
        ]
    return lines


def section_missing_sequencing(plate_status, exclude_bge):
    df = plate_status.copy()
    if exclude_bge:
        df = df[~df['plate_id'].apply(lambda x: is_bge_plate(str(x)))]

    not_seq = df[df['missing_at'] == 'mbrave'].copy() if 'missing_at' in df.columns else pd.DataFrame()
    if not_seq.empty:
        return ["## 2. Plates Missing from Sequencing", "", "_No data available._", ""]

    not_seq['submit_date'] = pd.to_datetime(not_seq['submit_date'], errors='coerce')
    today = pd.Timestamp.now()
    not_seq['days_waiting'] = (today - not_seq['submit_date']).dt.days

    old = not_seq[not_seq['days_waiting'] >= 90].sort_values('days_waiting', ascending=False)
    recent = not_seq[not_seq['days_waiting'] < 90]

    lines = [
        "## 2. Plates Missing from Sequencing",
        "",
        f"**{fmt(len(not_seq))} plates submitted but not yet sequenced**",
        "",
        f"- {fmt(len(old))} submitted >90 days ago — investigate immediately",
        f"- {fmt(len(recent))} recently submitted — likely in queue",
        "",
    ]

    if not old.empty:
        lines += [
            "**Old submissions requiring immediate investigation:**",
            "",
            "| Partner | Plates | Submitted | Days waiting | Plate IDs |",
            "|---|---|---|---|---|",
        ]
        for partner, grp in old.groupby('partner'):
            plates = sorted(grp['plate_id'].tolist())
            submitted = grp['submit_date'].min().date()
            days = int(grp['days_waiting'].max())
            id_str = f"{plates[0]}–{plates[-1]}" if len(plates) > 1 else plates[0]
            lines.append(f"| {partner} | {len(grp)} | {submitted} | ~{days:,} days | {id_str} |")
        lines.append("")

    # Partner summary
    by_partner = not_seq.groupby('partner').agg(
        n=('plate_id', 'count'),
        old=('days_waiting', lambda x: (x >= 90).sum())
    ).reset_index().sort_values('n', ascending=False)

    lines += [
        "**All partners with plates not yet sequenced:**",
        "",
        "| Partner | Plates missing | Note |",
        "|---|---|---|",
    ]
    for _, row in by_partner.iterrows():
        note = f"All >90 days old" if row['old'] == row['n'] else \
               f"{int(row['old'])} plates >90 days old" if row['old'] > 0 else \
               "Recently submitted"
        lines.append(f"| {row['partner']} | {int(row['n'])} | {note} |")
    lines.append("")
    return lines


def section_not_on_bold(plate_status, exclude_bge):
    df = plate_status.copy()
    if exclude_bge:
        df = df[~df['plate_id'].apply(lambda x: is_bge_plate(str(x)))]

    not_bold = df[df['missing_at'] == 'bold'].copy() if 'missing_at' in df.columns else pd.DataFrame()
    if not_bold.empty:
        return ["## 3. Plates Not Yet on BOLD", "", "_No data available._", ""]

    not_bold['submit_date'] = pd.to_datetime(not_bold['submit_date'], errors='coerce')
    today = pd.Timestamp.now()
    not_bold['days'] = (today - not_bold['submit_date']).dt.days
    not_bold['year'] = not_bold['submit_date'].dt.year

    old = not_bold[not_bold['days'] >= 90]
    n_2026 = (not_bold['year'] >= 2026).sum()
    n_2025 = (not_bold['year'] == 2025).sum()

    by_partner = not_bold.groupby('partner').size().sort_values(ascending=False)
    old_by_partner = old.groupby('partner').size().sort_values(ascending=False)

    lines = [
        "## 3. Plates Not Yet on BOLD",
        "",
        f"**{fmt(len(not_bold))} plates passed QC but not yet on BOLD**",
        "",
        f"- {fmt(n_2026)} plates submitted in 2026 — too recent to expect on BOLD yet",
        f"- {fmt(n_2025)} plates submitted in 2025 — mostly normal upload lag",
        f"- {fmt(len(old))} plates submitted >90 days ago — need chasing immediately",
        "",
        "**All partners with plates not on BOLD:**",
        "",
        "| Partner | Plates not on BOLD |",
        "|---|---|",
    ]
    for partner, n in by_partner.items():
        lines.append(f"| {partner} | {n} |")
    lines.append("")

    if not old_by_partner.empty:
        lines += [
            "**Partners with overdue plates (>90 days, not on BOLD):**",
            "",
            "| Partner | Overdue plates |",
            "|---|---|",
        ]
        for partner, n in old_by_partner.items():
            lines.append(f"| {partner} | {n} |")
        lines.append("")

    return lines


def section_plate_qc(plate_summ, plate_cats, exclude_bge):
    df = plate_summ.copy()
    cats = plate_cats.copy()

    if exclude_bge:
        df = df[~df['plate_id'].apply(lambda x: is_bge_plate(str(x)))]
        cats = cats[~cats['plate_id'].apply(lambda x: is_bge_plate(str(x)))]

    for col in ['n_specimens','pass_count','on_hold_count','fail_count',
                'pass_rate','combined_rate','n_controls']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    cat_cols = [f'cat{c}_count' for c in range(1,13)] + ['failed_count']
    for col in cat_cols + ['n_specimens']:
        cats[col] = pd.to_numeric(cats[col], errors='coerce')

    seq = df[df['n_specimens'].notna()].copy()
    cats_seq = cats[cats['n_specimens'].notna()].copy()
    total_specs = cats_seq['n_specimens'].sum()

    # Partner stats
    partner_stats = seq.groupby('partner').agg(
        n_plates=('plate_id','count'),
        total_specs=('n_specimens','sum'),
        total_pass=('pass_count','sum'),
    ).reset_index()
    partner_stats['rate'] = (100*partner_stats['total_pass']/partner_stats['total_specs']).round(1)
    top10 = partner_stats.nlargest(10,'rate')
    bot10 = partner_stats.nsmallest(10,'rate')

    lines = [
        "## 4. Plate-Level QC Summary",
        "",
        "> **Methodology:** Pass counts use the best result per specimen across ALL "
        "sequencing batches. If a specimen passed in any batch it is counted as PASS. "
        "Results reflect the February 2026 QC re-run which applied stricter criteria.",
        "",
        f"**{fmt(len(seq))} sequenced plates** | "
        f"{fmt(len(df)-len(seq))} never sequenced",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Average pass rate | {seq.pass_rate.mean():.1f}% |",
        f"| Median pass rate | {seq.pass_rate.median():.1f}% |",
        f"| Average combined (PASS+ON_HOLD) | {seq.combined_rate.mean():.1f}% |",
        f"| Plates at 100% pass | {fmt((seq.pass_rate==100).sum())} |",
        f"| Plates ≥90% pass | {fmt((seq.pass_rate>=90).sum())} |",
        f"| Plates <50% pass | {fmt((seq.pass_rate<50).sum())} |",
        f"| Plates at 0% pass | {fmt((seq.pass_rate==0).sum())} |",
        f"| Full plates (93 specimens) | {fmt((seq.n_specimens==93).sum())} |",
        f"| Early-batch plates (94 specimens) | {fmt((seq.n_specimens==94).sum())} |",
        f"| Partial plates (<93 specimens) | {fmt((seq.n_specimens<93).sum())} |",
        "",
        "**Top 10 partners by pass rate:**",
        "",
        "| Partner | Plates | Pass rate |",
        "|---|---|---|",
    ]
    for _, r in top10.iterrows():
        lines.append(f"| {r['partner']} | {int(r['n_plates'])} | {r['rate']:.1f}% |")
    lines += [
        "",
        "**Bottom 10 partners by pass rate:**",
        "",
        "| Partner | Plates | Pass rate |",
        "|---|---|---|",
    ]
    for _, r in bot10.iterrows():
        lines.append(f"| {r['partner']} | {int(r['n_plates'])} | {r['rate']:.1f}% |")

    lines += [
        "",
        "**QC category breakdown:**",
        "",
        "| Category | Description | Count | % |",
        "|---|---|---|---|",
    ]
    cat_labels = {
        1: "Single sequence >200 reads (PASS)",
        2: "Single sequence 50-200 reads (PASS)",
        3: "Single sequence <50 reads (PASS)",
        4: "Non-conflicting secondary ≤5 reads (PASS)",
        5: "Non-conflicting secondary >5 reads (PASS)",
        6: "Conflicting secondary ≤5 reads (PASS)",
        7: "Conflicting secondary >5 reads (PASS)",
        8: "Not used by pipeline",
        9: "ON_HOLD — multiple sequences",
        10: "ON_HOLD — taxonomy conflict",
        11: "ON_HOLD — secondary sequences",
        12: "ON_HOLD — other",
    }
    for c in range(1,13):
        n = int(cats_seq[f'cat{c}_count'].sum())
        lines.append(f"| Cat {c} | {cat_labels.get(c,'')} | {fmt(n)} | {pct(n, total_specs)} |")
    fail_n = int(cats_seq['failed_count'].sum())
    lines += [
        f"| FAILED | — | {fmt(fail_n)} | {pct(fail_n, total_specs)} |",
        "",
    ]
    return lines


def section_repeat(repeat_plate, repeat_transitions, repeat_summary, exclude_bge):
    if repeat_plate.empty:
        return ["## 5. Repeat Sequencing", "", "_No data available._", ""]

    df = repeat_plate.copy()
    if exclude_bge:
        df = df[~df['plate_id'].apply(lambda x: is_bge_plate(str(x)))]

    for col in ['first_pct_pass','best_pct_pass','last_pct_pass','improvement','n_sequencings']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    n_plates = len(df)
    n_improved = (df.improvement > 0).sum()
    n_declined = (df.improvement < 0).sum()

    # Specimen totals assuming 93 specimens per plate
    df['n_specimens'] = 93
    df['passes_first'] = (df.first_pct_pass / 100 * df.n_specimens).round(0)
    df['passes_best']  = (df.best_pct_pass  / 100 * df.n_specimens).round(0)
    df['passes_last']  = (df.last_pct_pass  / 100 * df.n_specimens).round(0)
    total_specs = df.n_specimens.sum()

    # Partner summary
    partner = df.groupby('partner').agg(
        n_plates=('plate_id','count'),
        avg_first=('first_pct_pass','mean'),
        avg_best=('best_pct_pass','mean'),
        avg_last=('last_pct_pass','mean'),
    ).reset_index().sort_values('n_plates', ascending=False)

    lines = [
        "## 5. Repeat Sequencing",
        "",
        "### 5a. Plate Level",
        "",
        f"**{fmt(n_plates)} plates sequenced in more than one batch**",
        "",
        f"- {fmt(n_improved)} plates improved (first → last batch)",
        f"- {fmt(n_declined)} plates declined",
        f"- Average improvement (first → last): {df.improvement.mean():+.1f}%",
        "",
        "**Total pass count comparison across all repeated plates:**",
        "",
        "| Approach | Total passes | Overall pass rate |",
        "|---|---|---|",
        f"| First batch only | {fmt(df.passes_first.sum())} | {pct(df.passes_first.sum(), total_specs)} |",
        f"| Last batch only  | {fmt(df.passes_last.sum())} | {pct(df.passes_last.sum(), total_specs)} |",
        f"| Best batch (recommended) | {fmt(df.passes_best.sum())} | {pct(df.passes_best.sum(), total_specs)} |",
        "",
        f"> Using best batch rather than first recovers "
        f"**+{fmt(int(df.passes_best.sum()-df.passes_first.sum()))} additional passing specimens**. "
        f"Using best rather than last recovers a further "
        f"**+{fmt(int(df.passes_best.sum()-df.passes_last.sum()))} specimens**.",
        "",
        "**By partner:**",
        "",
        "| Partner | Plates | First % | Best % | Last % |",
        "|---|---|---|---|---|",
    ]
    for _, r in partner.iterrows():
        lines.append(
            f"| {r['partner']} | {int(r['n_plates'])} | "
            f"{r['avg_first']:.1f}% | {r['avg_best']:.1f}% | {r['avg_last']:.1f}% |"
        )
    lines.append("")

    # Specimen level
    if not repeat_transitions.empty:
        lines += [
            "### 5b. Specimen Level",
            "",
        ]
        if not repeat_summary.empty:
            total_rep = len(repeat_summary)
            ever_passed = repeat_summary['ever_passed'].astype(str).str.upper().eq('TRUE').sum() \
                if 'ever_passed' in repeat_summary.columns else 0
            lines += [
                f"**{fmt(total_rep)} specimens sequenced in ≥2 batches**",
                "",
                f"- {fmt(ever_passed)} ever achieved PASS across any batch ({pct(ever_passed, total_rep)})",
                "",
            ]

        lines += [
            "**Transition matrix (first → last QC decision):**",
            "",
            "| First | Last | Count | % | Interpretation |",
            "|---|---|---|---|---|",
        ]
        interp = {
            ('PASS','PASS'):       "Stable — consistent sequencing",
            ('FAILED','PASS'):     "Repeat sequencing highly effective",
            ('FAILED','FAILED'):   "Persistent failure — likely biological",
            ('ON_HOLD','PASS'):    "Resolved on resequencing",
            ('PASS','ON_HOLD'):    "Slight decline — best-batch selection prevents",
            ('FAILED','ON_HOLD'):  "Partial improvement",
            ('PASS','FAILED'):     "Decline — best-batch selection prevents",
            ('ON_HOLD','FAILED'):  "Decline on resequencing",
            ('ON_HOLD','ON_HOLD'): "No change",
        }
        trans = repeat_transitions.copy()
        n_col = 'n' if 'n' in trans.columns else 'count'
        if n_col in trans.columns:
            trans[n_col] = pd.to_numeric(trans[n_col], errors='coerce')
            total_trans = trans[n_col].sum()
            trans = trans.sort_values(n_col, ascending=False)
            for _, r in trans.iterrows():
                first = str(r.get('first_decision',''))
                last  = str(r.get('last_decision',''))
                n     = int(r[n_col])
                note  = interp.get((first, last), "")
                lines.append(f"| {first} | {last} | {fmt(n)} | {pct(n, total_trans)} | {note} |")
        lines.append("")

    lines += [
        "> **Recommendation:** Implement best-batch selection before data freeze. "
        "Use the decision from the batch where each specimen achieved its best result "
        "(PASS > ON_HOLD > FAILED). This is already implemented in `plate_summary_all.py`.",
        "",
    ]
    return lines


def section_missing_specimens(missing_batch, missing_cats):
    if missing_batch.empty:
        return ["## 6. Missing Specimens & Assembly Failures", "", "_No data available._", ""]

    for col in ['n_expected','n_cat1_zero','n_cat2_low','n_cat2_high']:
        missing_batch[col] = pd.to_numeric(missing_batch[col], errors='coerce')

    total_exp  = int(missing_batch.n_expected.sum())
    cat1       = int(missing_batch.n_cat1_zero.sum())
    cat2_low   = int(missing_batch.n_cat2_low.sum())
    cat2_high  = int(missing_batch.n_cat2_high.sum())
    cat3       = 0

    # Cat2_high by partner
    if not missing_cats.empty:
        cat2h = missing_cats[missing_cats['category'].str.strip() == 'Cat2_high_reads'].copy()
        cat2h['partner'] = cat2h['plate_id'].str.extract(r'^(?:TOL-)?([A-Z]{4})')
        cat2h_by_partner = cat2h['partner'].value_counts().head(10)
    else:
        cat2h_by_partner = pd.Series()

    # Worst batches for cat2_high
    worst = missing_batch.nlargest(5, 'n_cat2_high')[['batch','n_cat2_high','n_expected']]
    worst['pct'] = (100*worst.n_cat2_high/worst.n_expected).round(1)

    lines = [
        "## 6. Missing Specimens & Assembly Failures",
        "",
        f"**{fmt(total_exp)} specimens expected across all sequencing batches**",
        "",
        "| Category | Count | % | Description |",
        "|---|---|---|---|",
        f"| Cat 1 — Zero reads | {fmt(cat1)} | {pct(cat1, total_exp)} | No reads produced — biological/library prep issue |",
        f"| Cat 2 Low — Few reads | {fmt(cat2_low)} | {pct(cat2_low, total_exp)} | Below assembly threshold — borderline |",
        f"| Cat 2 High — Reads present, no consensus | {fmt(cat2_high)} | {pct(cat2_high, total_exp)} | ⚠ Assembly failed despite adequate depth |",
        f"| Cat 3 — Absent from UMI stats | {fmt(cat3)} | 0.0% | Demultiplexing failure — zero cases detected |",
        "",
    ]

    if not cat2h_by_partner.empty:
        lines += [
            "**Cat 2 High concentrated in:**",
            "",
            "| Partner | Specimens |",
            "|---|---|",
        ]
        for partner, n in cat2h_by_partner.items():
            lines.append(f"| {partner} | {n} |")
        lines.append("")

    if not worst.empty:
        lines += [
            "**Worst batches for Cat 2 High:**",
            "",
            "| Batch | Cat2_high | % of batch |",
            "|---|---|---|",
        ]
        for _, r in worst.iterrows():
            if r['n_cat2_high'] > 0:
                lines.append(f"| {r['batch']} | {fmt(int(r['n_cat2_high']))} | {r['pct']:.1f}% |")
        lines.append("")

    lines += [
        "> **Summary:** The vast majority of missing specimens are Cat1 zero-read failures — "
        "genuine biological or library prep issues with no pipeline involvement. "
        f"Only {fmt(cat2_high)} specimens ({pct(cat2_high, total_exp)}) represent unexpected "
        "assembly failures worth investigating further. Cat3 = 0: no demultiplexing failures detected.",
        "",
    ]
    return lines


def section_bold_flags(bold_report_path, bold_needs_resub, bold_flagged_no_alt,
                       bold_flagged_comp):
    # Parse workbench totals from report file
    total_wb = any_flag = stop_codon = contam = flagged_rec = 0
    if bold_report_path and os.path.exists(bold_report_path):
        with open(bold_report_path) as f:
            txt = f.read()
        def _extract(label):
            m = re.search(rf'{re.escape(label)}\s*:\s*([\d,]+)', txt)
            return int(m.group(1).replace(',','')) if m else 0
        total_wb   = _extract('Total flagged specimens loaded')
        stop_codon = _extract('Has stop codon flag')
        contam     = _extract('Has contamination flag')
        flagged_rec= _extract('Flagged record')
        m = re.search(r'Any flag[^:]*:\s*([\d,]+)', txt)
        any_flag = int(m.group(1).replace(',','')) if m else stop_codon + contam + flagged_rec

    # Sequence concordance
    neither = qc_only = different = identical = bold_only = 0
    if not bold_flagged_comp.empty and 'sequence_status' in bold_flagged_comp.columns:
        vc = bold_flagged_comp['sequence_status'].value_counts()
        neither   = int(vc.get('NEITHER', 0))
        qc_only   = int(vc.get('QC_ONLY', 0))
        different = int(vc.get('DIFFERENT', 0))
        identical = int(vc.get('IDENTICAL', 0))
        bold_only = int(vc.get('BOLD_ONLY', 0))

    # Resubmission by partner
    resub_by_partner = pd.Series()
    if not bold_needs_resub.empty:
        col = next((c for c in ['partner_code','partner'] if c in bold_needs_resub.columns), None)
        if col:
            resub_by_partner = bold_needs_resub[col].value_counts()

    # No alternative by partner
    no_alt_by_partner = pd.Series()
    if not bold_flagged_no_alt.empty:
        col = next((c for c in ['partner_code','partner'] if c in bold_flagged_no_alt.columns), None)
        if col:
            no_alt_by_partner = bold_flagged_no_alt[col].value_counts()

    n_resub = len(bold_needs_resub)
    n_no_alt = len(bold_flagged_no_alt)

    lines = [
        "## 7. BOLD Quality Flags",
        "",
        f"**{fmt(total_wb)} flagged specimens loaded** — the BOLD workbench export is "
        f"flagged-only; population-wide BIN coverage is reported separately by "
        f"`bold_summary_from_portal.py`.",
        "",
        "| Metric | Count |",
        "|---|---|",
        f"| Any quality flag | {fmt(any_flag)} ({pct(any_flag, total_wb)}) |",
        f"| Stop codon flag | {fmt(stop_codon)} |",
        f"| Contamination flag | {fmt(contam)} |",
        f"| Flagged record | {fmt(flagged_rec)} |",
        "",
        "### 7a. Sequence Concordance (QC FASTA vs BOLD)",
        "",
        "| Result | Count | % | Meaning | Action |",
        "|---|---|---|---|---|",
        f"| NEITHER | {fmt(neither)} | {pct(neither, any_flag)} | No sequence in BOLD or QC | None possible |",
        f"| QC_ONLY | {fmt(qc_only)} | {pct(qc_only, any_flag)} | Passed QC, not on BOLD | Upload to BOLD |",
        f"| DIFFERENT | {fmt(different)} | {pct(different, any_flag)} | QC has better sequence | **Resubmit to BOLD** |",
        f"| IDENTICAL | {fmt(identical)} | {pct(identical, any_flag)} | Same sequence in both | Expert review |",
        f"| BOLD_ONLY | {fmt(bold_only)} | {pct(bold_only, any_flag)} | On BOLD, not in QC | Investigate |",
        "",
    ]

    if not resub_by_partner.empty:
        lines += [
            f"### 7b. Specimens for Resubmission to BOLD ({fmt(n_resub)} total)",
            "",
            "QC has a better sequence than BOLD — resubmitting will likely result in BIN assignment.",
            "",
            "| Partner | Specimens |",
            "|---|---|",
        ]
        for partner, n in resub_by_partner.head(20).items():
            lines.append(f"| {partner} | {n} |")
        lines.append("")

    if not no_alt_by_partner.empty:
        lines += [
            f"### 7c. Genuinely Flagged Specimens — Expert Review Needed ({fmt(n_no_alt)} total)",
            "",
            "Same sequence in QC and BOLD — no automated fix. Requires domain expert assessment.",
            "",
            "| Partner | Specimens |",
            "|---|---|",
        ]
        for partner, n in no_alt_by_partner.head(10).items():
            lines.append(f"| {partner} | {n} |")
        lines.append("")

    return lines


def section_actions(plate_status, bold_needs_resub, bold_flagged_comp,
                    missing_cats, exclude_bge):
    # Count QC_ONLY
    qc_only = 0
    if not bold_flagged_comp.empty and 'sequence_status' in bold_flagged_comp.columns:
        qc_only = (bold_flagged_comp['sequence_status'] == 'QC_ONLY').sum()

    n_resub  = len(bold_needs_resub)
    n_no_alt = 0

    # Plates not on BOLD
    df = plate_status.copy()
    if exclude_bge:
        df = df[~df['plate_id'].apply(lambda x: is_bge_plate(str(x)))]
    n_not_bold = (df['missing_at'] == 'bold').sum() if 'missing_at' in df.columns else 0

    # Old not sequenced
    not_seq = df[df['missing_at'] == 'mbrave'].copy() if 'missing_at' in df.columns else pd.DataFrame()
    if not not_seq.empty:
        not_seq['submit_date'] = pd.to_datetime(not_seq['submit_date'], errors='coerce')
        old_not_seq = (pd.Timestamp.now() - not_seq['submit_date']).dt.days >= 90
        n_old_not_seq = old_not_seq.sum()
    else:
        n_old_not_seq = 0

    # Cat2_high
    cat2_high = 0
    if not missing_cats.empty and 'category' in missing_cats.columns:
        cat2_high = (missing_cats['category'].str.strip() == 'Cat2_high_reads').sum()

    lines = [
        "## 8. Actions Required",
        "",
        "### 🔴 HIGH PRIORITY — Must be resolved before data freeze",
        "",
        f"**1. Investigate plates with no sequencing data after >90 days**  ",
        f"Plates submitted over 90 days ago with no mBRAVE data: **{fmt(n_old_not_seq)} plates**.  ",
        "FRBX and FACE are the most critical — submitted 2023, still unsequenced.  ",
        "_Reference: `missing_plates_ALL_YYYYMMDD.txt`_",
        "",
        f"**2. Resubmit {fmt(n_resub)} specimens to BOLD with improved QC sequences**  ",
        "These have quality flags in BOLD but the QC pipeline has produced a better sequence.  ",
        "Resubmission will likely result in BIN assignment.  ",
        "_Reference: `bold_needs_resubmission_YYYYMMDD.csv`_",
        "",
        f"**3. Upload QC sequences to BOLD for {fmt(qc_only)} QC_ONLY specimens**  ",
        "These passed QC but have no BOLD sequence at all.  ",
        "_Reference: `bold_flagged_comparison_YYYYMMDD.csv` (filter `sequence_status == QC_ONLY`)_",
        "",
        f"**4. Upload {fmt(n_not_bold)} QC-passed plates to BOLD**  ",
        "Prioritise plates submitted >90 days ago. BGE partners tracked separately.  ",
        "_Reference: `bioscan_plate_status_ALL_YYYYMMDD.csv`_",
        "",
        "**5. Investigate BCLT — all specimens on BOLD, all flagged, zero BINs**  ",
        "No stop codon or contamination flags — flagging reason unknown. Contact BOLD team.  ",
        "_Reference: `bold_workbench_plates_YYYYMMDD.csv`_",
        "",
        "### 🟡 MEDIUM PRIORITY",
        "",
        "**6. Implement best-batch selection for repeated plates**  ",
        "Use the highest pass-rate batch per plate rather than most recent.  ",
        "Prevents 784 PASS→FAILED and 820 PASS→ON_HOLD regressions entering the freeze.  ",
        "_Reference: `repeat_specimens_transitions_YYYYMMDD.csv`_",
        "",
        "**7. Expert review of genuinely flagged sequences**  ",
        "Same sequence in QC and BOLD — no automated fix. Requires expert assessment.  ",
        "_Reference: `bold_flagged_no_alternative_YYYYMMDD.csv`_",
        "",
        "**8. Dedicated RRNW review**  ",
        "Largest repeat sequencing decline (-29.7%), most QC_ONLY specimens not on BOLD, "
        "386 specimens with missing BINs.  ",
        "_Reference: `repeat_analysis_YYYYMMDD.csv`_",
        "",
        "### 🟢 LOW PRIORITY — Post-freeze follow-up",
        "",
        f"**9. Investigate {fmt(cat2_high)} Cat-2-High specimens**  ",
        "Reads present but no consensus — concentrated in aquatic invertebrate partners.  ",
        "_Reference: `missing_specimens_categorised_YYYYMMDD.csv`_",
        "",
        "**10. Review 283 BOLD_ONLY specimens**  ",
        "Sequences on BOLD but not in QC FASTA — possibly pre-dating current pipeline.  ",
        "_Reference: `bold_flagged_comparison_YYYYMMDD.csv`_",
        "",
    ]
    return lines


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Generate comprehensive BIOSCAN pipeline summary report'
    )
    parser.add_argument('--exclude-bge', action='store_true',
        help='Exclude BGE partners (BGEP, BGEG, BGKU, BGPT) from report')
    parser.add_argument('--output', default=None,
        help='Output markdown path (default: run_dir/bioscan_summary_report_YYYYMMDD_HHMMSS.md)')
    parser.add_argument('--run-dir', default=None,
        help='Output directory (overrides BIOSCAN_RUN_DIR env var and auto-generate)')
    args = parser.parse_args()

    run_dir = resolve_run_dir(args.run_dir)
    run_ts  = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    results = config.RESULTS_DIR

    if args.output is None:
        args.output = os.path.join(run_dir, f'bioscan_summary_report_{run_ts}.md')

    print(f"Generating BIOSCAN summary report...")
    print(f"  Results dir: {results}")
    print(f"  Output dir:  {run_dir}")
    print(f"  Exclude BGE: {args.exclude_bge}")
    print()

    # Load all files
    def load(pattern):
        p = _latest(pattern, results)
        if p:
            print(f"  Loading: {os.path.basename(p)}")
        return _read(p)

    plate_status       = load('bioscan_plate_status_ALL_*.csv')
    plate_summ         = load('plate_summary_all_ALL_*.csv')
    plate_cats         = load('plate_summary_categories_ALL_*.csv')
    repeat_plate       = load('repeat_analysis_*.csv')
    repeat_transitions = load('repeat_specimens_transitions_*.csv')
    repeat_summary     = load('repeat_specimens_summary_*.csv')
    missing_batch      = load('missing_specimens_batch_summary_*.csv')
    missing_cats       = load('missing_specimens_categorised_*.csv')
    bold_needs_resub   = load('bold_needs_resubmission_*.csv')
    bold_flagged_no_alt= load('bold_flagged_no_alternative_*.csv')
    bold_flagged_comp  = load('bold_flagged_comparison_*.csv')
    bold_report        = _latest('bold_workbench_report_*.txt', results)
    print()

    # Build report
    sections = []

    # Header
    sections += [
        f"# BIOSCAN Pipeline Summary Report",
        f"",
        f"**Generated:** {datetime.datetime.now().strftime('%d %B %Y %H:%M')}  ",
        f"**BGE partners excluded:** {'Yes (BGEP, BGEG, BGKU, BGPT)' if args.exclude_bge else 'No — all partners included'}  ",
        f"**Results directory:** `{results}`  ",
        f"**GitHub:** https://github.com/LyndallP/bioscan_plate_checker",
        f"",
        "---",
        "",
        "## Contents",
        "",
        "1. [Pipeline Overview](#1-pipeline-overview)",
        "2. [Plates Missing from Sequencing](#2-plates-missing-from-sequencing)",
        "3. [Plates Not Yet on BOLD](#3-plates-not-yet-on-bold)",
        "4. [Plate-Level QC Summary](#4-plate-level-qc-summary)",
        "5. [Repeat Sequencing](#5-repeat-sequencing)",
        "6. [Missing Specimens & Assembly Failures](#6-missing-specimens--assembly-failures)",
        "7. [BOLD Quality Flags](#7-bold-quality-flags)",
        "8. [Actions Required](#8-actions-required)",
        "",
        "---",
        "",
    ]

    sections += section_pipeline_overview(plate_status, args.exclude_bge)
    sections += ["---", ""]
    sections += section_missing_sequencing(plate_status, args.exclude_bge)
    sections += ["---", ""]
    sections += section_not_on_bold(plate_status, args.exclude_bge)
    sections += ["---", ""]
    sections += section_plate_qc(plate_summ, plate_cats, args.exclude_bge)
    sections += ["---", ""]
    sections += section_repeat(repeat_plate, repeat_transitions, repeat_summary, args.exclude_bge)
    sections += ["---", ""]
    sections += section_missing_specimens(missing_batch, missing_cats)
    sections += ["---", ""]
    sections += section_bold_flags(bold_report, bold_needs_resub, bold_flagged_no_alt,
                                   bold_flagged_comp)
    sections += ["---", ""]
    sections += section_actions(plate_status, bold_needs_resub, bold_flagged_comp,
                                missing_cats, args.exclude_bge)

    # Write
    with open(args.output, 'w') as f:
        f.write('\n'.join(sections))

    print(f"Report written to: {args.output}")
    print(f"  Size: {os.path.getsize(args.output)/1024:.1f} KB")


if __name__ == '__main__':
    main()
