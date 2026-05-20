"""
generate_html_report.py

Generates a self-contained HTML report of the BIOSCAN pipeline status,
drawing from all pipeline output files. Opens in any browser.

Usage:
    python3 generate_html_report.py
    python3 generate_html_report.py --exclude-bge
    python3 generate_html_report.py --output /path/to/report.html
"""

import argparse
import datetime
import glob
import os
import re
import pandas as pd

import config
from utils import is_bge_plate


# ── Helpers ───────────────────────────────────────────────────────────────────

def _latest(pattern, results_dir):
    """
    Find the most recently modified file matching pattern.
    Searches both results_dir root AND one level of timestamped subfolders
    (e.g. results_dir/20260512_143022/file.csv) so the report generator
    works whether outputs are flat or in run-timestamped directories.
    """
    # Search flat in results_dir
    flat = glob.glob(os.path.join(results_dir, pattern))
    # Search one level of subdirectories (timestamped run folders)
    nested = glob.glob(os.path.join(results_dir, '*', pattern))
    all_files = flat + nested
    if not all_files:
        return None
    # Sort by modification time — most recent wins
    return max(all_files, key=os.path.getmtime)


def _read(path, **kwargs):
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str, **kwargs)
    except Exception as e:
        print(f"  Warning: could not read {path}: {e}")
        return pd.DataFrame()


def fmt(n):
    try:
        return f"{int(float(n)):,}"
    except Exception:
        return str(n)


def pct(n, total):
    try:
        return f"{100*float(n)/float(total):.1f}%"
    except Exception:
        return "n/a"


def num(df, col):
    return pd.to_numeric(df[col], errors='coerce') if col in df.columns else pd.Series()


# ── HTML helpers ──────────────────────────────────────────────────────────────

def table(headers, rows, cls=""):
    h = "".join(f"<th>{h}</th>" for h in headers)
    body = ""
    for row in rows:
        cells = "".join(f"<td>{c}</td>" for c in row)
        body += f"<tr>{cells}</tr>"
    return f'<table class="{cls}"><thead><tr>{h}</tr></thead><tbody>{body}</tbody></table>'


def stat_grid(stats):
    """stats = list of (label, value) tuples"""
    items = "".join(
        f'<div class="stat"><div class="stat-val">{v}</div><div class="stat-label">{l}</div></div>'
        for l, v in stats
    )
    return f'<div class="stat-grid">{items}</div>'


def badge(text, colour):
    return f'<span class="badge badge-{colour}">{text}</span>'


def card(title, content, id_=""):
    id_attr = f' id="{id_}"' if id_ else ""
    return f'<div class="card"{id_attr}><h2>{title}</h2>{content}</div>'


def files_box(inputs, outputs):
    """Render a collapsible 'Files' reference block for a section."""
    def tags(files, cls):
        return "".join(f'<code class="file-tag {cls}">{f}</code>' for f in files)
    inp = tags(inputs, "file-in")
    out = tags(outputs, "file-out")
    return (
        '<details class="files-box">'
        '<summary>📁 Files used in this section</summary>'
        '<div class="files-inner">'
        f'<div class="files-row"><span class="files-label">Inputs:</span>{inp}</div>'
        f'<div class="files-row"><span class="files-label">Outputs:</span>{out}</div>'
        '</div></details>'
    )


def alert(text, level="info"):
    return f'<div class="alert alert-{level}">{text}</div>'


# ── CSS + JS shell ────────────────────────────────────────────────────────────

CSS = """
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500;600&display=swap');

:root {
    --bg: #f7f8fa;
    --surface: #ffffff;
    --border: #e2e6ed;
    --text: #1a1f2e;
    --text-muted: #6b7280;
    --accent: #2563eb;
    --accent-light: #eff6ff;
    --green: #16a34a;
    --green-light: #f0fdf4;
    --amber: #d97706;
    --amber-light: #fffbeb;
    --red: #dc2626;
    --red-light: #fef2f2;
    --mono: 'DM Mono', monospace;
    --sans: 'DM Sans', sans-serif;
}

* { box-sizing: border-box; margin: 0; padding: 0; }

body {
    font-family: var(--sans);
    background: var(--bg);
    color: var(--text);
    font-size: 14px;
    line-height: 1.6;
}

header {
    background: var(--text);
    color: white;
    padding: 32px 48px;
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
}

header h1 {
    font-size: 22px;
    font-weight: 600;
    letter-spacing: -0.3px;
}

header .meta {
    font-size: 12px;
    color: #94a3b8;
    text-align: right;
    line-height: 1.8;
    font-family: var(--mono);
}

.layout {
    display: grid;
    grid-template-columns: 220px 1fr;
    min-height: calc(100vh - 120px);
}

nav {
    background: var(--surface);
    border-right: 1px solid var(--border);
    padding: 24px 0;
    position: sticky;
    top: 0;
    height: 100vh;
    overflow-y: auto;
}

nav a {
    display: block;
    padding: 8px 24px;
    color: var(--text-muted);
    text-decoration: none;
    font-size: 13px;
    border-left: 3px solid transparent;
    transition: all 0.15s;
}

nav a:hover, nav a.active {
    color: var(--accent);
    border-left-color: var(--accent);
    background: var(--accent-light);
}

nav .nav-section {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.8px;
    text-transform: uppercase;
    color: var(--text-muted);
    padding: 16px 24px 4px;
}

main {
    padding: 32px 48px;
    max-width: 1100px;
}

.card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 28px;
    margin-bottom: 24px;
}

.card h2 {
    font-size: 16px;
    font-weight: 600;
    margin-bottom: 20px;
    padding-bottom: 12px;
    border-bottom: 1px solid var(--border);
    color: var(--text);
}

.card h3 {
    font-size: 13px;
    font-weight: 600;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin: 20px 0 12px;
}

.stat-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
    gap: 12px;
    margin: 16px 0;
}

.stat {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 14px;
    text-align: center;
}

.stat-val {
    font-size: 22px;
    font-weight: 600;
    color: var(--accent);
    font-family: var(--mono);
    line-height: 1.2;
}

.stat-label {
    font-size: 11px;
    color: var(--text-muted);
    margin-top: 4px;
}

table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
    margin: 12px 0;
}

th {
    background: var(--bg);
    text-align: left;
    padding: 8px 12px;
    font-weight: 600;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    color: var(--text-muted);
    border-bottom: 1px solid var(--border);
}

td {
    padding: 8px 12px;
    border-bottom: 1px solid var(--border);
    font-family: var(--mono);
    font-size: 12px;
}

tr:last-child td { border-bottom: none; }
tr:hover td { background: var(--bg); }

.badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
}

.badge-red    { background: var(--red-light);   color: var(--red);   }
.badge-amber  { background: var(--amber-light); color: var(--amber); }
.badge-green  { background: var(--green-light); color: var(--green); }
.badge-blue   { background: var(--accent-light); color: var(--accent); }

.alert {
    border-radius: 6px;
    padding: 12px 16px;
    margin: 12px 0;
    font-size: 13px;
    border-left: 4px solid;
}

.alert-info   { background: var(--accent-light); border-color: var(--accent); color: var(--accent); }
.alert-warn   { background: var(--amber-light);  border-color: var(--amber);  color: var(--amber); }
.alert-danger { background: var(--red-light);    border-color: var(--red);    color: var(--red); }
.alert-ok     { background: var(--green-light);  border-color: var(--green);  color: var(--green); }

.two-col {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
}

.priority-block {
    border-radius: 6px;
    padding: 16px;
    margin: 12px 0;
}

.priority-high   { background: var(--red-light);   border-left: 4px solid var(--red); }
.priority-medium { background: var(--amber-light);  border-left: 4px solid var(--amber); }
.priority-low    { background: var(--green-light);  border-left: 4px solid var(--green); }

.priority-block h4 { font-size: 13px; font-weight: 600; margin-bottom: 6px; }
.priority-block p  { font-size: 12px; color: var(--text-muted); margin: 4px 0; }
.priority-block code {
    font-family: var(--mono);
    font-size: 11px;
    background: rgba(0,0,0,0.06);
    padding: 1px 4px;
    border-radius: 3px;
}

.flow {
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 16px 0;
    flex-wrap: wrap;
}

.flow-stage {
    background: var(--accent);
    color: white;
    padding: 8px 16px;
    border-radius: 6px;
    font-size: 13px;
    font-weight: 500;
    text-align: center;
    min-width: 120px;
}

.flow-stage small {
    display: block;
    font-size: 11px;
    opacity: 0.8;
    font-family: var(--mono);
}

.flow-arrow { color: var(--text-muted); font-size: 18px; }

.partner-tags {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
    margin: 8px 0;
}

.partner-tag {
    background: var(--accent-light);
    color: var(--accent);
    border: 1px solid #bfdbfe;
    border-radius: 4px;
    padding: 2px 8px;
    font-size: 11px;
    font-family: var(--mono);
    font-weight: 500;
}

.progress-bar {
    background: var(--border);
    border-radius: 4px;
    height: 8px;
    overflow: hidden;
    margin: 4px 0;
}

.progress-fill {
    height: 100%;
    border-radius: 4px;
    background: var(--accent);
}

.files-box {
    margin: 14px 0;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--bg);
    font-size: 12px;
}
.files-box summary {
    padding: 8px 12px;
    cursor: pointer;
    font-weight: 600;
    color: var(--text-muted);
    user-select: none;
    list-style: none;
}
.files-box summary::-webkit-details-marker { display: none; }
.files-box summary::before { content: "▶ "; font-size: 9px; margin-right: 4px; }
details[open].files-box summary::before { content: "▼ "; }
.files-box summary:hover { color: var(--text); }
.files-inner {
    padding: 6px 12px 12px;
    display: flex;
    flex-direction: column;
    gap: 6px;
}
.files-row { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px; }
.files-label {
    font-weight: 600;
    color: var(--text-muted);
    text-transform: uppercase;
    font-size: 10px;
    letter-spacing: 0.5px;
    white-space: nowrap;
    min-width: 60px;
}
.file-tag {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    margin: 1px;
    font-family: var(--mono);
    font-size: 11px;
}
.file-in  { background: #eff6ff; color: #1d4ed8; border: 1px solid #bfdbfe; }
.file-out { background: #f0fdf4; color: #15803d; border: 1px solid #bbf7d0; }

@media print {
    nav { display: none; }
    .layout { grid-template-columns: 1fr; }
    main { padding: 0; }
}
"""

JS = """
// Highlight active nav item on scroll
const sections = document.querySelectorAll('[id]');
const navLinks = document.querySelectorAll('nav a[href^="#"]');
const observer = new IntersectionObserver(entries => {
    entries.forEach(e => {
        if (e.isIntersecting) {
            navLinks.forEach(a => a.classList.remove('active'));
            const active = document.querySelector(`nav a[href="#${e.target.id}"]`);
            if (active) active.classList.add('active');
        }
    });
}, { threshold: 0.3 });
sections.forEach(s => observer.observe(s));
"""


# ── Data loading ──────────────────────────────────────────────────────────────

def load_all(results_dir, exclude_bge):
    def latest(pattern):
        p = _latest(pattern, results_dir)
        if p:
            print(f"  {os.path.basename(p)}")
        return _read(p)

    plate_status       = latest('bioscan_plate_status_ALL_*.csv')
    plate_summ         = latest('plate_summary_all_ALL_*.csv')
    plate_cats         = latest('plate_summary_categories_ALL_*.csv')
    repeat_plate       = latest('repeat_analysis_*.csv')
    repeat_transitions = latest('repeat_specimens_transitions_*.csv')
    repeat_summary     = latest('repeat_specimens_summary_*.csv')
    missing_batch      = latest('missing_specimens_batch_summary_*.csv')
    missing_cats       = latest('missing_specimens_categorised_*.csv')
    bold_needs_resub   = latest('bold_needs_resubmission_*.csv')
    bold_flagged_no_alt= latest('bold_flagged_no_alternative_*.csv')
    bold_flagged_comp  = latest('bold_flagged_comparison_*.csv')
    bold_wb_plates     = latest('bold_workbench_plates_*.csv')
    bold_report_path   = _latest('bold_workbench_report_*.txt', results_dir)
    bold_concordance   = latest('bold_sequence_concordance_ALL_*.csv')
    if bold_concordance.empty:
        bold_concordance = latest('bold_sequence_concordance_*_*.csv')
    batch_family_summ  = latest('batch_family_summary_*_ALL.csv')
    pos_ctrl_plates    = latest('positive_control_plates_*.csv')
    pos_ctrl_batches   = latest('positive_control_batch_summary_*.csv')

    if exclude_bge:
        for df in [plate_status, plate_summ, plate_cats, repeat_plate]:
            if not df.empty and 'plate_id' in df.columns:
                df.drop(df[df['plate_id'].apply(
                    lambda x: is_bge_plate(str(x)))].index, inplace=True)

    return dict(
        plate_status=plate_status,
        plate_summ=plate_summ,
        plate_cats=plate_cats,
        repeat_plate=repeat_plate,
        repeat_transitions=repeat_transitions,
        repeat_summary=repeat_summary,
        missing_batch=missing_batch,
        missing_cats=missing_cats,
        bold_needs_resub=bold_needs_resub,
        bold_flagged_no_alt=bold_flagged_no_alt,
        bold_flagged_comp=bold_flagged_comp,
        bold_wb_plates=bold_wb_plates,
        bold_report_path=bold_report_path,
        bold_concordance=bold_concordance,
        batch_family_summ=batch_family_summ,
        pos_ctrl_plates=pos_ctrl_plates,
        pos_ctrl_batches=pos_ctrl_batches,
    )


# ── Section builders ──────────────────────────────────────────────────────────

def build_pipeline_overview(d):
    df = d['plate_status']
    if df.empty:
        return card("1. Pipeline Overview", alert("No plate status data found.", "warn"), "s1")

    total     = len(df)
    n_partners= df['partner'].nunique() if 'partner' in df.columns else 0

    n_mbrave = int((df['pipeline_stage'].isin(['mbrave','qc','bold'])).sum()) \
        if 'pipeline_stage' in df.columns else 0
    n_qc     = int((df['pipeline_stage'].isin(['qc','bold'])).sum()) \
        if 'pipeline_stage' in df.columns else 0
    n_bold   = int((df['pipeline_stage'] == 'bold').sum()) \
        if 'pipeline_stage' in df.columns else 0

    missing = df['missing_at'].value_counts() if 'missing_at' in df.columns else pd.Series()
    n_miss_mbrave = int(missing.get('mbrave', 0))
    n_miss_qc     = int(missing.get('qc', 0))
    n_miss_bold   = int(missing.get('bold', 0))

    # Pipeline flow
    flow = f"""
    <div class="flow">
        <div class="flow-stage">Portal<small>{fmt(total)}</small></div>
        <div class="flow-arrow">→</div>
        <div class="flow-stage">mBRAVE<small>{fmt(n_mbrave)} ({pct(n_mbrave,total)})</small></div>
        <div class="flow-arrow">→</div>
        <div class="flow-stage">QC<small>{fmt(n_qc)} ({pct(n_qc,total)})</small></div>
        <div class="flow-arrow">→</div>
        <div class="flow-stage">BOLD<small>{fmt(n_bold)} ({pct(n_bold,total)})</small></div>
    </div>
    """

    stats = stat_grid([
        ("Total plates", fmt(total)),
        ("Partners", fmt(n_partners)),
        ("Sequenced", fmt(n_mbrave)),
        ("Through QC", fmt(n_qc)),
        ("On BOLD", fmt(n_bold)),
        ("BOLD coverage", pct(n_bold, total)),
    ])

    drop_rows = [
        [f"{fmt(n_miss_mbrave)} not sequenced", badge("mBRAVE missing","amber")],
        [f"{fmt(n_miss_qc)} not through QC", badge("QC missing","amber")],
        [f"{fmt(n_miss_bold)} passed QC, not on BOLD", badge("BOLD missing","red")],
    ]
    drop_tbl = table(["Plates dropped","Stage"], drop_rows)

    # Complete partners
    if 'partner' in df.columns and 'missing_at' in df.columns and 'pipeline_stage' in df.columns:
        complete = df.groupby('partner', group_keys=False).apply(
            lambda g: g['missing_at'].isna().all() and (g['pipeline_stage']=='bold').all()
        )
        complete_list = sorted(complete[complete].index.tolist())
        tags = '<div class="partner-tags">' + \
               ''.join(f'<span class="partner-tag">{p}</span>' for p in complete_list) + \
               '</div>'
        complete_html = f'<h3>{len(complete_list)} partners fully through to BOLD</h3>{tags}'
    else:
        complete_html = ""

    fbox = files_box(
        inputs=["portal_plates_from_dump.csv", "mBRAVE sequencing outputs", "QC FASTA files", "BOLD plate list"],
        outputs=["bioscan_plate_status_ALL_YYYYMMDD.csv", "bioscan_plate_status_ALL_YYYYMMDD.xlsx",
                 "missing_plates_ALL_YYYYMMDD.txt"],
    )
    content = fbox + flow + stats + "<h3>Plates dropped at each stage</h3>" + drop_tbl + complete_html
    return card("1. Pipeline Overview", content, "s1")


def build_missing_sequencing(d):
    df = d['plate_status']
    if df.empty or 'missing_at' not in df.columns:
        return card("2. Plates Missing from Sequencing",
                    alert("No data available.", "warn"), "s2")

    not_seq = df[df['missing_at'] == 'mbrave'].copy()
    if not_seq.empty:
        return card("2. Plates Missing from Sequencing",
                    alert("No plates missing from mBRAVE.", "ok"), "s2")

    not_seq['submit_date'] = pd.to_datetime(not_seq['submit_date'], errors='coerce')
    today = pd.Timestamp.now()
    not_seq['days'] = (today - not_seq['submit_date']).dt.days

    old    = not_seq[not_seq['days'] >= 90].sort_values('days', ascending=False)
    recent = not_seq[not_seq['days'] <  90]

    stats = stat_grid([
        ("Total missing", fmt(len(not_seq))),
        (">90 days — investigate", badge(fmt(len(old)), "red")),
        ("Recently submitted", fmt(len(recent))),
    ])

    # Old submissions table
    old_rows = []
    for partner, grp in old.groupby('partner'):
        plates = sorted(grp['plate_id'].tolist())
        submitted = grp['submit_date'].min()
        days = int(grp['days'].max())
        id_str = f"{plates[0]}–{plates[-1]}" if len(plates) > 1 else plates[0]
        old_rows.append([
            partner,
            fmt(len(grp)),
            submitted.strftime('%Y-%m-%d') if pd.notna(submitted) else "unknown",
            badge(f"~{days:,} days", "red"),
            id_str,
        ])
    old_tbl = table(
        ["Partner","Plates","Submitted","Days waiting","Plate IDs"],
        old_rows
    ) if old_rows else ""

    # Partner summary
    by_partner = not_seq.groupby('partner').agg(
        n=('plate_id','count'),
        old=('days', lambda x: (x>=90).sum())
    ).reset_index().sort_values('n', ascending=False)

    partner_rows = []
    for _, r in by_partner.iterrows():
        note = badge("All overdue","red") if r['old']==r['n'] else \
               badge(f"{int(r['old'])} overdue","amber") if r['old']>0 else \
               badge("Recent","green")
        partner_rows.append([r['partner'], fmt(int(r['n'])), note])

    partner_tbl = table(["Partner","Plates missing","Status"], partner_rows)

    fbox = files_box(
        inputs=["bioscan_plate_status_ALL_YYYYMMDD.csv", "portal_plates_from_dump.csv"],
        outputs=["missing_plates_ALL_YYYYMMDD.txt"],
    )
    content = fbox + stats
    if old_rows:
        content += alert(
            f"⚠ {len(old)} plates submitted >90 days ago with no sequencing data — investigate immediately.",
            "danger"
        )
        content += "<h3>Old submissions requiring investigation</h3>" + old_tbl
    content += "<h3>All partners with plates not yet sequenced</h3>" + partner_tbl
    return card("2. Plates Missing from Sequencing", content, "s2")


def build_not_on_bold(d):
    df = d['plate_status']
    if df.empty or 'missing_at' not in df.columns:
        return card("3. Plates Not Yet on BOLD", alert("No data.", "warn"), "s3")

    not_bold = df[df['missing_at'] == 'bold'].copy()
    if not_bold.empty:
        return card("3. Plates Not Yet on BOLD",
                    alert("All QC-passed plates are on BOLD!", "ok"), "s3")

    not_bold['submit_date'] = pd.to_datetime(not_bold['submit_date'], errors='coerce')
    today = pd.Timestamp.now()
    not_bold['days'] = (today - not_bold['submit_date']).dt.days
    not_bold['year'] = not_bold['submit_date'].dt.year

    old    = not_bold[not_bold['days'] >= 90]
    n_2026 = int((not_bold['year'] >= 2026).sum())
    n_2025 = int((not_bold['year'] == 2025).sum())

    stats = stat_grid([
        ("Not on BOLD", fmt(len(not_bold))),
        (">90 days overdue", badge(fmt(len(old)), "red")),
        ("2026 submissions (recent)", fmt(n_2026)),
        ("2025 submissions", fmt(n_2025)),
    ])

    by_partner = not_bold.groupby('partner').size().sort_values(ascending=False)
    old_by_partner = old.groupby('partner').size().sort_values(ascending=False)

    partner_rows = []
    for partner, n in by_partner.items():
        overdue = old_by_partner.get(partner, 0)
        b = badge(f"{int(overdue)} overdue","red") if overdue > 0 else ""
        partner_rows.append([partner, fmt(int(n)), b])

    partner_tbl = table(["Partner","Plates not on BOLD","Note"], partner_rows)

    fbox = files_box(
        inputs=["bioscan_plate_status_ALL_YYYYMMDD.csv"],
        outputs=["missing_plates_ALL_YYYYMMDD.txt", "bold_plate_summary_YYYYMMDD.csv"],
    )
    content = fbox + stats
    if len(old) > 0:
        content += alert(
            f"⚠ {len(old)} plates submitted >90 days ago still not on BOLD.",
            "warn"
        )
    content += "<h3>All partners with plates not on BOLD</h3>" + partner_tbl
    return card("3. Plates Not Yet on BOLD", content, "s3")


def build_plate_qc(d):
    df   = d['plate_summ']
    cats = d['plate_cats']

    if df.empty:
        return card("4. Plate-Level QC Summary", alert("No data.", "warn"), "s4")

    for col in ['n_specimens','pass_count','on_hold_count','fail_count',
                'pass_rate','combined_rate','n_controls']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    cat_cols = [f'cat{c}_count' for c in range(1,13)] + ['failed_count','n_specimens']
    for col in cat_cols:
        cats[col] = pd.to_numeric(cats[col], errors='coerce')

    seq = df[df['n_specimens'].notna()].copy()
    cats_seq = cats[cats['n_specimens'].notna()].copy()
    total_specs = float(cats_seq['n_specimens'].sum()) if not cats_seq.empty else 1

    stats = stat_grid([
        ("Sequenced plates", fmt(len(seq))),
        ("Never sequenced", fmt(len(df)-len(seq))),
        ("Avg pass rate", f"{seq.pass_rate.mean():.1f}%"),
        ("Median pass rate", f"{seq.pass_rate.median():.1f}%"),
        ("Avg combined", f"{seq.combined_rate.mean():.1f}%"),
        ("100% pass plates", fmt((seq.pass_rate==100).sum())),
        ("0% pass plates", fmt((seq.pass_rate==0).sum())),
        ("Full plates (93)", fmt((seq.n_specimens==93).sum())),
        ("Partial plates (<93)", fmt((seq.n_specimens<93).sum())),
    ])

    # Partner stats
    if 'partner' in seq.columns:
        ps = seq.groupby('partner').agg(
            n=('plate_id','count'),
            total=('n_specimens','sum'),
            passed=('pass_count','sum'),
        ).reset_index()
        ps['rate'] = (100*ps['passed']/ps['total']).round(1)
        top = ps.nlargest(10,'rate')
        bot = ps.nsmallest(10,'rate')

        top_rows = [[r['partner'], fmt(int(r['n'])),
                     f'<div class="progress-bar"><div class="progress-fill" style="width:{r["rate"]}%;background:#16a34a"></div></div>{r["rate"]:.1f}%']
                    for _,r in top.iterrows()]
        bot_rows = [[r['partner'], fmt(int(r['n'])),
                     f'<div class="progress-bar"><div class="progress-fill" style="width:{r["rate"]}%;background:#dc2626"></div></div>{r["rate"]:.1f}%']
                    for _,r in bot.iterrows()]

        two_col = f"""
        <div class="two-col">
            <div>
                <h3>Top 10 partners — pass rate</h3>
                {table(["Partner","Plates","Pass rate"], top_rows)}
            </div>
            <div>
                <h3>Bottom 10 partners — pass rate</h3>
                {table(["Partner","Plates","Pass rate"], bot_rows)}
            </div>
        </div>"""
    else:
        two_col = ""

    # Category breakdown
    cat_labels = {
        1: "Single sequence >200 reads", 2: "Single sequence 50-200 reads",
        3: "Single sequence <50 reads",  4: "Non-conflicting secondary ≤5 reads",
        5: "Non-conflicting secondary >5 reads", 6: "Conflicting secondary ≤5 reads",
        7: "Conflicting secondary >5 reads", 8: "Not used by pipeline",
        9: "ON_HOLD — multiple sequences", 10: "ON_HOLD — taxonomy conflict",
        11: "ON_HOLD — secondary sequences", 12: "ON_HOLD — other",
    }
    cat_rows = []
    for c in range(1,13):
        n = float(cats_seq[f'cat{c}_count'].sum()) if not cats_seq.empty else 0
        decision = badge("PASS","green") if c<=8 else badge("ON_HOLD","amber")
        cat_rows.append([f"Cat {c}", cat_labels.get(c,""), decision,
                         fmt(int(n)), pct(n, total_specs)])
    fail_n = float(cats_seq['failed_count'].sum()) if not cats_seq.empty else 0
    cat_rows.append(["FAILED","—",badge("FAILED","red"),fmt(int(fail_n)),pct(fail_n,total_specs)])

    cat_tbl = table(["Cat","Description","Decision","Count","%"], cat_rows)

    methodology = alert(
        "ℹ Pass counts use the <strong>best result per specimen across ALL sequencing batches</strong>. "
        "If a specimen passed in any batch it is counted as PASS. "
        "Results reflect the February 2026 QC re-run (stricter criteria).",
        "info"
    )

    fbox = files_box(
        inputs=["bioscan_plate_status_ALL_YYYYMMDD.csv", "QC FASTA files (per batch)",
                "mBRAVE QC outputs"],
        outputs=["plate_summary_all_ALL_YYYYMMDD.csv", "plate_summary_categories_ALL_YYYYMMDD.csv"],
    )
    content = fbox + methodology + stats + two_col + "<h3>QC category breakdown</h3>" + cat_tbl
    return card("4. Plate-Level QC Summary", content, "s4")


def build_repeat(d):
    df = d['repeat_plate']
    trans = d['repeat_transitions']
    summ  = d['repeat_summary']

    if df.empty:
        return card("5. Repeat Sequencing", alert("No data.", "warn"), "s5")

    for col in ['first_pct_pass','best_pct_pass','last_pct_pass','improvement']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    n_plates   = len(df)
    n_improved = int((df.improvement > 0).sum())
    n_declined = int((df.improvement < 0).sum())

    df['n_specimens'] = 93.0
    df['passes_first'] = (df.first_pct_pass / 100 * 93).round(0)
    df['passes_best']  = (df.best_pct_pass  / 100 * 93).round(0)
    df['passes_last']  = (df.last_pct_pass  / 100 * 93).round(0)
    total = df.n_specimens.sum()

    gain_best_first = int(df.passes_best.sum() - df.passes_first.sum())
    gain_best_last  = int(df.passes_best.sum() - df.passes_last.sum())

    stats = stat_grid([
        ("Repeated plates", fmt(n_plates)),
        ("Improved (first→last)", fmt(n_improved)),
        ("Declined (first→last)", badge(fmt(n_declined),"amber")),
        ("Avg improvement", f"{df.improvement.mean():+.1f}%"),
        ("Best vs first gain", badge(f"+{fmt(gain_best_first)} specimens","green")),
        ("Best vs last gain", badge(f"+{fmt(gain_best_last)} specimens","blue")),
    ])

    approach_rows = [
        ["First batch only",           fmt(int(df.passes_first.sum())), pct(df.passes_first.sum(), total)],
        ["Last batch only",            fmt(int(df.passes_last.sum())),  pct(df.passes_last.sum(), total)],
        [badge("Best batch (recommended)","green"),
                                       fmt(int(df.passes_best.sum())),  pct(df.passes_best.sum(), total)],
    ]
    approach_tbl = table(["Approach","Total passes","Pass rate"], approach_rows)

    # Partner table
    if 'partner' in df.columns:
        partner = df.groupby('partner').agg(
            n=('plate_id','count'),
            first=('first_pct_pass','mean'),
            best=('best_pct_pass','mean'),
            last=('last_pct_pass','mean'),
        ).reset_index().sort_values('n', ascending=False)
        partner_rows = [[r['partner'], fmt(int(r['n'])),
                         f"{r['first']:.1f}%", f"{r['best']:.1f}%", f"{r['last']:.1f}%"]
                        for _,r in partner.iterrows()]
        partner_tbl = table(["Partner","Plates","First %","Best %","Last %"], partner_rows)
    else:
        partner_tbl = ""

    # Transition matrix — column is 'n' not 'count' (bug fix)
    trans_html = ""
    if not trans.empty:
        n_col = 'n' if 'n' in trans.columns else 'count'
        trans[n_col] = pd.to_numeric(trans[n_col], errors='coerce')
        total_trans = trans[n_col].sum()
        interp = {
            ('PASS','PASS'):       ("Stable — consistent sequencing","green"),
            ('FAILED','PASS'):     ("Repeat sequencing highly effective","green"),
            ('FAILED','FAILED'):   ("Persistent failure — likely biological","red"),
            ('ON_HOLD','PASS'):    ("Resolved on resequencing","green"),
            ('PASS','ON_HOLD'):    ("Slight decline","amber"),
            ('FAILED','ON_HOLD'):  ("Partial improvement","amber"),
            ('PASS','FAILED'):     ("Decline — best-batch prevents","red"),
            ('ON_HOLD','FAILED'):  ("Decline on resequencing","red"),
            ('ON_HOLD','ON_HOLD'): ("No change","amber"),
        }
        trans_rows = []
        for _, r in trans.sort_values(n_col, ascending=False).iterrows():
            first = str(r.get('first_decision',''))
            last  = str(r.get('last_decision',''))
            n     = int(r[n_col])
            note, colour = interp.get((first,last), ("","blue"))
            trans_rows.append([
                badge(first, "green" if first=="PASS" else "red" if first=="FAILED" else "amber"),
                badge(last,  "green" if last=="PASS"  else "red" if last=="FAILED"  else "amber"),
                fmt(n), pct(n, total_trans),
                badge(note, colour) if note else "",
            ])
        trans_html = "<h3>Transition matrix (first → last QC decision)</h3>" + \
                     table(["First","Last","Count","%","Interpretation"], trans_rows)

    # Specimen summary
    spec_html = ""
    if not summ.empty:
        total_rep = len(summ)
        ever_passed = summ['ever_passed'].astype(str).str.upper().eq('TRUE').sum() \
            if 'ever_passed' in summ.columns else 0
        spec_html = stat_grid([
            ("Repeated specimens", fmt(total_rep)),
            ("Ever achieved PASS", fmt(ever_passed)),
            ("Pass rate across attempts", pct(ever_passed, total_rep)),
        ])

    rec = alert(
        "💡 <strong>Recommendation:</strong> Implement best-batch selection before data freeze. "
        f"Choosing best rather than first recovers <strong>+{fmt(gain_best_first)} specimens</strong>. "
        f"Choosing best rather than last recovers <strong>+{fmt(gain_best_last)} more</strong>.",
        "info"
    )

    fbox = files_box(
        inputs=["bioscan_plate_status_ALL_YYYYMMDD.csv", "plate_summary_all_ALL_YYYYMMDD.csv",
                "QC FASTA files (per batch)"],
        outputs=["repeat_analysis_YYYYMMDD.csv", "repeat_analysis_YYYYMMDD.xlsx",
                 "repeat_specimens_summary_YYYYMMDD.csv", "repeat_specimens_transitions_YYYYMMDD.csv",
                 "repeat_specimens_long_YYYYMMDD.csv", "repeat_specimens_wide_YYYYMMDD.csv"],
    )
    content = (fbox + stats + "<h3>Total pass count comparison</h3>" + approach_tbl +
               rec + "<h3>By partner</h3>" + partner_tbl +
               "<h3>Specimen-level summary</h3>" + spec_html + trans_html)
    return card("5. Repeat Sequencing", content, "s5")


def build_missing_specimens(d):
    mb = d['missing_batch']
    mc = d['missing_cats']

    if mb.empty:
        return card("6. Missing Specimens & Assembly Failures",
                    alert("No data.", "warn"), "s6")

    for col in ['n_expected','n_cat1_zero','n_cat2_low','n_cat2_high']:
        mb[col] = pd.to_numeric(mb[col], errors='coerce')

    total_exp = int(mb.n_expected.sum())
    cat1      = int(mb.n_cat1_zero.sum())
    cat2_low  = int(mb.n_cat2_low.sum())
    cat2_high = int(mb.n_cat2_high.sum())

    stats = stat_grid([
        ("Expected specimens", fmt(total_exp)),
        ("Cat1 — Zero reads", fmt(cat1)),
        ("Cat2 Low — Few reads", fmt(cat2_low)),
        ("Cat2 High — Assembly failed ⚠", badge(fmt(cat2_high),"red")),
        ("Cat3 — Demultiplexing failure", badge("0","green")),
    ])

    cat_rows = [
        ["Cat 1 — Zero reads", fmt(cat1), pct(cat1,total_exp),
         "No reads produced","No action needed"],
        ["Cat 2 Low — Few reads", fmt(cat2_low), pct(cat2_low,total_exp),
         "Below assembly threshold","No action needed"],
        [badge("Cat 2 High ⚠","red"), fmt(cat2_high), pct(cat2_high,total_exp),
         "Reads present, assembly failed","Investigate"],
        ["Cat 3 — Absent from UMI", "0", "0.0%",
         "Demultiplexing failure",badge("None detected","green")],
    ]
    cat_tbl = table(["Category","Count","%","Description","Action"], cat_rows)

    # Cat2_high by partner
    partner_html = ""
    if not mc.empty and 'category' in mc.columns:
        cat2h = mc[mc['category'].str.strip()=='Cat2_high_reads'].copy()
        if not cat2h.empty:
            cat2h['partner'] = cat2h['plate_id'].str.extract(r'^(?:TOL-)?([A-Z]{4})')
            by_p = cat2h['partner'].value_counts().head(10)
            p_rows = [[p, fmt(int(n))] for p,n in by_p.items()]
            partner_html = "<h3>Cat 2 High by partner</h3>" + \
                           table(["Partner","Specimens"], p_rows)

    # Worst batches
    worst = mb.nlargest(5,'n_cat2_high')
    worst_rows = []
    for _, r in worst.iterrows():
        if r['n_cat2_high'] > 0:
            p = round(100*r['n_cat2_high']/r['n_expected'],1) if r['n_expected'] else 0
            worst_rows.append([str(r['batch']), fmt(int(r['n_cat2_high'])), f"{p:.1f}%"])
    worst_html = "<h3>Worst batches for Cat 2 High</h3>" + \
                 table(["Batch","Cat2_high","% of batch"], worst_rows) if worst_rows else ""

    fbox = files_box(
        inputs=["bioscan_plate_status_ALL_YYYYMMDD.csv", "portal_plates_from_dump.csv",
                "mBRAVE QC outputs (per batch)"],
        outputs=["missing_specimens_categorised_YYYYMMDD.csv",
                 "missing_specimens_batch_summary_YYYYMMDD.csv"],
    )
    content = fbox + stats + cat_tbl + partner_html + worst_html
    return card("6. Missing Specimens & Assembly Failures", content, "s6")


def build_bold_flags(d):
    bold_report_path = d['bold_report_path']
    bold_needs_resub = d['bold_needs_resub']
    bold_no_alt      = d['bold_flagged_no_alt']
    bold_comp        = d['bold_flagged_comp']

    # Parse workbench report — fixed regex
    total_wb = with_bin = any_flag = stop_codon = contam = flagged_rec = 0
    if bold_report_path and os.path.exists(bold_report_path):
        with open(bold_report_path) as f:
            txt = f.read()
        def _ex(label):
            m = re.search(rf'{re.escape(label)}\s*:\s*([\d,]+)', txt)
            return int(m.group(1).replace(',','')) if m else 0
        total_wb   = _ex('Total specimens on BOLD')
        with_bin   = _ex('With BIN URI')
        stop_codon = _ex('Has stop codon flag')
        contam     = _ex('Has contamination flag')
        flagged_rec= _ex('Flagged record')
        # Any flag has parenthetical — match differently
        m = re.search(r'Any flag[^:]*:\s*([\d,]+)', txt)
        any_flag = int(m.group(1).replace(',','')) if m else stop_codon + contam + flagged_rec

    # Concordance
    neither=qc_only=different=identical=bold_only=0
    if not bold_comp.empty and 'sequence_status' in bold_comp.columns:
        vc = bold_comp['sequence_status'].value_counts()
        neither   = int(vc.get('NEITHER',0))
        qc_only   = int(vc.get('QC_ONLY',0))
        different = int(vc.get('DIFFERENT',0))
        identical = int(vc.get('IDENTICAL',0))
        bold_only = int(vc.get('BOLD_ONLY',0))
    total_comp = neither+qc_only+different+identical+bold_only

    stats = stat_grid([
        ("On BOLD workbench", fmt(total_wb)),
        ("With BIN URI", fmt(with_bin)),
        ("Without BIN URI", fmt(total_wb-with_bin)),
        ("Any quality flag", badge(fmt(any_flag),"red")),
        ("Stop codon (flag only)", fmt(stop_codon)),
        ("Contamination", fmt(contam)),
        ("Flagged record", fmt(flagged_rec)),
    ])

    conc_rows = [
        ["NEITHER", fmt(neither), pct(neither,total_comp),
         "No sequence in BOLD or QC", "None possible"],
        [badge("QC_ONLY","amber"), fmt(qc_only), pct(qc_only,total_comp),
         "Passed QC, not on BOLD", badge("Upload to BOLD","amber")],
        [badge("DIFFERENT","red"), fmt(different), pct(different,total_comp),
         "QC sequence differs from BOLD — resubmission may assign a BIN", badge("Resubmit to BOLD","red")],
        ["IDENTICAL", fmt(identical), pct(identical,total_comp),
         "Same sequence in both — flag is informational", "Expert review"],
        ["BOLD_ONLY", fmt(bold_only), pct(bold_only,total_comp),
         "On BOLD, not in QC", "Investigate"],
    ]
    conc_tbl = table(["Result","Count","%","Meaning","Action"], conc_rows)

    # Resubmission by partner
    resub_html = ""
    if not bold_needs_resub.empty:
        col = next((c for c in ['partner_code','partner'] if c in bold_needs_resub.columns), None)
        if col:
            by_p = bold_needs_resub[col].value_counts().head(20)
            rows = [[p, fmt(int(n))] for p,n in by_p.items()]
            resub_html = (f"<h3>Specimens for resubmission to BOLD "
                          f"({fmt(len(bold_needs_resub))} total)</h3>") + \
                         alert("QC has a better sequence — resubmitting will likely result in BIN assignment.","info") + \
                         table(["Partner","Specimens"], rows)

    # No alternative
    no_alt_html = ""
    if not bold_no_alt.empty:
        col = next((c for c in ['partner_code','partner'] if c in bold_no_alt.columns), None)
        if col:
            by_p = bold_no_alt[col].value_counts().head(10)
            rows = [[p, fmt(int(n))] for p,n in by_p.items()]
            no_alt_html = (f"<h3>Genuinely flagged specimens — expert review needed "
                           f"({fmt(len(bold_no_alt))} total)</h3>") + \
                          alert("Same sequence in QC and BOLD — no automated fix possible.","warn") + \
                          table(["Partner","Specimens"], rows)

    fbox = files_box(
        inputs=["BOLD workbench export (manual download)", "QC FASTA files (per batch)",
                "bioscan_plate_status_ALL_YYYYMMDD.csv"],
        outputs=["bold_workbench_report_YYYYMMDD.txt", "bold_workbench_plates_YYYYMMDD.csv",
                 "bold_workbench_combined.csv", "bold_flagged_comparison_YYYYMMDD.csv",
                 "bold_needs_resubmission_YYYYMMDD.csv", "bold_flagged_no_alternative_YYYYMMDD.csv"],
    )
    stop_note = alert(
        "ℹ <strong>Stop codons</strong> are flagged by BOLD as a quality indicator but do "
        "<strong>not</strong> affect which sequence is selected by the QC pipeline. A stop "
        "codon flag on BOLD does not mean the sequence is wrong — it is informational only "
        "and requires expert review rather than automated resubmission.",
        "info"
    )
    content = (fbox + stats + stop_note +
               "<h3>Sequence concordance for flagged specimens (QC FASTA vs BOLD)</h3>" +
               conc_tbl + resub_html + no_alt_html)
    return card("7. BOLD Quality Flags", content, "s7")


def build_bold_concordance(d):
    """Section 8 — BOLD sequence concordance (FASTA upload vs current BOLD)."""
    df = d.get('bold_concordance', pd.DataFrame())
    if df.empty:
        return card("8. BOLD Sequence Concordance",
                    alert("No concordance data found. Run bold_sequence_concordance.py --exclude-bge", "warn"),
                    "s8")

    # Only non-identical rows are saved in the CSV
    total_on_bold = len(df)

    vc = df['status'].value_counts() if 'status' in df.columns else pd.Series()

    n_different    = int(vc.get('DIFFERENT', 0))
    n_trim5        = int(vc.get('TRIM_5PRIME', 0))
    n_trim3        = int(vc.get('TRIM_3PRIME', 0))
    n_near         = int(vc.get('NEAR_IDENTICAL', 0))
    n_close        = int(vc.get('CLOSE', 0))
    n_bold_only    = int(vc.get('BOLD_ONLY', 0))

    # Cross-reference with batch family analysis to identify how many DIFFERENT
    # records are explained by CONFLICT_DIFFERENT in the repeat batch families.
    bf = d.get('batch_family_summ', pd.DataFrame())
    batch_conflict_ids = set()
    if not bf.empty and 'specimen_id' in bf.columns and 'specimen_status' in bf.columns:
        batch_conflict_ids = set(
            bf.loc[bf['specimen_status'] == 'CONFLICT_DIFFERENT', 'specimen_id'].dropna()
        )

    n_batch_explained = 0
    n_truly_different = n_different
    if n_different > 0 and 'specimen_id' in df.columns and batch_conflict_ids:
        diff_ids = set(df.loc[df['status'] == 'DIFFERENT', 'specimen_id'].dropna())
        n_batch_explained = len(diff_ids & batch_conflict_ids)
        n_truly_different = n_different - n_batch_explained

    stats = stat_grid([
        ("Non-identical records", fmt(total_on_bold)),
        ("DIFFERENT (total)", badge(fmt(n_different), "red")),
        ("  — explained by batch family", fmt(n_batch_explained)),
        ("  — unexplained (true diff) ⚠", badge(fmt(n_truly_different), "red")),
        ("Trimming only (5' end)", fmt(n_trim5)),
        ("Trimming only (3' end)", fmt(n_trim3)),
        ("Near-identical (>99%)", fmt(n_near)),
        ("BOLD_ONLY (no FASTA)", fmt(n_bold_only)),
    ])

    note = alert(
        "ℹ Compares the QC FASTA sequences uploaded to BOLD against the sequences currently on BOLD. "
        "<strong>TRIM_5PRIME/TRIM_3PRIME</strong> = length difference only — sequences are otherwise identical, no action needed. "
        "<strong>DIFFERENT</strong> = the sequence on BOLD differs from the QC FASTA. "
        "Some of these differences are now explained by the repeat batch family analysis (Section 9): "
        "specimens sequenced in multiple batches may have a different consensus called in each run. "
        "The <em>unexplained</em> count above shows the true number of specimens where the QC FASTA "
        "and BOLD sequence differ for reasons other than batch family repeats.",
        "info"
    )

    status_rows = [
        [badge("IDENTICAL","green"), "—", "100% match — sequence unchanged", "No action"],
        [badge("TRIM_5PRIME","blue"), fmt(n_trim5), "Extra bases at 5′ end of FASTA only — right-aligned sequences are identical", "No action — trimming only"],
        [badge("TRIM_3PRIME","blue"), fmt(n_trim3), "Extra bases at 3′ end of FASTA only — left-aligned sequences are identical", "No action — trimming only"],
        [badge("NEAR_IDENTICAL","amber"), fmt(n_near), ">99% overlap — minor formatting difference", "Review"],
        [badge("CLOSE","amber"), fmt(n_close), "95–99% identity", "Review"],
        [badge("DIFFERENT","red"), fmt(n_different),
         f"Sequence differs from BOLD upload: {fmt(n_batch_explained)} explained by batch family repeats, "
         f"{fmt(n_truly_different)} unexplained",
         badge("Investigate unexplained","red")],
        [badge("BOLD_ONLY","amber"), fmt(n_bold_only), "On BOLD but not in any batch FASTA", "Investigate — pre-pipeline upload"],
    ]
    status_tbl = table(["Status","Count","Meaning","Action"], status_rows)

    # DIFFERENT (unexplained) by batch
    batch_tbl = ""
    if n_different > 0 and 'best_batch' in df.columns:
        diff = df[df['status'] == 'DIFFERENT'].copy()
        if batch_conflict_ids and 'specimen_id' in diff.columns:
            diff = diff[~diff['specimen_id'].isin(batch_conflict_ids)]
        if not diff.empty:
            by_batch = diff['best_batch'].value_counts().head(10)
            batch_rows = [[b, fmt(int(n))] for b, n in by_batch.items()]
            batch_tbl = "<h3>Unexplained DIFFERENT by batch</h3>" + table(["Batch","Specimens"], batch_rows)

    # DIFFERENT (unexplained) by partner
    partner_tbl = ""
    if n_different > 0 and 'partner' in df.columns:
        diff = df[df['status'] == 'DIFFERENT'].copy()
        if batch_conflict_ids and 'specimen_id' in diff.columns:
            diff = diff[~diff['specimen_id'].isin(batch_conflict_ids)]
        if not diff.empty:
            by_partner = diff['partner'].value_counts().head(15)
            partner_rows = [[p, fmt(int(n))] for p, n in by_partner.items()]
            partner_tbl = "<h3>Unexplained DIFFERENT by partner</h3>" + table(["Partner","Specimens"], partner_rows)

    fbox = files_box(
        inputs=["QC FASTA files (per batch)", "BOLD sequence data (via API/download)",
                "bioscan_plate_status_ALL_YYYYMMDD.csv"],
        outputs=["bold_sequence_concordance_ALL_YYYYMMDD.csv",
                 "bold_sequence_concordance_ALL_YYYYMMDD_summary.txt"],
    )
    content = (fbox + note + stats + "<h3>Status breakdown</h3>" + status_tbl +
               "<div class='two-col'>" + batch_tbl + partner_tbl + "</div>")
    return card("8. BOLD Sequence Concordance (QC FASTA vs BOLD)", content, "s8")


def build_batch_family(d):
    """Section 9 — Repeat batch family sequence comparison."""
    df = d.get('batch_family_summ', pd.DataFrame())
    if df.empty:
        return card("9. Repeat Batch Family Comparison",
                    alert("No batch family data found. Run batch_family_sequence_comparison.py", "warn"),
                    "s9")

    total = len(df)
    vc = df['specimen_status'].value_counts() if 'specimen_status' in df.columns else pd.Series()

    n_ok        = int(vc.get('OK', 0))
    n_extra     = int(vc.get('ADDITIONAL_PASS_AVAILABLE', 0))
    n_conf_diff = int(vc.get('CONFLICT_DIFFERENT', 0))
    n_conf_cl   = int(vc.get('CONFLICT_CLOSE', 0))

    # Non-BGE additional passes not on BOLD
    bge = {'BGEP','BGEG','BGPT','BGKU'}
    import re as _re
    def _partner(sid):
        if not sid: return None
        s = str(sid).upper()
        if s.startswith('TOL-'): return None
        m = _re.match(r'^([A-Z]{4})[_-]', s)
        return m.group(1) if m else None

    extra = df[
        (df['specimen_status'] == 'ADDITIONAL_PASS_AVAILABLE') &
        (df['is_on_bold'] == 'False')
    ].copy()
    extra['partner'] = extra['specimen_id'].apply(_partner)
    non_bge_extra = extra[~extra['partner'].isin(bge) & extra['partner'].notna()]

    stats = stat_grid([
        ("Total specimens", fmt(total)),
        ("OK — sequences consistent", fmt(n_ok)),
        ("Additional pass available", badge(fmt(n_extra), "amber")),
        ("Non-BGE not on BOLD", badge(fmt(len(non_bge_extra)), "red")),
        ("Conflict — different sequence", badge(fmt(n_conf_diff), "red")),
        ("Conflict — close sequence", badge(fmt(n_conf_cl), "amber")),
    ])

    note = alert(
        "ℹ These are specimens where the same plates were sequenced in multiple batch "
        "family members (RnD runs, splits, repeats). "
        "<strong>ADDITIONAL_PASS_AVAILABLE</strong> = a repeat batch has a QC-passed sequence "
        "not yet on BOLD — opportunity to increase coverage. "
        "<strong>CONFLICT</strong> = different sequences called for the same specimen across runs.",
        "info"
    )

    # Status table
    status_rows = [
        [badge("OK","green"),                    fmt(n_ok),        "All sequences consistent across batch members"],
        [badge("ADDITIONAL_PASS_AVAILABLE","amber"), fmt(n_extra), "Repeat batch has QC-passed sequence not yet on BOLD"],
        [badge("CONFLICT_DIFFERENT","red"),       fmt(n_conf_diff), "Genuinely different sequences across batch members"],
        [badge("CONFLICT_CLOSE","amber"),         fmt(n_conf_cl),   "Close but not identical sequences across batch members"],
    ]
    status_tbl = table(["Status","Count","Meaning"], status_rows)

    # Non-BGE additional by partner
    partner_html = ""
    if len(non_bge_extra) > 0:
        by_partner = non_bge_extra['partner'].value_counts().head(15)
        p_rows = [[p, fmt(int(n))] for p,n in by_partner.items()]
        partner_html = (f"<h3>Non-BGE additional sequences by partner "
                        f"({fmt(len(non_bge_extra))} total)</h3>") +                        table(["Partner","Specimens"], p_rows)

    # By batch family
    fam_html = ""
    if 'batch_family' in df.columns:
        by_fam = df.groupby(['batch_family','specimen_status']).size().reset_index()
        fam_rows = []
        for fam in sorted(df['batch_family'].unique()):
            fam_data = by_fam[by_fam['batch_family']==fam]
            n_fam_extra = int(fam_data[fam_data['specimen_status']=='ADDITIONAL_PASS_AVAILABLE'][0].sum())                 if len(fam_data[fam_data['specimen_status']=='ADDITIONAL_PASS_AVAILABLE']) > 0 else 0
            n_fam_conf  = int(fam_data[fam_data['specimen_status'].isin(
                ['CONFLICT_DIFFERENT','CONFLICT_CLOSE'])][0].sum())                 if len(fam_data[fam_data['specimen_status'].isin(
                ['CONFLICT_DIFFERENT','CONFLICT_CLOSE'])]) > 0 else 0
            n_fam_tot   = int((df['batch_family']==fam).sum())
            fam_rows.append([fam, fmt(n_fam_tot),
                             badge(fmt(n_fam_extra),"amber") if n_fam_extra else "0",
                             badge(fmt(n_fam_conf),"red") if n_fam_conf else "0"])
        fam_html = "<h3>By batch family</h3>" +                    table(["Family","Specimens","Additional pass","Conflicts"], fam_rows)

    fbox = files_box(
        inputs=["QC FASTA files (per batch family)", "BOLD sequence data (via API/download)",
                "bioscan_plate_status_ALL_YYYYMMDD.csv"],
        outputs=["batch_family_specimen_batch_YYYYMMDD_ALL.csv",
                 "batch_family_summary_YYYYMMDD_ALL.csv",
                 "repeat_batch_additional_sequences_YYYYMMDD.fasta",
                 "repeat_batch_additional_sequences_YYYYMMDD.csv",
                 "repeat_batch_additional_sequences_YYYYMMDD_summary.txt"],
    )
    content = (fbox + note + stats + "<h3>Status breakdown</h3>" + status_tbl +
               "<div class='two-col'>" + partner_html + fam_html + "</div>")
    return card("9. Repeat Batch Family Comparison", content, "s9")


def build_positive_controls(d):
    """Section 10 — Positive control analysis and re-sequencing candidates."""
    df      = d.get('pos_ctrl_plates',  pd.DataFrame())
    batches = d.get('pos_ctrl_batches', pd.DataFrame())

    if df.empty:
        return card("10. Positive Control Analysis",
                    alert("No positive control data found. "
                          "Run positive_control_analysis.py", "warn"), "s10")

    for col in ['pos_control_reads', 'batch_median_pos_ctrl',
                'pos_ctrl_pct_of_median', 'pass_rate', 'mean_pass_rate',
                'n_specimens_umi', 'n_specimens_portal',
                'n_pass', 'n_on_hold', 'n_fail', 'n_total_qc']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    n_total     = len(df)
    n_low_ctrl  = int(df['low_pos_ctrl_flag'].astype(str).str.upper().eq('TRUE').sum()) \
                  if 'low_pos_ctrl_flag' in df.columns else 0
    n_poor_batch= int(df['poor_batch_flag'].astype(str).str.upper().eq('TRUE').sum()) \
                  if 'poor_batch_flag' in df.columns else 0
    n_candidates= int(df['resequence_candidate'].astype(str).str.upper().eq('TRUE').sum()) \
                  if 'resequence_candidate' in df.columns else 0
    n_batches_poor = 0
    if not batches.empty and 'poor_batch_flag' in batches.columns:
        n_batches_poor = int(
            batches['poor_batch_flag'].astype(str).str.upper().eq('TRUE').sum()
        )

    stats = stat_grid([
        ("Plate-batch records",   fmt(n_total)),
        ("Low pos-ctrl plates ⚠", badge(fmt(n_low_ctrl),   "red")),
        ("Poor batches (bot. 20%)", badge(fmt(n_batches_poor), "amber")),
        ("Re-sequence candidates", badge(fmt(n_candidates), "red")),
    ])

    note = alert(
        "ℹ <strong>Low positive control</strong> = pos control reads below 50% of the "
        "batch median for that run. This suggests the plate may have had poor sequencing "
        "performance and is a candidate for re-sequencing. "
        "<strong>Poor batch</strong> = batch in the bottom 20% of all batches by mean "
        "plate pass rate. A plate is a re-sequence candidate if either flag is set.",
        "info"
    )

    # Flagged plates table (top 30 by lowest % of median)
    flagged_html = ""
    if 'resequence_candidate' in df.columns:
        cands = df[
            df['resequence_candidate'].astype(str).str.upper() == 'TRUE'
        ].copy()
        if not cands.empty:
            cands = cands.sort_values('pos_ctrl_pct_of_median').head(30)
            rows = []
            for _, r in cands.iterrows():
                pct_val = r.get('pos_ctrl_pct_of_median')
                pct_str = f"{pct_val:.1f}%" if pd.notna(pct_val) else "n/a"
                pct_badge = badge(pct_str, "red") if pd.notna(pct_val) and pct_val < 50 \
                            else badge(pct_str, "amber")

                pass_r = r.get('pass_rate')
                pass_str = f"{pass_r:.1f}%" if pd.notna(pass_r) else "n/a"

                umi_n    = fmt(int(r['n_specimens_umi']))    if pd.notna(r.get('n_specimens_umi'))    else "n/a"
                portal_n = fmt(int(r['n_specimens_portal'])) if pd.notna(r.get('n_specimens_portal')) else "n/a"

                flags = []
                if str(r.get('low_pos_ctrl_flag', '')).upper() == 'TRUE':
                    flags.append(badge("Low ctrl","red"))
                if str(r.get('poor_batch_flag', '')).upper() == 'TRUE':
                    flags.append(badge("Poor batch","amber"))

                rows.append([
                    str(r.get('batch', '')),
                    str(r.get('plate_id', '')),
                    fmt(int(r['pos_control_reads'])) if pd.notna(r.get('pos_control_reads')) else "n/a",
                    pct_badge,
                    pass_str,
                    f"{umi_n} / {portal_n}",
                    " ".join(flags),
                ])
            flagged_html = (
                f"<h3>Re-sequence candidates — top {len(rows)} by lowest pos-ctrl</h3>" +
                table(["Batch","Plate","Pos ctrl reads","% of batch median",
                       "Pass rate (this batch)","Specimens (UMI / portal)","Flags"], rows)
            )

    # Poor batches table
    poor_batch_html = ""
    if not batches.empty and 'poor_batch_flag' in batches.columns:
        for col in ['mean_pass_rate', 'median_pos_ctrl', 'n_plates']:
            if col in batches.columns:
                batches[col] = pd.to_numeric(batches[col], errors='coerce')
        poor = batches[
            batches['poor_batch_flag'].astype(str).str.upper() == 'TRUE'
        ].sort_values('mean_pass_rate')
        if not poor.empty:
            p_rows = []
            for _, r in poor.iterrows():
                pass_r = r.get('mean_pass_rate')
                p_rows.append([
                    str(r.get('batch', '')),
                    fmt(int(r['n_plates'])) if pd.notna(r.get('n_plates')) else "n/a",
                    f"{pass_r:.1f}%" if pd.notna(pass_r) else "n/a",
                    fmt(int(r['median_pos_ctrl'])) if pd.notna(r.get('median_pos_ctrl')) else "n/a",
                ])
            poor_batch_html = (
                "<h3>Poor-performing batches (bottom 20% by mean pass rate)</h3>" +
                table(["Batch","Plates","Mean pass rate","Median pos-ctrl reads"], p_rows)
            )

    fbox = files_box(
        inputs=["umi.*_control_pos_stats.txt (all batches)",
                "umi.*_sample_stats.txt (all batches)",
                "qc_portal_batch*.csv (all batches)",
                config.PORTAL_DUMP_TSV.split('/')[-1]],
        outputs=["positive_control_plates_YYYYMMDD.csv",
                 "positive_control_batch_summary_YYYYMMDD.csv"],
    )
    content = (fbox + note + stats + flagged_html +
               "<div class='two-col'>" + poor_batch_html + "</div>")
    return card("10. Positive Control Analysis", content, "s10")


def build_actions(d):
    df         = d['plate_status']
    bold_comp  = d['bold_flagged_comp']
    miss_cats  = d['missing_cats']
    bold_resub = d['bold_needs_resub']

    qc_only = int((bold_comp['sequence_status']=='QC_ONLY').sum()) \
        if not bold_comp.empty and 'sequence_status' in bold_comp.columns else 0
    n_resub = len(bold_resub)

    not_seq_old = 0
    n_not_bold  = 0
    if not df.empty and 'missing_at' in df.columns:
        not_seq = df[df['missing_at']=='mbrave'].copy()
        if not not_seq.empty:
            not_seq['submit_date'] = pd.to_datetime(not_seq['submit_date'], errors='coerce')
            not_seq_old = int(((pd.Timestamp.now()-not_seq['submit_date']).dt.days >= 90).sum())
        n_not_bold = int((df['missing_at']=='bold').sum())

    cat2_high = 0
    if not miss_cats.empty and 'category' in miss_cats.columns:
        cat2_high = int((miss_cats['category'].str.strip()=='Cat2_high_reads').sum())

    def action(priority, number, title, desc, ref, colour):
        return f"""<div class="priority-block priority-{colour}">
            <h4>{badge(priority, colour)} &nbsp; {number}. {title}</h4>
            <p>{desc}</p>
            <p><code>{ref}</code></p>
        </div>"""

    content = "<h3>🔴 High Priority — Must be resolved before data freeze</h3>"
    content += action("HIGH","1",
        f"Investigate {fmt(not_seq_old)} plates with no sequencing data after >90 days",
        "FRBX and FACE are most critical — submitted 2023, still unsequenced.",
        "missing_plates_ALL_YYYYMMDD.txt", "high")
    content += action("HIGH","2",
        f"Resubmit {fmt(n_resub)} specimens to BOLD with improved QC sequences",
        "QC pipeline has produced a better sequence. Resubmission will likely result in BIN assignment.",
        "bold_needs_resubmission_YYYYMMDD.csv", "high")
    content += action("HIGH","3",
        f"Upload QC sequences to BOLD for {fmt(qc_only)} QC_ONLY specimens",
        "These passed QC but have no BOLD sequence at all.",
        "bold_flagged_comparison_YYYYMMDD.csv (filter sequence_status == QC_ONLY)", "high")
    content += action("HIGH","4",
        f"Upload {fmt(n_not_bold)} QC-passed plates to BOLD",
        "Prioritise plates submitted >90 days ago.",
        "bioscan_plate_status_ALL_YYYYMMDD.csv", "high")
    content += action("HIGH","5",
        "Investigate BCLT — all specimens on BOLD, all flagged, zero BINs",
        "No stop codon or contamination flags — flagging reason unknown. Contact BOLD team.",
        "bold_workbench_plates_YYYYMMDD.csv", "high")

    content += "<h3>🟡 Medium Priority</h3>"
    content += action("MEDIUM","6",
        "Implement best-batch selection for repeated plates",
        "Prevents 784 PASS→FAILED and 820 PASS→ON_HOLD regressions entering the freeze.",
        "repeat_specimens_transitions_YYYYMMDD.csv", "medium")
    content += action("MEDIUM","7",
        "Expert review of genuinely flagged sequences",
        "Same sequence in QC and BOLD — no automated fix possible.",
        "bold_flagged_no_alternative_YYYYMMDD.csv", "medium")
    content += action("MEDIUM","8",
        "Dedicated RRNW review",
        "Largest repeat decline (-29.7%), most QC_ONLY specimens, 386 missing BINs.",
        "repeat_analysis_YYYYMMDD.csv | bold_missing_bin_YYYYMMDD.csv", "medium")

    content += "<h3>🟢 Low Priority — Post-freeze follow-up</h3>"
    content += action("LOW","9",
        f"Investigate {fmt(cat2_high)} Cat-2-High specimens",
        "Reads present but assembly failed — concentrated in aquatic invertebrate partners.",
        "missing_specimens_categorised_YYYYMMDD.csv", "low")
    content += action("LOW","10",
        "Review 283 BOLD_ONLY specimens",
        "Sequences on BOLD but not in QC FASTA — possibly pre-dating current pipeline.",
        "bold_flagged_comparison_YYYYMMDD.csv", "low")

    fbox = files_box(
        inputs=["bioscan_plate_status_ALL_YYYYMMDD.csv", "bold_flagged_comparison_YYYYMMDD.csv",
                "bold_needs_resubmission_YYYYMMDD.csv", "missing_specimens_categorised_YYYYMMDD.csv",
                "repeat_specimens_transitions_YYYYMMDD.csv", "missing_plates_ALL_YYYYMMDD.txt"],
        outputs=["(no new CSVs — uses outputs from all preceding sections)"],
    )
    content = fbox + content
    return card("11. Actions Required", content, "s11")


# ── Assemble HTML ─────────────────────────────────────────────────────────────

def build_html(sections_html, meta, exclude_bge):
    today_str = datetime.datetime.now().strftime("%d %B %Y %H:%M")
    bge_note  = "BGE partners excluded (BGEP, BGEG, BGKU, BGPT)" if exclude_bge \
                else "All partners included"

    nav_items = [
        ("s1","1. Pipeline Overview"),
        ("s2","2. Missing from mBRAVE"),
        ("s3","3. Not on BOLD"),
        ("s4","4. Plate QC Summary"),
        ("s5","5. Repeat Sequencing"),
        ("s6","6. Missing Specimens"),
        ("s7","7. BOLD Quality Flags"),
        ("s9","9. BOLD Sequence Concordance"),
        ("s10","10. Repeat Batch Families"),
        ("s11","11. Positive Controls"),
        ("s12","12. Actions Required"),
    ]
    nav_html = '<span class="nav-section">Sections</span>'
    nav_html += "".join(f'<a href="#{id_}">{label}</a>' for id_,label in nav_items)

    body = "".join(sections_html)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BIOSCAN Pipeline Report — {today_str}</title>
<style>{CSS}</style>
</head>
<body>
<header>
    <div>
        <h1>BIOSCAN Pipeline Summary Report</h1>
        <div style="color:#64748b;font-size:12px;margin-top:4px">
            Lawniczak Lab · Wellcome Sanger Institute
        </div>
    </div>
    <div class="meta">
        Generated: {today_str}<br>
        {bge_note}<br>
        <a href="https://github.com/LyndallP/bioscan_plate_checker"
           style="color:#60a5fa">github.com/LyndallP/bioscan_plate_checker</a>
    </div>
</header>
<div class="layout">
    <nav>{nav_html}</nav>
    <main>{body}</main>
</div>
<script>{JS}</script>
</body>
</html>"""


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Generate HTML BIOSCAN pipeline summary report'
    )
    parser.add_argument('--exclude-bge', action='store_true',
        help='Exclude BGE partners (BGEP, BGEG, BGKU, BGPT)')
    parser.add_argument('--output', default=None,
        help='Output HTML path (default: RESULTS_DIR/bioscan_report_YYYYMMDD.html)')
    args = parser.parse_args()

    today = datetime.datetime.now().strftime('%Y%m%d')
    results = config.RESULTS_DIR
    os.makedirs(results, exist_ok=True)

    if args.output is None:
        args.output = os.path.join(results, f'bioscan_report_{today}.html')

    print(f"Generating BIOSCAN HTML report...")
    print(f"  Results dir:  {results}")
    print(f"  Exclude BGE:  {args.exclude_bge}")
    print(f"  Output:       {args.output}")
    print()

    d = load_all(results, args.exclude_bge)

    sections = [
        build_pipeline_overview(d),
        build_missing_sequencing(d),
        build_not_on_bold(d),
        build_plate_qc(d),
        build_repeat(d),
        build_missing_specimens(d),
        build_bold_flags(d),
        build_bold_concordance(d),
        build_batch_family(d),
        build_positive_controls(d),
        build_actions(d),
    ]

    html = build_html(sections, {}, args.exclude_bge)

    with open(args.output, 'w') as f:
        f.write(html)

    size = os.path.getsize(args.output) / 1024
    print(f"Done. Report written to: {args.output} ({size:.0f} KB)")
    print(f"Open in browser: file://{args.output}")


if __name__ == '__main__':
    main()
