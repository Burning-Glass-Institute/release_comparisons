# rev_version_comparison_app.py
# Streamlit dashboard: BGI v1 vs v2 vs Lightcast postings comparison
# Reads pre-computed tables from PROJECT_DATA.POSTINGS_RELEASE_COMPARISONS (BGI_REL_* prefix)
# ─────────────────────────────────────────────────────────────────────────

import pandas as pd
import altair as alt
import streamlit as st
from snowflake.snowpark import Session

st.set_page_config(
    page_title="Postings Release Comparison",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────── CONSTANTS ─────────────────────────────
SCHEMA = "PROJECT_DATA.POSTINGS_RELEASE_COMPARISONS"
TABLE_PREFIX = "BGI_REL"

_SOURCE_ORDER = ["v1", "v2", "lc"]
_SOURCE_LABELS = {"v1": "BGI v1", "v2": "BGI 2026-02", "lc": "Lightcast"}

# BGI palette: red, blue, orange
_RED = "#C22036"
_BLUE = "#03497A"
_ORANGE = "#E0732B"
_GOLD = "#C68C0A"
THREE_COLOR_SCALE = [_RED, _BLUE, _ORANGE]

_DARK_GREY = "#333333"
NULL_LABEL = "<NULL>"

# ─────────────────────────── TABLE NAME HELPERS ─────────────────────────────
def _clean(s: str) -> str:
    return s.upper().replace(" ", "_").replace("-", "_").replace("'", "")

def make_kpi_name(topic: str, field: str) -> str:
    return f"{TABLE_PREFIX}_KPI_{_clean(topic)}_{_clean(field)}"

def make_comp_name(topic: str, field: str) -> str:
    return f"{TABLE_PREFIX}_COMP_{_clean(topic)}_{_clean(field)}"

def make_comp_lc_name(topic: str, field: str) -> str:
    return f"{TABLE_PREFIX}_COMPLC_{_clean(topic)}_{_clean(field)}"

# ─────────────────────────── TOPIC METADATA ─────────────────────────────
TOPICS = {
    "dashboard information": {"fields": []},
    "total counts": {"fields": [], "kind": "totals"},
    "employers": {
        "fields": ["EMPLOYER_NAME"],
        "display_name": "Employers",
    },
    "industry_naics2": {
        "fields": ["NAICS2_DISTRIBUTION"],
        "display_name": "Industry (NAICS-2)",
    },
    "education": {
        "fields": ["MIN_REQUIRED_DEGREE"],
        "display_name": "Education",
    },
    "occupation": {
        "fields": ["ONET_NAME", "SOC2"],
        "display_name": "Occupation",
    },
    "location": {
        "fields": ["CITY", "STATE", "MSA"],
        "display_name": "Location",
    },
    "titles": {
        "fields": ["TITLE_NAME"],
        "display_name": "Titles",
    },
}

# ─────────────────────────── AXIS / LEGEND HELPERS ───────────────────────
_Y_AXIS = dict(
    title=None,
    axis=alt.Axis(
        labelLimit=350,
        labelColor=_DARK_GREY,
        titleColor=_DARK_GREY,
        labelFontSize=13,
        labelFont="Verdana",
    ),
)

def _x_axis(fmt=None, title=None):
    kw = {"labelColor": _DARK_GREY, "titleColor": _DARK_GREY}
    if fmt is not None:
        kw["format"] = fmt
    if title is not None:
        kw["title"] = title
    return alt.Axis(**kw)

_LEGEND = alt.Legend(labelColor=_DARK_GREY, titleColor=_DARK_GREY)

_COLOR_ENC = alt.Color(
    "source:N",
    title="Source",
    sort=_SOURCE_ORDER,
    scale=alt.Scale(domain=_SOURCE_ORDER, range=THREE_COLOR_SCALE),
    legend=_LEGEND,
)

# ───────────────────────── SNOWFLAKE CONNECTION ─────────────────────────
@st.cache_resource(show_spinner=False, ttl="24h")
def get_session() -> Session:
    from snowflake.snowpark.context import get_active_session
    try:
        active = get_active_session()
        if active:
            return active
    except Exception:
        pass
    cfg = dict(
        account="PCA67849",
        user="JULIA_NANIA",
        role="ANALYST",
        warehouse="WH_3_XS",
        database="PROJECT_DATA",
        schema="POSTINGS_RELEASE_COMPARISONS",
        authenticator="externalbrowser",
    )
    return Session.builder.configs(cfg).create()

session = get_session()

# ─────────────────────────── QUERY HELPER ─────────────────────────────
def _query(table_name: str) -> pd.DataFrame:
    df = session.sql(f"SELECT * FROM {SCHEMA}.{table_name}").to_pandas()
    df.columns = [c.lower() for c in df.columns]
    return df

# ─────────────────────────── CHART HELPERS ─────────────────────────────
def _prepare(val):
    if isinstance(val, pd.Series):
        return val.fillna(NULL_LABEL).astype(str)
    return NULL_LABEL if pd.isna(val) else str(val)


def _melt(df, value_cols, value_name):
    df_disp = df.copy()
    df_disp["field_value"] = _prepare(df_disp["field_value"])
    return df_disp.melt(
        id_vars="field_value",
        value_vars=value_cols,
        var_name="source",
        value_name=value_name,
    )


def chart_line_counts(df, x_field, title, x_fmt=None):
    chart = (
        alt.Chart(df, title=title)
        .mark_line(point=True)
        .encode(
            x=alt.X(x_field, axis=_x_axis(fmt=x_fmt)),
            y=alt.Y("cnt:Q", axis=_x_axis(title="Count")),
            color=_COLOR_ENC,
            tooltip=[
                "source",
                alt.Tooltip(x_field, title="Period"),
                alt.Tooltip("cnt:Q", title="Count", format=","),
            ],
        )
        .properties(height=350)
    )
    st.altair_chart(chart, use_container_width=True)


def chart_bars(df, value_col, title, fmt=",", sort_col=None):
    """Grouped horizontal bar chart for 3 sources."""
    sort_col = sort_col or "v2_count"
    order = (
        df.sort_values(sort_col, ascending=False)["field_value"]
        .apply(_prepare)
        .tolist()
    )
    long = df.rename(columns={
        "v1_count": "v1", "v2_count": "v2", "lc_count": "lc",
        "v1_frac": "v1", "v2_frac": "v2", "lc_frac": "lc",
    })
    long = _melt(long, _SOURCE_ORDER, value_col)

    x_fmt = ".1%" if "frac" in sort_col or "pct" in value_col else None
    chart = (
        alt.Chart(long, title=title)
        .mark_bar()
        .encode(
            y=alt.Y("field_value:N", sort=order, **_Y_AXIS),
            yOffset=alt.YOffset("source:N", sort=_SOURCE_ORDER),
            x=alt.X(f"{value_col}:Q", axis=_x_axis(fmt=x_fmt, title=value_col.title()), stack=None),
            color=_COLOR_ENC,
            tooltip=[
                "field_value",
                "source",
                alt.Tooltip(f"{value_col}:Q", format=fmt),
            ],
        )
        .properties(height=700)
    )
    st.altair_chart(chart, use_container_width=True)


def chart_pct_diff(df, title, col1, col2, label):
    df_disp = df.copy()
    df_disp["field_value"] = _prepare(df_disp["field_value"])
    df_disp["pct_point_diff"] = df_disp[col1] - df_disp[col2]
    order = (
        df_disp.sort_values("pct_point_diff", key=lambda s: s.abs(), ascending=False)
        ["field_value"].tolist()
    )
    chart = (
        alt.Chart(df_disp, title=title)
        .mark_bar()
        .encode(
            y=alt.Y("field_value:N", sort=order, **_Y_AXIS),
            x=alt.X("pct_point_diff:Q", axis=_x_axis(fmt=".1%", title=label)),
            color=alt.condition(
                alt.datum.pct_point_diff > 0, alt.value(_BLUE), alt.value(_RED)
            ),
            tooltip=[
                "field_value",
                alt.Tooltip("pct_point_diff:Q", format=".2%"),
                alt.Tooltip(f"{col1}:Q", format=".2%", title=col1.replace("_frac", " %")),
                alt.Tooltip(f"{col2}:Q", format=".2%", title=col2.replace("_frac", " %")),
            ],
        )
        .properties(height=600)
    )
    st.altair_chart(chart, use_container_width=True)


# ─────────────────────────── SECTION: LANDING ──────────────────────────
def page_landing():
    st.header("Postings Release Comparison")
    st.markdown("""
    Compare **BGI v1**, **BGI 2026-02 (v2)**, and **Lightcast** job postings data.

    **Topics available:**
    Employers, Industry (NAICS-2), Education, Occupation, Location, Titles

    For each topic you'll see **coverage KPIs**, **distribution shares**,
    **raw counts**, and **%-point differences** across all three sources.
    """)
    st.info("Pick a topic on the left to get started.")


# ─────────────────────────── SECTION: TOTAL COUNTS ─────────────────────
def page_total_counts():
    st.header("Total Counts")

    # Yearly
    try:
        yearly = _query(f"{TABLE_PREFIX}_TOTAL_COUNTS_YEARLY")
        yearly = yearly.rename(columns={"yr": "year"}).sort_values(["source", "year"])
        st.subheader("Yearly totals — v1 vs v2 vs Lightcast")
        chart_line_counts(yearly, "year:Q", "Total Postings by Year")
    except Exception as e:
        st.error(f"Error loading yearly data: {e}")

    st.divider()

    # Monthly
    try:
        monthly = _query(f"{TABLE_PREFIX}_TOTAL_COUNTS_MONTHLY")
        monthly = monthly.sort_values(["source", "month_start"])
        st.subheader("Monthly totals — last 12 months")
        chart_line_counts(monthly, "month_start:T", "Total Postings by Month (Last 12 Months)", x_fmt="%b %y")
    except Exception as e:
        st.error(f"Error loading monthly data: {e}")

    st.divider()

    # JOLTS monthly benchmark
    show_jolts_monthly()


# ─────────────────────────── SECTION: KPI + COMP ──────────────────────
def show_kpis(topic, field):
    """Display coverage KPIs for one field."""
    kpi_table = make_kpi_name(topic, field)
    try:
        kpi = _query(kpi_table)
        if kpi.empty:
            st.info("No KPI data returned.")
            return
        row = kpi.iloc[0]

        cols = st.columns(6)
        for i, src in enumerate(["v1", "v2", "lc"]):
            total = int(row[f"total_{src}"])
            covered = int(row[f"covered_{src}"])
            pct = covered / total if total else 0
            cols[i * 2].metric(f"Total · {_SOURCE_LABELS[src]}", f"{total:,}")
            cols[i * 2 + 1].metric(f"Coverage · {_SOURCE_LABELS[src]}", f"{pct:.1%}")
    except Exception as e:
        st.error(f"Error loading KPI: {e}")
        st.info(f"Expected table: {kpi_table}")


def show_comp(topic, field):
    """Display distribution charts + diffs for one field."""
    comp_table = make_comp_name(topic, field)
    try:
        df = _query(comp_table)
        if df.empty:
            st.info("No comparison data returned.")
            return

        if "field_value" not in df.columns:
            df = df.rename(columns={df.columns[0]: "field_value"})

        # Top 25 by v2 fraction
        df_top = df.sort_values("v2_frac", ascending=False).head(25)

        # Percentage chart
        pct_df = df_top.rename(columns={"v1_frac": "v1", "v2_frac": "v2", "lc_frac": "lc"})
        pct_long = _melt(pct_df, _SOURCE_ORDER, "pct")
        order = df_top["field_value"].apply(_prepare).tolist()
        pct_chart = (
            alt.Chart(pct_long, title="Top 25 — Percentage")
            .mark_bar()
            .encode(
                y=alt.Y("field_value:N", sort=order, **_Y_AXIS),
                yOffset=alt.YOffset("source:N", sort=_SOURCE_ORDER),
                x=alt.X("pct:Q", axis=_x_axis(fmt=".1%", title="Percentage"), stack=None),
                color=_COLOR_ENC,
                tooltip=["field_value", "source", alt.Tooltip("pct:Q", format=".1%")],
            )
            .properties(height=700)
        )
        st.altair_chart(pct_chart, use_container_width=True)

        # Count chart
        cnt_df = df_top.rename(columns={"v1_count": "v1", "v2_count": "v2", "lc_count": "lc"})
        cnt_long = _melt(cnt_df, _SOURCE_ORDER, "count")
        cnt_chart = (
            alt.Chart(cnt_long, title="Top 25 — Counts")
            .mark_bar()
            .encode(
                y=alt.Y("field_value:N", sort=order, **_Y_AXIS),
                yOffset=alt.YOffset("source:N", sort=_SOURCE_ORDER),
                x=alt.X("count:Q", axis=_x_axis(title="Count"), stack=None),
                color=_COLOR_ENC,
                tooltip=["field_value", "source", alt.Tooltip("count:Q", format=",")],
            )
            .properties(height=700)
        )
        st.altair_chart(cnt_chart, use_container_width=True)

        # %-point diffs: three pairwise comparisons
        st.markdown("#### Percentage Point Differences")
        col_a, col_b, col_c = st.columns(3)

        pairs = [
            (col_a, "v1 − v2",  "v1_frac", "v2_frac"),
            (col_b, "v1 − LC",  "v1_frac", "lc_frac"),
            (col_c, "v2 − LC",  "v2_frac", "lc_frac"),
        ]
        for col, label, c1, c2 in pairs:
            with col:
                diff = (
                    df.assign(abs_diff=lambda d, a=c1, b=c2: (d[a] - d[b]).abs())
                    .sort_values("abs_diff", ascending=False)
                    .head(15)
                )
                if not diff.empty:
                    chart_pct_diff(diff, label, c1, c2, f"{label} (pct pts)")

    except Exception as e:
        st.error(f"Error loading comparison: {e}")
        st.info(f"Expected table: {comp_table}")


def show_comp_lc(topic, field):
    """Top 25 by Lightcast count, with v1/v2 alongside."""
    complc_table = make_comp_lc_name(topic, field)
    try:
        df = _query(complc_table)
        if df.empty:
            st.info("No LC comparison data returned.")
            return

        if "field_value" not in df.columns:
            df = df.rename(columns={df.columns[0]: "field_value"})

        order = df.sort_values("lc_count", ascending=False)["field_value"].apply(_prepare).tolist()

        # Percentage chart
        pct_df = df.rename(columns={"v1_frac": "v1", "v2_frac": "v2", "lc_frac": "lc"})
        pct_long = _melt(pct_df, _SOURCE_ORDER, "pct")
        pct_chart = (
            alt.Chart(pct_long, title="Top 25 Lightcast Employers — Percentage")
            .mark_bar()
            .encode(
                y=alt.Y("field_value:N", sort=order, **_Y_AXIS),
                yOffset=alt.YOffset("source:N", sort=_SOURCE_ORDER),
                x=alt.X("pct:Q", axis=_x_axis(fmt=".1%", title="Percentage"), stack=None),
                color=_COLOR_ENC,
                tooltip=["field_value", "source", alt.Tooltip("pct:Q", format=".1%")],
            )
            .properties(height=700)
        )
        st.altair_chart(pct_chart, use_container_width=True)

        # Count chart
        cnt_df = df.rename(columns={"v1_count": "v1", "v2_count": "v2", "lc_count": "lc"})
        cnt_long = _melt(cnt_df, _SOURCE_ORDER, "count")
        cnt_chart = (
            alt.Chart(cnt_long, title="Top 25 Lightcast Employers — Counts")
            .mark_bar()
            .encode(
                y=alt.Y("field_value:N", sort=order, **_Y_AXIS),
                yOffset=alt.YOffset("source:N", sort=_SOURCE_ORDER),
                x=alt.X("count:Q", axis=_x_axis(title="Count"), stack=None),
                color=_COLOR_ENC,
                tooltip=["field_value", "source", alt.Tooltip("count:Q", format=",")],
            )
            .properties(height=700)
        )
        st.altair_chart(cnt_chart, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading LC comparison: {e}")
        st.info(f"Expected table: {complc_table}")


def show_onet_changes():
    """ONET classification changes between v1 and v2."""
    st.markdown("#### ONET Classification Changes: v1 → v2 (2024)")

    # Summary
    try:
        summary = _query(f"{TABLE_PREFIX}_ONET_CHANGE_SUMMARY")
        if not summary.empty:
            row = summary.iloc[0]
            total = int(row["total_matched"])
            same = int(row["same_onet"])
            diff = int(row["different_onet"])
            cols = st.columns(4)
            cols[0].metric("Total Matched", f"{total:,}")
            cols[1].metric("Same ONET", f"{same:,}")
            cols[2].metric("Changed ONET", f"{diff:,}")
            cols[3].metric("% Changed", f"{diff / total:.1%}" if total else "N/A")
    except Exception as e:
        st.warning(f"Could not load ONET change summary: {e}")

    # Top changes
    try:
        changes = _query(f"{TABLE_PREFIX}_ONET_CHANGES")
        if not changes.empty:
            st.dataframe(
                changes.style.format({"posting_count": "{:,.0f}"}),
                use_container_width=True,
                height=400,
            )
    except Exception as e:
        st.warning(f"Could not load ONET changes: {e}")


# ─────────────────────────── BENCHMARK CHART HELPERS ─────────────────────
def _bench_color(domain):
    return alt.Color(
        "source:N", title="Source", sort=domain,
        scale=alt.Scale(domain=domain, range=[_GOLD, _RED, _BLUE]),
        legend=_LEGEND,
    )

_BENCH_ORDER = ["JOLTS", "BGI", "Lightcast"]
_OEWS_ORDER = ["OEWS", "BGI", "Lightcast"]


def show_jolts_monthly():
    """JOLTS monthly openings vs BGI vs Lightcast line chart."""
    st.subheader("Benchmark: Monthly JOLTS Openings vs BGI vs Lightcast")
    try:
        jm = _query("JOLTS_MONTHLY_COMPARISON")
        jm["source"] = jm["source"].replace(
            {"Full_BGI": "BGI", "Full_LC": "Lightcast"})
        jm = jm.sort_values(["source", "month_start"])
        chart = (
            alt.Chart(jm, title="Monthly Postings vs JOLTS Job Openings")
            .mark_line(point=True)
            .encode(
                x=alt.X("month_start:T", axis=_x_axis(fmt="%b %Y")),
                y=alt.Y("cnt:Q", axis=_x_axis(title="Count")),
                color=_bench_color(_BENCH_ORDER),
                tooltip=[
                    "source",
                    alt.Tooltip("month_start:T", title="Month"),
                    alt.Tooltip("cnt:Q", title="Count", format=","),
                ],
            )
            .properties(height=400)
        )
        st.altair_chart(chart, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading JOLTS monthly: {e}")


def show_jolts_industry():
    """JOLTS vs BGI vs Lightcast industry distribution bar chart."""
    st.subheader("Benchmark: JOLTS vs BGI vs Lightcast Industry Distribution (2024)")
    try:
        ji = _query("JOLTS_INDUSTRY_COMPARISON")
        ji_long = ji[["sector", "jolts_pct", "bgi_pct", "lc_pct"]].melt(
            id_vars="sector", var_name="source", value_name="pct")
        ji_long["source"] = ji_long["source"].replace(
            {"jolts_pct": "JOLTS", "bgi_pct": "BGI", "lc_pct": "Lightcast"})
        order = ji.sort_values("jolts_count", ascending=False)["sector"].tolist()
        chart = (
            alt.Chart(ji_long, title="Industry Share (%)")
            .mark_bar()
            .encode(
                y=alt.Y("sector:N", sort=order, **_Y_AXIS),
                yOffset=alt.YOffset("source:N", sort=_BENCH_ORDER),
                x=alt.X("pct:Q", axis=_x_axis(fmt=".1%", title="Share"),
                         stack=None),
                color=_bench_color(_BENCH_ORDER),
                tooltip=["sector", "source",
                         alt.Tooltip("pct:Q", format=".1%")],
            )
            .properties(height=600)
        )
        st.altair_chart(chart, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading JOLTS industry: {e}")


def show_oews_soc2():
    """OEWS employment vs BGI vs Lightcast SOC-2 distribution bar chart."""
    st.subheader("Benchmark: OEWS Employment vs BGI vs Lightcast (SOC-2)")
    try:
        oe = _query("OEWS_SOC2_COMPARISON")
        oe["soc2_name"] = oe["soc2_name"].str.title()
        oe_long = oe[["soc2_name", "oews_pct", "bgi_pct", "lc_pct"]].melt(
            id_vars="soc2_name", var_name="source", value_name="pct")
        oe_long["source"] = oe_long["source"].replace(
            {"oews_pct": "OEWS", "bgi_pct": "BGI", "lc_pct": "Lightcast"})
        order = oe.sort_values("oews_empl", ascending=False)["soc2_name"].tolist()
        chart = (
            alt.Chart(oe_long,
                      title="SOC-2: Postings Share vs OEWS Employment Share (%)")
            .mark_bar()
            .encode(
                y=alt.Y("soc2_name:N", sort=order, **_Y_AXIS),
                yOffset=alt.YOffset("source:N", sort=_OEWS_ORDER),
                x=alt.X("pct:Q", axis=_x_axis(fmt=".1%", title="Share"),
                         stack=None),
                color=_bench_color(_OEWS_ORDER),
                tooltip=["soc2_name", "source",
                         alt.Tooltip("pct:Q", format=".1%")],
            )
            .properties(height=700)
        )
        st.altair_chart(chart, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading OEWS SOC2: {e}")


_STATE_SOURCE_ORDER = ["JOLTS", "v1", "v2", "Lightcast"]
_STATE_COLORS = [_RED, _ORANGE, _BLUE, _GOLD]


def show_jolts_state():
    """JOLTS vs v1 vs v2 vs Lightcast state distribution charts (2024)."""
    st.subheader("Benchmark: State Share of Postings vs JOLTS Job Openings (2024)")
    try:
        js = _query(f"{TABLE_PREFIX}_JOLTS_STATE_COMPARISON")
        if js.empty:
            st.info("No JOLTS state data returned.")
            return

        # ── Grouped bar chart: top 15 states by JOLTS share (descending) ──
        top15 = js.nlargest(15, "jolts_pct").sort_values("jolts_pct", ascending=False)
        top15["state"] = top15["state"].str.title()
        bar_long = top15[["state", "jolts_pct", "v1_pct", "v2_pct", "lc_pct"]].melt(
            id_vars="state", var_name="source", value_name="pct")
        bar_long["source"] = bar_long["source"].replace({
            "jolts_pct": "JOLTS", "v1_pct": "v1", "v2_pct": "v2", "lc_pct": "Lightcast"})
        order = top15["state"].tolist()
        state_color = alt.Color(
            "source:N", title="Source", sort=_STATE_SOURCE_ORDER,
            scale=alt.Scale(domain=_STATE_SOURCE_ORDER, range=_STATE_COLORS),
            legend=_LEGEND,
        )
        bar_chart = (
            alt.Chart(bar_long, title="Top 15 States: Share of Postings vs JOLTS Job Openings")
            .mark_bar()
            .encode(
                y=alt.Y("state:N", sort=order, **_Y_AXIS),
                yOffset=alt.YOffset("source:N", sort=_STATE_SOURCE_ORDER),
                x=alt.X("pct:Q", axis=_x_axis(title="Share (%)"), stack=None),
                color=state_color,
                tooltip=["state", "source", alt.Tooltip("pct:Q", format=".1f")],
            )
            .properties(height=600)
        )
        st.altair_chart(bar_chart, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading JOLTS state comparison: {e}")


# ─────────────────────────── MAIN APP ──────────────────────────────────
def main():
    st.title("Postings Release Comparison: v1 vs v2 vs Lightcast")

    topic_options = list(TOPICS.keys())

    def format_topic(t):
        meta = TOPICS.get(t, {})
        return meta.get("display_name", t.replace("_", " ").title())

    topic = st.sidebar.selectbox("Topic:", topic_options, index=0, format_func=format_topic)

    if topic == "dashboard information":
        page_landing()
        return

    if topic == "total counts":
        page_total_counts()
        return

    # ── Field selector ──
    st.sidebar.markdown("---")
    topic_meta = TOPICS[topic]
    all_fields = topic_meta.get("fields", [])

    fields = st.sidebar.multiselect(
        "Fields:",
        all_fields,
        default=[all_fields[0]] if all_fields else [],
    )

    display_name = topic_meta.get("display_name", topic.replace("_", " ").title())
    st.caption(
        f"Topic **{display_name}** — comparing BGI v1, BGI 2026-02, and Lightcast (2015+).",
        unsafe_allow_html=True,
    )

    # ── Per-field sections ──
    for field in fields:
        with st.container():
            st.subheader(f"{display_name} · {field}")
            show_kpis(topic, field)
            show_comp(topic, field)
            if topic == "employers":
                show_comp_lc(topic, field)
            st.divider()

    # ── JOLTS industry benchmark (industry topic only) ──
    if topic == "industry_naics2":
        show_jolts_industry()

    # ── JOLTS state benchmark (location topic only) ──
    if topic == "location":
        show_jolts_state()

    # ── OEWS SOC-2 benchmark (occupation topic only) ──
    if topic == "occupation":
        show_oews_soc2()


# ───────────────────────── RUN APP ─────────────────────────────────────
if __name__ == "__main__":
    main()
