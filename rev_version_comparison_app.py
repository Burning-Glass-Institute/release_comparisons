# rev_version_comparison_app.py
# Streamlit dashboard to compare BGI vs Lightcast postings using pre-computed temp tables
# Now shows 4 sources: Overlap_BGI, Overlap_LC, Full_BGI, Full_LC
# ─────────────────────────────────────────────────────────────────────────

import pandas as pd
import altair as alt
import streamlit as st
from snowflake.snowpark import Session

st.set_page_config(
    page_title="Postings: BGI vs Lightcast Comparison",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────── TABLE NAME HELPERS ─────────────────────────────
def clean_name(s: str) -> str:
    return s.upper().replace(" ", "_").replace("-", "_").replace("'", "")

def make_comparison_table_name(topic: str, field: str) -> str:
    return f"COMP_{clean_name(topic)}_{clean_name(field)}"

def make_kpi_table_name(topic: str, field: str) -> str:
    return f"KPI_{clean_name(topic)}_{clean_name(field)}"

# Pre-computed tables schema
SCHEMA = "PROJECT_DATA.POSTINGS_RELEASE_COMPARISONS"

# ─────────────────────────── COLOR / AXIS HELPERS ───────────────────────────
_BLACK = "#000000"
_DARK_GREY = "#333333"

_Y_AXIS = dict(
    title=None,
    axis=alt.Axis(
        labelLimit=350,
        labelColor=_DARK_GREY,
        titleColor=_DARK_GREY,
        labelFontSize=13,
        labelFont="Verdana"
    ),
)

def _x_axis(fmt: str | None = None, title: str | None = None) -> alt.Axis:
    axis_kwargs: dict[str, object] = {
        "labelColor": _DARK_GREY,
        "titleColor": _DARK_GREY
    }
    if fmt is not None:
        axis_kwargs["format"] = fmt
    if title is not None:
        axis_kwargs["title"] = title
    return alt.Axis(**axis_kwargs)

_LEGEND = alt.Legend(labelColor=_DARK_GREY, titleColor=_DARK_GREY)

# ───────────────────────── CUSTOM COLOR PALETTE ────────────────────────────
_RED = "#C22036"
_ORANGE = "#A44914"
_TAN = "#C68C0A"
_BLUE = "#03497A"
_LIGHT_BLUE = "#729ACB"
_LIGHT_RED = "#D15868"

# 4-color scale for Overlap_BGI, Overlap_LC, Full_BGI, Full_LC
FOUR_COLOR_SCALE = [_BLUE, _RED, _LIGHT_BLUE, _LIGHT_RED]

NULL_LABEL = "<NULL>"
_SOURCE_ORDER = ["Overlap_BGI", "Overlap_LC", "Full_BGI", "Full_LC"]

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

# ───────────────────────── TOPIC METADATA ─────────────────────────────
TOPICS = {
    "dashboard information": {"fields": {}},
    "total counts": {"fields": {}, "kind": "totals"},
    "industry_naics2": {
        "fields": ["NAICS2_DISTRIBUTION"],
        "display_name": "Industry (NAICS-2)"
    },
    "education": {
        "fields": ["MIN_REQUIRED_DEGREE"],
        "display_name": "Education"
    },
    "occupation": {
        "fields": ["ONET_NAME", "SOC2"],
        "display_name": "Occupation"
    },
    "location": {
        "fields": ["CITY", "STATE", "MSA"],
        "display_name": "Location"
    },
    "titles": {
        "fields": ["TITLE_NAME"],
        "display_name": "Titles"
    },
}

# ─────────────────────────── CHART HELPERS ─────────────────────────────
def _prepare(val):
    if isinstance(val, pd.Series):
        return val.fillna(NULL_LABEL).astype(str)
    return NULL_LABEL if pd.isna(val) else str(val)

def _melt(df: pd.DataFrame, value_cols: list[str], value_name: str) -> pd.DataFrame:
    df_disp = df.copy()
    df_disp["field_value"] = _prepare(df_disp["field_value"])
    return df_disp.melt(
        id_vars="field_value",
        value_vars=value_cols,
        var_name="source",
        value_name=value_name
    )

def chart_line_counts(df: pd.DataFrame, x_field: str, title: str, x_fmt: str | None = None):
    chart = (
        alt.Chart(df, title=title)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{x_field}", axis=_x_axis(fmt=x_fmt)),
            y=alt.Y("cnt:Q", axis=_x_axis(title="Count")),
            color=alt.Color(
                "source:N",
                title="Source",
                sort=_SOURCE_ORDER,
                scale=alt.Scale(domain=_SOURCE_ORDER, range=FOUR_COLOR_SCALE),
                legend=_LEGEND,
            ),
            strokeDash=alt.StrokeDash(
                "source:N",
                sort=_SOURCE_ORDER,
                scale=alt.Scale(
                    domain=_SOURCE_ORDER,
                    range=[[1, 0], [1, 0], [4, 4], [4, 4]]  # solid for overlap, dashed for full
                ),
                legend=None
            ),
            tooltip=["source", alt.Tooltip(x_field, title="Period"), alt.Tooltip("cnt:Q", title="Count", format=",")]
        )
        .properties(height=350)
    )
    st.altair_chart(chart, use_container_width=True)

def chart_counts(df: pd.DataFrame, title: str):
    """Bar chart for raw counts with 4 sources"""
    order = (
        df.sort_values("full_bgi_count", ascending=False)["field_value"]
          .apply(_prepare).tolist()
    )
    # Rename columns for display
    long = df.rename(columns={
        "overlap_bgi_count": "Overlap_BGI",
        "overlap_lc_count": "Overlap_LC",
        "full_bgi_count": "Full_BGI",
        "full_lc_count": "Full_LC"
    })
    long = _melt(long, ["Overlap_BGI", "Overlap_LC", "Full_BGI", "Full_LC"], "count")

    chart = (
        alt.Chart(long, title=title)
        .mark_bar()
        .encode(
            y=alt.Y("field_value:N", sort=order, **_Y_AXIS),
            yOffset=alt.YOffset("source:N", sort=_SOURCE_ORDER),
            x=alt.X("count:Q", axis=_x_axis(title="Count"), stack=None),
            color=alt.Color(
                "source:N",
                title="Source",
                sort=_SOURCE_ORDER,
                scale=alt.Scale(domain=_SOURCE_ORDER, range=FOUR_COLOR_SCALE),
                legend=_LEGEND,
            ),
            tooltip=["field_value", "source", alt.Tooltip("count:Q", format=",")],
        )
        .properties(height=700)
    )
    st.altair_chart(chart, use_container_width=True)

def chart_percentages(df: pd.DataFrame, title: str):
    """Bar chart for percentages with 4 sources"""
    order = (
        df.sort_values("full_bgi_frac", ascending=False)["field_value"]
          .apply(_prepare).tolist()
    )
    long = df.rename(columns={
        "overlap_bgi_frac": "Overlap_BGI",
        "overlap_lc_frac": "Overlap_LC",
        "full_bgi_frac": "Full_BGI",
        "full_lc_frac": "Full_LC"
    })
    long = _melt(long, ["Overlap_BGI", "Overlap_LC", "Full_BGI", "Full_LC"], "pct")

    chart = (
        alt.Chart(long, title=title)
        .mark_bar()
        .encode(
            y=alt.Y("field_value:N", sort=order, **_Y_AXIS),
            yOffset=alt.YOffset("source:N", sort=_SOURCE_ORDER),
            x=alt.X("pct:Q", axis=_x_axis(fmt=".1%", title="Percentage"), stack=None),
            color=alt.Color(
                "source:N",
                title="Source",
                sort=_SOURCE_ORDER,
                scale=alt.Scale(domain=_SOURCE_ORDER, range=FOUR_COLOR_SCALE),
                legend=_LEGEND,
            ),
            tooltip=["field_value", "source", alt.Tooltip("pct:Q", format=".1%")],
        )
        .properties(height=700)
    )
    st.altair_chart(chart, use_container_width=True)

def chart_pct_diff(df: pd.DataFrame, title: str, col1: str, col2: str, label: str):
    """Bar chart of %-point difference between two sources"""
    df_disp = df.copy()
    df_disp["field_value"] = _prepare(df_disp["field_value"])
    df_disp["pct_point_diff"] = (df_disp[col1] - df_disp[col2])
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
            color=alt.condition(alt.datum.pct_point_diff > 0, alt.value(_BLUE), alt.value(_RED)),
            tooltip=[
                "field_value",
                alt.Tooltip("pct_point_diff:Q", format=".1%"),
                alt.Tooltip(f"{col1}:Q", format=".1%", title=col1.replace("_frac", " %")),
                alt.Tooltip(f"{col2}:Q", format=".1%", title=col2.replace("_frac", " %")),
            ],
        )
        .properties(height=600)
    )
    st.altair_chart(chart, use_container_width=True)

# ─────────────────────────── MAIN APP ──────────────────────────────────
def main():
    st.title("Postings: BGI vs Lightcast Comparison")

    # Topic selector with display names
    topic_options = list(TOPICS.keys())
    def format_topic(t):
        meta = TOPICS.get(t, {})
        return meta.get("display_name", t.replace("_", " ").title())
    
    topic = st.sidebar.selectbox("Topic:", topic_options, index=0, format_func=format_topic)

    # Landing page
    if topic == "dashboard information":
        st.header("BGI vs Lightcast Postings — Release Comparison")
        st.markdown(
            """
            Explore differences between **BGI** and **Lightcast** job postings data.

            **Four data sources are compared:**
            - **Overlap_BGI**: BGI postings that exist in the LC_RL_MAY25_XWALK crosswalk (all years)
            - **Overlap_LC**: Lightcast postings that exist in the LC_RL_MAY25_XWALK crosswalk (all years)
            - **Full_BGI**: All BGI postings from 2015+ (no crosswalk filter)
            - **Full_LC**: All Lightcast postings from 2015+ (no crosswalk filter)

            **Topics available:**
            - Industry (NAICS-2), Education, Occupation, Location, Titles
            
            For each topic, you'll see **totals**, **coverage**, **distribution shares**, 
            **raw counts**, and **%-point differences**.
            """
        )
        st.info("Pick a topic on the left to get started.")
        return

    # Total Counts topic
    if topic == "total counts":
        st.header("Total Counts")

        # Yearly totals
        try:
            yearly_sql = f"SELECT * FROM {SCHEMA}.TOTAL_COUNTS_YEARLY"
            yearly_df = session.sql(yearly_sql).to_pandas()
            if not yearly_df.empty:
                yearly_df.columns = [c.lower() for c in yearly_df.columns]
                yearly_df = (yearly_df
                             .rename(columns={"yr": "year"})
                             .sort_values(["source", "year"]))
                st.subheader("Yearly totals — All 4 Sources")
                chart_line_counts(
                    yearly_df,
                    x_field="year:Q",
                    title="Total Postings by Year",
                )
                st.caption("Solid lines = Overlap (crosswalk-matched), Dashed lines = Full dataset")
        except Exception as e:
            st.error(f"Error loading yearly data: {e}")
            st.info("Expected table: TOTAL_COUNTS_YEARLY")

        st.divider()

        # Monthly totals
        try:
            monthly_sql = f"SELECT * FROM {SCHEMA}.TOTAL_COUNTS_MONTHLY"
            monthly_df = session.sql(monthly_sql).to_pandas()
            if monthly_df.empty:
                st.info("No monthly data returned.")
            else:
                monthly_df.columns = [c.lower() for c in monthly_df.columns]
                st.subheader("Monthly totals — last 12 months")
                monthly_df = monthly_df.sort_values("month_start")
                chart_line_counts(
                    monthly_df,
                    x_field="month_start:T",
                    title="Total Postings by Month (Last 12 Months)",
                    x_fmt="%b %y"
                )
                st.caption("Solid lines = Overlap (crosswalk-matched), Dashed lines = Full dataset")
        except Exception as e:
            st.error(f"Error loading monthly data: {e}")
            st.info("Expected table: TOTAL_COUNTS_MONTHLY")

        return

    # Field selector for other topics
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
        f"Topic **{display_name}**. "
        "Comparing Overlap (crosswalk-matched, all years) vs Full datasets (2015+) for BGI and Lightcast.",
        unsafe_allow_html=True,
    )

    # Per-field sections
    for field in fields:
        with st.container():
            st.subheader(f"{display_name} · Field: {field}")

            # Query pre-computed KPI table
            kpi_table = make_kpi_table_name(topic, field)
            kpi_sql = f"SELECT * FROM {SCHEMA}.{kpi_table}"

            try:
                kpi_df = session.sql(kpi_sql).to_pandas()
                if kpi_df.empty:
                    st.info("No KPI data returned.")
                    st.divider()
                    continue

                row = kpi_df.iloc[0]
                # Extract all 8 values
                total_overlap_bgi = int(row["TOTAL_OVERLAP_BGI"])
                covered_overlap_bgi = int(row["COVERED_OVERLAP_BGI"])
                total_overlap_lc = int(row["TOTAL_OVERLAP_LC"])
                covered_overlap_lc = int(row["COVERED_OVERLAP_LC"])
                total_full_bgi = int(row["TOTAL_FULL_BGI"])
                covered_full_bgi = int(row["COVERED_FULL_BGI"])
                total_full_lc = int(row["TOTAL_FULL_LC"])
                covered_full_lc = int(row["COVERED_FULL_LC"])

                # Calculate coverage percentages
                overlap_bgi_cov = covered_overlap_bgi / total_overlap_bgi if total_overlap_bgi else 0
                overlap_lc_cov = covered_overlap_lc / total_overlap_lc if total_overlap_lc else 0
                full_bgi_cov = covered_full_bgi / total_full_bgi if total_full_bgi else 0
                full_lc_cov = covered_full_lc / total_full_lc if total_full_lc else 0

                # Display KPI metrics in two rows
                st.markdown("**Overlap (crosswalk-matched)**")
                cols1 = st.columns(4)
                cols1[0].metric("Total · Overlap_BGI", f"{total_overlap_bgi:,}")
                cols1[1].metric("Total · Overlap_LC", f"{total_overlap_lc:,}")
                cols1[2].metric("Coverage · Overlap_BGI", f"{overlap_bgi_cov:.1%}")
                cols1[3].metric("Coverage · Overlap_LC", f"{overlap_lc_cov:.1%}")

                st.markdown("**Full datasets (no crosswalk filter)**")
                cols2 = st.columns(4)
                cols2[0].metric("Total · Full_BGI", f"{total_full_bgi:,}")
                cols2[1].metric("Total · Full_LC", f"{total_full_lc:,}")
                cols2[2].metric("Coverage · Full_BGI", f"{full_bgi_cov:.1%}")
                cols2[3].metric("Coverage · Full_LC", f"{full_lc_cov:.1%}")

            except Exception as e:
                st.error(f"Error loading KPI data: {e}")
                st.info(f"Expected table: {kpi_table}")

            # Query pre-computed comparison table
            comp_table = make_comparison_table_name(topic, field)
            comparison_sql = f"SELECT * FROM {SCHEMA}.{comp_table}"

            try:
                df = session.sql(comparison_sql).to_pandas()
                df.columns = [c.lower() for c in df.columns]

                if df.empty:
                    st.info("No data returned.")
                    st.divider()
                    continue

                if "field_value" not in df.columns:
                    value_col = df.columns[0]
                    df = df.rename(columns={value_col: "field_value"})

                # Top-k and charts
                df_top = df.sort_values("full_bgi_frac", ascending=False).head(25)
                chart_percentages(df_top, "Top 25 — Percentage (All 4 Sources)")
                chart_counts(df_top, "Top 25 — Counts (All 4 Sources)")

                # %-point differences - show two comparisons
                st.markdown("#### Percentage Point Differences")
                
                col_a, col_b = st.columns(2)
                
                with col_a:
                    # Overlap: BGI vs LC
                    df_diff_overlap = (
                        df.assign(abs_diff=lambda d: (d["overlap_bgi_frac"] - d["overlap_lc_frac"]).abs())
                          .sort_values("abs_diff", ascending=False)
                          .head(15)
                    )
                    if not df_diff_overlap.empty:
                        chart_pct_diff(
                            df_diff_overlap, 
                            "Overlap: BGI − LC",
                            "overlap_bgi_frac", 
                            "overlap_lc_frac",
                            "Overlap_BGI − Overlap_LC (pct pts)"
                        )

                with col_b:
                    # Full: BGI vs LC
                    df_diff_full = (
                        df.assign(abs_diff=lambda d: (d["full_bgi_frac"] - d["full_lc_frac"]).abs())
                          .sort_values("abs_diff", ascending=False)
                          .head(15)
                    )
                    if not df_diff_full.empty:
                        chart_pct_diff(
                            df_diff_full, 
                            "Full: BGI − LC",
                            "full_bgi_frac", 
                            "full_lc_frac",
                            "Full_BGI − Full_LC (pct pts)"
                        )

            except Exception as e:
                st.error(f"Error loading comparison data: {e}")
                st.info(f"Expected table: {comp_table}")

            st.divider()

# ───────────────────────── RUN APP ─────────────────────────────────────
if __name__ == "__main__":
    main()
