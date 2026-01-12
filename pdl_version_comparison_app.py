# pdl_version_comparison_app.py
# Streamlit dashboard to compare PDL v5 vs v5_OCT25 data using pre-computed temp tables
#
# ─────────────────────────────────────────────────────────────────────────
import pandas as pd
import altair as alt
import streamlit as st
from snowflake.snowpark import Session

st.set_page_config(
    page_title="PDL v5 - v5_OCT25 Comparison Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────── VERSION CONFIG ───────────────────────────

BASE_VERSION = "v5"
NEW_VERSION = "v5_OCT25"


def _alias(version: str) -> str:
    """Safe SQL identifier-ish alias for CTEs/columns."""
    return version.lower().replace("-", "_").replace(" ", "_")


def _label(version: str) -> str:
    """Display label in charts."""
    return version


# ─────────────────────────── TABLE NAME HELPERS ─────────────────────────────
def clean_name(s: str) -> str:
    """Convert to valid SQL identifier."""
    return s.upper().replace(" ", "_").replace("-", "_").replace("'", "")


def make_comparison_table_name(topic: str, field: str, country: str) -> str:
    """Generate table name for comparison query results."""
    return f"COMP_{clean_name(topic)}_{clean_name(field)}_{clean_name(country)}"


def make_kpi_table_name(topic: str, field: str, country: str) -> str:
    """Generate table name for KPI query results."""
    return f"KPI_{clean_name(topic)}_{clean_name(field)}_{clean_name(country)}"


# ─────────────────────────── COLOR / AXIS HELPERS ───────────────────────────

_DARK_GREY = "#000000"

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


def _x_axis(fmt: str | None = None, title: str | None = None) -> alt.Axis:
    axis_kwargs: dict[str, object] = {
        "labelColor": _DARK_GREY,
        "titleColor": _DARK_GREY,
    }
    if fmt is not None:
        axis_kwargs["format"] = fmt
    if title is not None:
        axis_kwargs["title"] = title
    return alt.Axis(**axis_kwargs)


_LEGEND = alt.Legend(labelColor=_DARK_GREY, titleColor=_DARK_GREY)

# ───────────────────────── CUSTOM 5-COLOR PALETTE ────────────────────────────

_RED = "#C22036"
_ORANGE = "#A44914"
_TAN = "#C68C0A"
_BLUE = "#03497A"
_BLACK = "#000000"

# Ordered list of five colors: red, orange, tan, blue, black
FIVE_COLOR_SCALE = [_RED, _ORANGE, _TAN, _BLUE, _BLACK]

COUNTRY_OPTIONS = [
    "United States",
    "United Kingdom",
    "Singapore",
    "Canada",
    "Australia",
    "New Zealand",
    "Switzerland",
    "Hong Kong",
    "China",
]
NULL_LABEL = "<NULL>"

_VERSION_ORDER_DEFAULT = [NEW_VERSION, BASE_VERSION]
_VERSION_ORDER_IPEDS = [NEW_VERSION, BASE_VERSION, "ipeds"]
_VERSION_ORDER_BLS = [NEW_VERSION, BASE_VERSION, "BLS 2023"]

# Roles benchmark ordering (include external benchmarks)
_VERSION_ORDER_ROLES = [NEW_VERSION, "OEWS", "ACS"]

_DEGREE_ORDER = [
    "High School",
    "Certificate",
    "Associate",
    "Bachelor's Degree",
    "Master's Degree",
    "Doctorate",
]

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
        schema="PDL_RELEASE_COMPARISONS_V5_V5_OCT25",
        authenticator="externalbrowser",
    )
    return Session.builder.configs(cfg).create()


session = get_session()

# ───────────────────────── TOPIC METADATA ─────────────────────────────
TOPICS = {
    "dashboard information": {
        "table_v5": None,
        "table_v5_OCT25": None,
        "fields": [],
        "country_col": None,
        "id_col": None,
    },
    "roles": {
        "table_v5": "pdl_clean.v5.experience",
        "table_v5_OCT25": "pdl_clean.v5_OCT25.experience",
        "fields": [
            "BGI_SOC2_NAME",
            "BGI_ONET_NAME",
            "BGI_SUBOCCUPATION_NAME",
            "BGI_STANDARD_TITLE",
        ],
        "country_col": "BGI_JOB_COUNTRY",
        "id_col": "ID",
    },
    "education": {
        "table_v5": "pdl_clean.v5.education",
        "table_v5_OCT25": "pdl_clean.v5_OCT25.education",
        "fields": [
            "BGI_DEGREE_MAX_PER_ENTRY",
            "BGI_DEGREE_MAX_PER_ID",
            "BGI_DEGREE",
            "BGI_MAJOR_CIP6_NAME",
            "BGI_SCHOOL_NAME",
        ],
        "country_col": "BGI_COUNTRY",
        "id_col": "PERSON_ID",
    },
    "employers": {
        "table_v5": "pdl_clean.v5.experience",
        "table_v5_OCT25": "pdl_clean.v5_OCT25.experience",
        "fields": [
            "BGI_NAICS2_NAME",
            "BGI_COMPANY_NAME",
        ],
        "country_col": "BGI_JOB_COUNTRY",
        "id_col": "ID",
    },
    "location": {
        "table_v5": "pdl_clean.v5.root_person",
        "table_v5_OCT25": "pdl_clean.v5_OCT25.root_person",
        "fields": [
            "BGI_STATE",
            "BGI_COUNTY_NAME",
            "BGI_CITY",
        ],
        "country_col": "BGI_COUNTRY",
        "id_col": "PERSON_ID",
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
        var_name="version",
        value_name=value_name,
    )


def _sort_by_new(df: pd.DataFrame, new_col: str) -> list[str]:
    if "cnt_ipeds" in df.columns:
        seen = df["field_value"].fillna(NULL_LABEL).astype(str).tolist()
        order = [d for d in _DEGREE_ORDER if d in seen] + [
            v for v in seen if v not in _DEGREE_ORDER
        ]
        return order

    return (
        df.sort_values(new_col, ascending=False)["field_value"].apply(_prepare).tolist()
    )


def chart_counts(df: pd.DataFrame, title: str):
    base_a = _alias(BASE_VERSION)
    new_a = _alias(NEW_VERSION)

    value_cols = [f"cnt_{base_a}", f"cnt_{new_a}"]
    version_order = _VERSION_ORDER_DEFAULT

    if "cnt_ipeds" in df.columns:
        value_cols.append("cnt_ipeds")
        version_order = _VERSION_ORDER_IPEDS

    order = _sort_by_new(df, f"cnt_{new_a}")
    long = _melt(df, value_cols, "count")

    version_map = {
        f"cnt_{base_a}": _label(BASE_VERSION),
        f"cnt_{new_a}": _label(NEW_VERSION),
        "cnt_ipeds": "ipeds",
    }
    long["version"] = long["version"].map(version_map)

    my_colors = FIVE_COLOR_SCALE[: len(version_order)]

    chart = (
        alt.Chart(long, title=title)
        .mark_bar()
        .encode(
            y=alt.Y("field_value:N", sort=order, **_Y_AXIS),
            yOffset=alt.YOffset("version:N", sort=version_order),
            x=alt.X("count:Q", axis=_x_axis(title="Count"), stack=None),
            color=alt.Color(
                "version:N",
                title="Version",
                sort=version_order,
                scale=alt.Scale(domain=version_order, range=my_colors),
                legend=_LEGEND,
            ),
            tooltip=["field_value", "version", "count"],
        )
        .properties(height=600)
    )
    st.altair_chart(chart, use_container_width=True)


def chart_percentages(df: pd.DataFrame, title: str):
    base_a = _alias(BASE_VERSION)
    new_a = _alias(NEW_VERSION)

    value_cols = [f"cnt_{base_a}", f"cnt_{new_a}"]
    version_order = _VERSION_ORDER_DEFAULT

    if "cnt_ipeds" in df.columns:
        value_cols.append("cnt_ipeds")
        version_order = _VERSION_ORDER_IPEDS

    totals = {col: df[col].sum() for col in value_cols}
    df_pct = df.copy()
    for col in value_cols:
        suffix = col.replace("cnt_", "")
        df_pct[f"pct_{suffix}"] = df[col] / totals[col] if totals[col] else 0

    order = _sort_by_new(df, f"cnt_{new_a}")

    pct_cols = [f"pct_{c.replace('cnt_', '')}" for c in value_cols]
    long = _melt(df_pct, pct_cols, "pct")

    version_map = {
        f"pct_{base_a}": _label(BASE_VERSION),
        f"pct_{new_a}": _label(NEW_VERSION),
        "pct_ipeds": "ipeds",
    }
    long["version"] = long["version"].map(version_map)

    my_colors = FIVE_COLOR_SCALE[: len(version_order)]

    chart = (
        alt.Chart(long, title=title)
        .mark_bar()
        .encode(
            y=alt.Y("field_value:N", sort=order, **_Y_AXIS),
            yOffset=alt.YOffset("version:N", sort=version_order),
            x=alt.X("pct:Q", axis=_x_axis(fmt=".1%", title="Percentage"), stack=None),
            color=alt.Color(
                "version:N",
                title="Version",
                sort=version_order,
                scale=alt.Scale(domain=version_order, range=my_colors),
                legend=_LEGEND,
            ),
            tooltip=["field_value", "version", alt.Tooltip("pct", format=".1%")],
        )
        .properties(height=600)
    )
    st.altair_chart(chart, use_container_width=True)


def chart_pct_change(df: pd.DataFrame, title: str):
    df_disp = df.copy()
    df_disp["field_value"] = _prepare(df_disp["field_value"])
    order = (
        df_disp.sort_values("pct_change", key=lambda s: s.abs(), ascending=False)[
            "field_value"
        ].tolist()
    )
    chart = (
        alt.Chart(df_disp, title=title)
        .mark_bar()
        .encode(
            y=alt.Y("field_value:N", sort=order, **_Y_AXIS),
            x=alt.X(
                "pct_change:Q",
                axis=_x_axis(
                    fmt=".1%",
                    title=f"% Change ({NEW_VERSION} − {BASE_VERSION}) / {BASE_VERSION}",
                ),
            ),
            color=alt.condition(
                alt.datum.pct_change > 0,
                alt.value(_BLUE),
                alt.value(_RED),
            ),
            tooltip=["field_value", alt.Tooltip("pct_change", format=".1%")],
        )
        .properties(height=600)
    )
    st.altair_chart(chart, use_container_width=True)


def chart_role_benchmarks(df: pd.DataFrame, title: str):
    # Expected columns: pct_v5, pct_v5_oct25, oews_emp_share, acs_emp_share (as 0-100)
    value_cols = ["pct_v5_oct25", "pct_v5", "oews_emp_share", "acs_emp_share"]
    version_map = {
        "pct_v5_oct25": NEW_VERSION,
        "pct_v5": BASE_VERSION,
        "oews_emp_share": "OEWS",
        "acs_emp_share": "ACS",
    }

    df_disp = df.copy()
    df_disp[value_cols] = df_disp[value_cols] / 100.0

    df_disp["field_value"] = _prepare(df_disp["bgi_soc2_name"])
    order = df_disp.sort_values("pct_v5_oct25", ascending=False)["field_value"]

    long = df_disp.melt(
        id_vars="field_value",
        value_vars=value_cols,
        var_name="version",
        value_name="pct",
    )
    long["version"] = long["version"].map(version_map)

    version_order = [NEW_VERSION, BASE_VERSION, "OEWS", "ACS"]
    my_colors = FIVE_COLOR_SCALE[: len(version_order)]

    chart = (
        alt.Chart(long, title=title)
        .mark_bar()
        .encode(
            y=alt.Y("field_value:N", sort=order, **_Y_AXIS),
            yOffset=alt.YOffset("version:N", sort=version_order),
            x=alt.X("pct:Q", axis=_x_axis(fmt=".1%", title="Percentage"), stack=None),
            color=alt.Color(
                "version:N",
                title="Version / Benchmark",
                sort=version_order,
                scale=alt.Scale(domain=version_order, range=my_colors),
                legend=_LEGEND,
            ),
            tooltip=["field_value", "version", alt.Tooltip("pct", format=".1%")],
        )
        .properties(height=700)
    )
    st.altair_chart(chart, use_container_width=True)


def chart_employer_industry(df: pd.DataFrame, title: str):
    """Render stacked-offset bar chart comparing BLS vs v5 vs v5_OCT25 industry mix."""
    value_cols = ["perc_bls", "perc_v5", "perc_v5_oct25"]
    col_to_version = {
        "perc_bls": "BLS 2023",
        "perc_v5": BASE_VERSION,
        "perc_v5_oct25": NEW_VERSION,
    }

    order = (
        df.sort_values("perc_v5_oct25", ascending=False)["industry"]
        .apply(_prepare)
        .tolist()
    )

    long = df.melt(
        id_vars="industry", value_vars=value_cols, var_name="source", value_name="pct"
    )
    long["version"] = long["source"].map(col_to_version)

    version_order = _VERSION_ORDER_BLS
    my_colors = FIVE_COLOR_SCALE[: len(version_order)]

    chart = (
        alt.Chart(long, title=title)
        .mark_bar()
        .encode(
            y=alt.Y("industry:N", sort=order, **_Y_AXIS),
            yOffset=alt.YOffset("version:N", sort=version_order),
            x=alt.X("pct:Q", axis=_x_axis(fmt=".1%", title="Percentage"), stack=None),
            color=alt.Color(
                "version:N",
                title="Version",
                sort=version_order,
                scale=alt.Scale(domain=version_order, range=my_colors),
                legend=_LEGEND,
            ),
            tooltip=["industry", "version", alt.Tooltip("pct", format=".1%")],
        )
        .properties(height=600)
    )
    st.altair_chart(chart, use_container_width=True)


# ─────────────────────────── MAIN APP ──────────────────────────────────
def main():
    st.title("PDL v5 vs v5_OCT25 Comparison Dashboard")

    topic_options = list(TOPICS.keys())
    topic = st.sidebar.selectbox("Topic:", topic_options, index=0, format_func=str.title)

    if topic == "dashboard information":
        st.header(f"PDL Release Comparison, {BASE_VERSION} - {NEW_VERSION}")
        st.markdown(
            f"""
            This dashboard lets you explore the changes between **{BASE_VERSION}** and **{NEW_VERSION}**.

            It is organised by **topics** which are loosely based on data fields, e.g. *Roles*, *Education*.
            Use the sidebar on the left to select a topic along with a country. You can then choose which data field you would like to look at.
            """
        )
        st.info("Select a topic on the left to get started.")
        return

    country = st.sidebar.selectbox("Country:", COUNTRY_OPTIONS, index=0)
    topic_meta = TOPICS[topic]
    fields = st.sidebar.multiselect(
        "Fields:",
        topic_meta["fields"],
        default=[topic_meta["fields"][0]] if topic_meta["fields"] else [],
    )

    st.caption(
        f"Topic **{topic.title()}**, filtered to **{topic_meta['country_col']} = \"{country}\"**.<br>"
        f"Charts are ordered by {NEW_VERSION} values.",
        unsafe_allow_html=True,
    )

    base_a = _alias(BASE_VERSION)
    new_a = _alias(NEW_VERSION)

    for field in fields:
        with st.container():
            st.subheader(f"{topic.title()} · Field: {field}")
            if field.upper() == "BGI_STANDARD_TITLE":
                st.info(
                    "Please note that the Standard Title format has changed from **singular** to **plural**, which means there's no direct match between the two versions."
                )

            # Query pre-computed KPI table
            kpi_table = make_kpi_table_name(topic, field, country)
            kpi_sql = f"SELECT * FROM PROJECT_DATA.PDL_RELEASE_COMPARISONS_V5_V5_OCT25.{kpi_table}"
            
            try:
                kpi_df = session.sql(kpi_sql).to_pandas()
                cov_v5, cov_v5_oct25, prof_v5, prof_v5_oct25 = kpi_df.iloc[0]

                cols = st.columns(4)
                cols[0].metric(f"Coverage {BASE_VERSION}", f"{cov_v5:.1%}")
                cols[1].metric(f"Coverage {NEW_VERSION}", f"{cov_v5_oct25:.1%}")
                cols[2].metric(f"Profiles {BASE_VERSION}", f"{int(prof_v5):,}")
                cols[3].metric(f"Profiles {NEW_VERSION}", f"{int(prof_v5_oct25):,}")
            except Exception as e:
                st.error(f"Error loading KPI data: {e}")
                st.info(f"Expected table: {kpi_table}")

            # Query pre-computed comparison table
            comp_table = make_comparison_table_name(topic, field, country)
            comparison_sql = f"SELECT * FROM PROJECT_DATA.PDL_RELEASE_COMPARISONS_V5_V5_OCT25.{comp_table}"
            
            try:
                df = session.sql(comparison_sql).to_pandas()
                df.columns = df.columns.str.lower()
                
                if df.empty:
                    st.info("No data returned.")
                    st.divider()
                    continue

                # Derive helper columns
                df["pct_change"] = (
                    (df[f"cnt_{new_a}"] - df[f"cnt_{base_a}"]) / df[f"cnt_{base_a}"]
                ).where(df[f"cnt_{base_a}"] != 0)

                df_top25 = df.sort_values(f"cnt_{new_a}", ascending=False).head(25)

                chart_percentages(
                    df_top25,
                    f"Top 25 Values – Percentages ({NEW_VERSION} vs {BASE_VERSION})",
                )
                chart_counts(
                    df_top25,
                    f"Top 25 Values – Counts ({NEW_VERSION} vs {BASE_VERSION})",
                )

                df_change = (
                    df.dropna(subset=["pct_change"])
                    .assign(abs_change=lambda d: d["pct_change"].abs())
                    .sort_values("abs_change", ascending=False)
                    .head(20)
                )
                if not df_change.empty:
                    chart_pct_change(
                        df_change,
                        f"Largest Percentage Differences ({NEW_VERSION} vs {BASE_VERSION})",
                    )
                    
            except Exception as e:
                st.error(f"Error loading comparison data: {e}")
                st.info(f"Expected table: {comp_table}")

            st.divider()

    # Extra roles-specific chart (SOC-2 vs OEWS/ACS)
    if topic == "roles":
        with st.container():
            st.subheader("Roles · SOC-2 Distribution vs OEWS & ACS Benchmarks")

            roles_sql = "SELECT * FROM PROJECT_DATA.PDL_RELEASE_COMPARISONS_V5_V5_OCT25.ROLES_BENCHMARK_OEWS_ACS"
            try:
                roles_df = session.sql(roles_sql).to_pandas()
                roles_df.columns = roles_df.columns.str.lower()

                if roles_df.empty:
                    st.info("No data returned for Roles benchmark chart.")
                else:
                    roles_df = roles_df.sort_values("pct_v5_oct25", ascending=False).head(25)
                    chart_role_benchmarks(
                        roles_df,
                        f"SOC-2 Employment Share: {NEW_VERSION} vs {BASE_VERSION} vs OEWS vs ACS",
                    )
            except Exception as e:
                st.error(f"Error loading roles benchmark data: {e}")
                st.info("Expected table: ROLES_BENCHMARK_OEWS_ACS")
            
            st.divider()

    # Extra employer-specific chart (Industry vs BLS)
    if topic == "employers":
        with st.container():
            st.subheader("Employer · Industry Distribution")

            employer_sql = "SELECT * FROM PROJECT_DATA.PDL_RELEASE_COMPARISONS_V5_V5_OCT25.EMPLOYERS_INDUSTRY_BLS"
            try:
                bls_df = session.sql(employer_sql).to_pandas()
                bls_df.columns = bls_df.columns.str.lower()
                
                if bls_df.empty:
                    st.info("No data returned for Employer Industry chart.")
                else:
                    bls_df = bls_df.sort_values("perc_v5_oct25", ascending=False).head(20)
                    chart_employer_industry(bls_df, "Comparison with BLS, Year = 2023")
            except Exception as e:
                st.error(f"Error loading employer industry data: {e}")
                st.info("Expected table: EMPLOYERS_INDUSTRY_BLS")
            
            st.divider()


# ───────────────────────── RUN APP ─────────────────────────────────────
if __name__ == "__main__":
    main()

