# create_temp_tables.py
# Script to pre-compute PDL comparison queries and store as temp tables
# in PROJECT_DATA.PDL_RELEASE_COMPARISONS

import pandas as pd
import snowflake.connector as snow
from datetime import datetime
import os
import importlib.util

# ─────────────────────────── VERSION CONFIG ───────────────────────────
BASE_VERSION = "v5"
NEW_VERSION = "v5_OCT25"

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

# ─────────────────────────── SNOWFLAKE CONNECTION ─────────────────────────
# Specify the path to the configuration file
config_file_path = r"C:\Users\JuliaNania\OneDrive - Burning Glass Institute\Documents\Python\config.py"

# Check if the file exists
if not os.path.isfile(config_file_path):
    raise FileNotFoundError(f"Config file does not exist at the specified path: {config_file_path}")

# Dynamically load the config module
spec = importlib.util.spec_from_file_location("config", config_file_path)
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)

# Set Connection
user = config.credentials['USERNAME']
password = config.credentials['PASSWORD']
account = 'PCA67849'
warehouse = config.credentials['WAREHOUSE']
database = 'PROJECT_DATA'
schema = 'PDL_RELEASE_COMPARISONS'

def get_query(query, conn):
    """Execute query and return pandas dataframe"""
    cur = conn.cursor()
    try:
        cur.execute(query)
        df = cur.fetch_pandas_all()
        return df
    finally:
        cur.close()

def execute_ddl(query, conn):
    """Execute DDL statement (CREATE TABLE, etc.)"""
    cur = conn.cursor()
    try:
        cur.execute(query)
        conn.commit()  # Explicitly commit DDL changes
        return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()

# ───────────────────────── TOPIC METADATA ─────────────────────────────
TOPICS = {
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

# ─────────────────────────── SQL HELPERS ────────────────────────────────
def _escape(value: str) -> str:
    return value.replace("'", "''")

def _alias(version: str) -> str:
    return version.lower().replace("-", "_").replace(" ", "_")

_NULL_PLACEHOLDER = "__NULL__"
_LOCATION_CONF_FILTER_FIELDS = {"BGI_STATE", "BGI_COUNTY_NAME", "BGI_CITY"}
_SCHOOL_CONF_TARGET_FIELD = "BGI_SCHOOL_NAME"

def _needs_loc_conf_filter(topic: str, field: str) -> bool:
    return topic == "location" and field.upper() in _LOCATION_CONF_FILTER_FIELDS

def _school_conf_filter(topic: str, field: str, country: str) -> str:
    if topic == "education" and field.upper() == _SCHOOL_CONF_TARGET_FIELD:
        if country.strip().lower() in {"united states", "us", "usa"}:
            return "BGI_SCHOOL_CONFIDENCE >= 72"
        else:
            return "BGI_INTERNATIONAL_SCHOOL_CONFIDENCE >= 70"
    return ""

def _build_conf_filter(topic: str, field: str, country: str) -> str:
    clauses: list[str] = []
    if _needs_loc_conf_filter(topic, field):
        clauses.append("BGI_LOCATION_CONFIDENCE >= 89")

    school_clause = _school_conf_filter(topic, field, country)
    if school_clause:
        clauses.append(school_clause)

    if topic == "roles" and field.upper() == "BGI_SOC2_NAME":
        clauses.append("BGI_ONET_CONFIDENCE >= 7")

    return "".join(f"\n  AND {c}" for c in clauses)

# ──────────────────────────── CTE BUILDERS ─────────────────────────────
def build_count_cte(
    version: str,
    table: str,
    field: str,
    country: str,
    country_col: str,
    *,
    topic: str,
) -> str:
    a = _alias(version)
    country_cond = f"{country_col} = '{_escape(country)}'"
    conf_filter = _build_conf_filter(topic, field, country)

    if field.upper() != "BGI_DEGREE":
        join_key_expr = f"COALESCE({field}, '{_NULL_PLACEHOLDER}')"
        return f"""
        {a} AS (
            SELECT
                {field}                              AS field_value,
                {join_key_expr}                      AS join_key,
                COUNT(*)                             AS cnt_{a}
            FROM {table}
            WHERE {country_cond}
              {conf_filter}
              AND ({field} IS NOT NULL AND {field} NOT ILIKE '%Unclassified%')
            GROUP BY {field}, join_key
        )"""

    return f"""
    {a} AS (
        SELECT
            field_value                                  AS field_value,
            COALESCE(field_value, '{_NULL_PLACEHOLDER}') AS join_key,
            COUNT(*)                                     AS cnt_{a}
        FROM (
            SELECT
                CASE
                    WHEN {field} IS NULL THEN 'Unknown'
                    ELSE TRIM(value)
                END AS field_value
            FROM {table},
            LATERAL FLATTEN(input => SPLIT(COALESCE({field}, 'Unknown'), ', '))
            WHERE {country_cond}
              {conf_filter}
              AND ({field} IS NOT NULL AND {field} NOT ILIKE '%Unclassified%')
              AND (CASE WHEN {field} IS NULL THEN 'Unknown' ELSE TRIM(value) END) IS NOT NULL
              AND EDUCATION_RAW <> '\"[\"\"]'
        )
        GROUP BY field_value, join_key
    )
"""

def build_comparison_query(
    table_base: str,
    table_new: str,
    field: str,
    country: str,
    country_col: str,
    *,
    topic: str,
) -> str:
    base_cte = build_count_cte(
        BASE_VERSION, table_base, field, country, country_col, topic=topic
    )
    new_cte = build_count_cte(
        NEW_VERSION, table_new, field, country, country_col, topic=topic
    )

    base_a = _alias(BASE_VERSION)
    new_a = _alias(NEW_VERSION)

    if field.upper() == "BGI_DEGREE_MAX_PER_ENTRY":
        ipeds_cte = f"""
        ipeds AS (
            SELECT
                BGI_DEGREE_MAX_PER_ENTRY                                  AS field_value,
                COALESCE(BGI_DEGREE_MAX_PER_ENTRY, '{_NULL_PLACEHOLDER}') AS join_key,
                CNT                                                       AS cnt_ipeds
            FROM TEMPORARY_DATA.JNANIA.IPEDS_2023_COUNTS
        )"""
        return f"""
        WITH {base_cte}, {new_cte}, {ipeds_cte}
        SELECT
            COALESCE({base_a}.field_value, {new_a}.field_value, ipeds.field_value) AS field_value,
            COALESCE({base_a}.cnt_{base_a}, 0) AS cnt_{base_a},
            COALESCE({new_a}.cnt_{new_a}, 0)   AS cnt_{new_a},
            COALESCE(ipeds.cnt_ipeds, 0)       AS cnt_ipeds
        FROM {base_a}
        FULL OUTER JOIN {new_a}  ON {base_a}.join_key = {new_a}.join_key
        FULL OUTER JOIN ipeds ON COALESCE({base_a}.join_key, {new_a}.join_key) = ipeds.join_key
        """

    return f"""
    WITH {base_cte}, {new_cte}
    SELECT
        COALESCE({base_a}.field_value, {new_a}.field_value) AS field_value,
        COALESCE({base_a}.cnt_{base_a}, 0) AS cnt_{base_a},
        COALESCE({new_a}.cnt_{new_a}, 0)   AS cnt_{new_a}
    FROM {base_a}
    FULL OUTER JOIN {new_a} ON {base_a}.join_key = {new_a}.join_key
    """

def build_kpi_query(
    table_base: str,
    table_new: str,
    field: str,
    country: str,
    country_col: str,
    id_col: str = "ID",
    *,
    topic: str,
) -> str:
    conf_filter = _build_conf_filter(topic, field, country)
    coverage_test = (
        f"{field} <> 'Unclassified'" if topic == "roles" else f"{field} IS NOT NULL"
    )

    return f"""
    WITH
    cov_base AS (
        SELECT
            COUNT_IF({coverage_test}) AS not_null_count,
            COUNT(*)                  AS total_count
        FROM {table_base}
        WHERE {country_col} = '{_escape(country)}'
          {conf_filter}
    ),
    cov_new AS (
        SELECT
            COUNT_IF({coverage_test}) AS not_null_count,
            COUNT(*)                  AS total_count
        FROM {table_new}
        WHERE {country_col} = '{_escape(country)}'
          {conf_filter}
    )
    SELECT
        cov_base.not_null_count / NULLIF(cov_base.total_count, 0) AS coverage_v5,
        cov_new.not_null_count  / NULLIF(cov_new.total_count, 0)  AS coverage_v5_oct25,
        cov_base.total_count AS profiles_v5,
        cov_new.total_count  AS profiles_v5_oct25
    FROM cov_base, cov_new
    """

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

# ─────────────────────────── MAIN SCRIPT ─────────────────────────────
def main():
    print(f"[{datetime.now()}] Starting temp table creation...")
    
    # Create Snowflake connection
    conn = snow.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    print(f"[{datetime.now()}] Connected to Snowflake")
    print(f"[{datetime.now()}] Using {database}.{schema}")
    
    total_tables = 0
    
    # Process standard topic/field/country combinations
    for topic, meta in TOPICS.items():
        table_base = meta["table_v5"]
        table_new = meta["table_v5_OCT25"]
        country_col = meta["country_col"]
        id_col = meta.get("id_col", "ID")
        
        for field in meta["fields"]:
            for country in COUNTRY_OPTIONS:
                print(f"\n[{datetime.now()}] Processing: {topic} | {field} | {country}")
                
                # KPI table
                kpi_table = make_kpi_table_name(topic, field, country)
                kpi_sql = build_kpi_query(
                    table_base, table_new, field, country, country_col,
                    id_col=id_col, topic=topic
                )
                create_kpi = f"CREATE OR REPLACE TABLE {kpi_table} AS\n{kpi_sql}"
                
                try:
                    execute_ddl(create_kpi, conn)
                    print(f"  ✓ Created {kpi_table}")
                    total_tables += 1
                except Exception as e:
                    print(f"  ✗ Failed to create {kpi_table}: {e}")
                
                # Comparison table
                comp_table = make_comparison_table_name(topic, field, country)
                comp_sql = build_comparison_query(
                    table_base, table_new, field, country, country_col,
                    topic=topic
                )
                create_comp = f"CREATE OR REPLACE TABLE {comp_table} AS\n{comp_sql}"
                
                try:
                    execute_ddl(create_comp, conn)
                    print(f"  ✓ Created {comp_table}")
                    total_tables += 1
                except Exception as e:
                    print(f"  ✗ Failed to create {comp_table}: {e}")
    
    # Special case: Roles benchmark (US only)
    print(f"\n[{datetime.now()}] Processing: roles benchmark (OEWS/ACS)")
    role_bench_sql = f"""
    WITH new_ver AS (
      SELECT
        bgi_soc2_name,
        COUNT(*) AS cnt_new,
        ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 2) * 100 AS pct_new
      FROM {TOPICS["roles"]["table_v5_OCT25"]}
      WHERE bgi_job_country = 'United States'
        AND BGI_ONET_CONFIDENCE >= 7
      GROUP BY bgi_soc2_name
    ),
    base_ver AS (
      SELECT
        bgi_soc2_name,
        COUNT(*) AS cnt_base,
        ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 2) * 100 AS pct_base
      FROM {TOPICS["roles"]["table_v5"]}
      WHERE bgi_job_country = 'United States'
        AND BGI_ONET_CONFIDENCE >= 7
      GROUP BY bgi_soc2_name
    )
    SELECT
      COALESCE(base_ver.bgi_soc2_name, new_ver.bgi_soc2_name) AS bgi_soc2_name,
      COALESCE(pct_base, 0) AS pct_v5,
      COALESCE(pct_new, 0)  AS pct_v5_oct25,
      COALESCE(oews_emp_share, 0) AS oews_emp_share,
      COALESCE(acs_emp_share , 0) AS acs_emp_share
    FROM base_ver
    FULL JOIN new_ver
      ON base_ver.bgi_soc2_name = new_ver.bgi_soc2_name
    FULL JOIN temporary_data.jnania.occ_distr_acs_oews oews
      ON oews.soc_2_name = COALESCE(base_ver.bgi_soc2_name, new_ver.bgi_soc2_name)
    """
    
    try:
        execute_ddl(f"CREATE OR REPLACE TABLE ROLES_BENCHMARK_OEWS_ACS AS\n{role_bench_sql}", conn)
        print(f"  ✓ Created ROLES_BENCHMARK_OEWS_ACS")
        total_tables += 1
    except Exception as e:
        print(f"  ✗ Failed to create ROLES_BENCHMARK_OEWS_ACS: {e}")
    
    # Special case: Employer industry vs BLS
    print(f"\n[{datetime.now()}] Processing: employer industry (BLS)")
    employer_bls_sql = f"""
    WITH NEW_EXP AS (
        SELECT
            exp.PERSON_ID,
            exp.BGI_NAICS2      AS NAICS2_CODE,
            exp.BGI_NAICS2_NAME AS NAICS2_NAME
        FROM {TOPICS["employers"]["table_v5_OCT25"]} exp
        WHERE COMPANY_ID IS NOT NULL
          AND exp.BGI_NAICS2 IS NOT NULL
          AND COMPANY_LOCATION_COUNTRY = 'united states'
          AND START_DATE <= '2023-01-01'
          AND (END_DATE >= '2023-12-31' OR END_DATE IS NULL)
    ),
    BASE_EXP AS (
        SELECT
            exp.PERSON_ID,
            exp.BGI_NAICS2      AS NAICS2_CODE,
            exp.BGI_NAICS2_NAME AS NAICS2_NAME
        FROM {TOPICS["employers"]["table_v5"]} exp
        WHERE COMPANY_ID IS NOT NULL
          AND exp.BGI_NAICS2 IS NOT NULL
          AND COMPANY_LOCATION_COUNTRY = 'united states'
          AND START_DATE <= '2023-01-01'
          AND (END_DATE >= '2023-12-31' OR END_DATE IS NULL)
    ),
    NEW_PERC AS (
        SELECT
            NAICS2_CODE,
            NAICS2_NAME,
            COUNT(DISTINCT PERSON_ID) / TTL AS PERC_V5_OCT25
        FROM NEW_EXP
        INNER JOIN (SELECT COUNT(DISTINCT PERSON_ID) AS TTL FROM NEW_EXP)
        GROUP BY NAICS2_CODE, NAICS2_NAME, TTL
    ),
    BASE_PERC AS (
        SELECT
            NAICS2_CODE,
            NAICS2_NAME,
            COUNT(DISTINCT PERSON_ID) / TTL AS PERC_V5
        FROM BASE_EXP
        INNER JOIN (SELECT COUNT(DISTINCT PERSON_ID) AS TTL FROM BASE_EXP)
        GROUP BY NAICS2_CODE, NAICS2_NAME, TTL
    )
    SELECT
        BLS_CODE,
        bls.INDUSTRY,
        AVG(bls.BLS_PCT)              AS PERC_BLS,
        SUM(BASE_PERC.PERC_V5)        AS PERC_V5,
        SUM(NEW_PERC.PERC_V5_OCT25)   AS PERC_V5_OCT25
    FROM TEMPORARY_DATA.MBONE.BLS_COMPANY_MAP_2023 bls
    LEFT JOIN NEW_PERC  ON NEW_PERC.NAICS2_CODE  = bls.NAICS2_CODE
    LEFT JOIN BASE_PERC ON BASE_PERC.NAICS2_CODE = bls.NAICS2_CODE
    GROUP BY BLS_CODE, bls.INDUSTRY
    """
    
    try:
        execute_ddl(f"CREATE OR REPLACE TABLE EMPLOYERS_INDUSTRY_BLS AS\n{employer_bls_sql}", conn)
        print(f"  ✓ Created EMPLOYERS_INDUSTRY_BLS")
        total_tables += 1
    except Exception as e:
        print(f"  ✗ Failed to create EMPLOYERS_INDUSTRY_BLS: {e}")
    
    # Close connection
    conn.close()
    print(f"\n[{datetime.now()}] ═══════════════════════════════════════")
    print(f"[{datetime.now()}] Complete! Created {total_tables} tables in {database}.{schema}")
    print(f"[{datetime.now()}] ═══════════════════════════════════════")

if __name__ == "__main__":
    main()

