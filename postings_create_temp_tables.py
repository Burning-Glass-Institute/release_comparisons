# postings_create_temp_tables.py
# Script to pre-compute BGI vs Lightcast postings comparison queries
# and store as tables in PROJECT_DATA.POSTINGS_RELEASE_COMPARISONS
# Now includes 4 sources: Overlap_BGI, Overlap_LC, Full_BGI, Full_LC

import snowflake.connector as snow
from datetime import datetime
import os
import importlib.util

# ─────────────────────────── SOURCE TABLES ───────────────────────────
BGI_POSTINGS = "revelio_clean.v1.bgi_postings"
LC_POSTINGS = "bgi_postings_backups.may_25.us_postings"
XWALK_TABLE = "TEMPORARY_DATA.ARATHI.LC_RL_MAY25_XWALK"

# Date columns
BGI_DATE_COL = "post_date"
LC_DATE_COL = "posted"

# Year filter for full datasets (not applied to overlap)
FULL_MIN_YEAR = 2015

# ─────────────────────────── SNOWFLAKE CONNECTION ─────────────────────────
config_file_path = r"C:\Users\JuliaNania\OneDrive - Burning Glass Institute\Documents\Python\config.py"

if not os.path.isfile(config_file_path):
    raise FileNotFoundError(f"Config file does not exist at the specified path: {config_file_path}")

spec = importlib.util.spec_from_file_location("config", config_file_path)
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)

user = config.credentials['USERNAME']
password = config.credentials['PASSWORD']
account = 'PCA67849'
warehouse = config.credentials['WAREHOUSE']
database = 'PROJECT_DATA'
schema = 'POSTINGS_RELEASE_COMPARISONS'

def execute_ddl(query, conn):
    """Execute DDL statement (CREATE TABLE, etc.)"""
    cur = conn.cursor()
    try:
        cur.execute(query)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()

# ───────────────────────── TOPIC METADATA ─────────────────────────────
# Each field now has overlap_bgi_from, overlap_lc_from, full_bgi_from, full_lc_from
TOPICS = {
    "industry_naics2": {
        "fields": {
            "NAICS2_DISTRIBUTION": {
                "bgi_expr": "n.naics_2022_2_name",
                "lc_expr": "lc.naics_2022_2_name",
                # Overlap (with crosswalk)
                "overlap_bgi_from": f"""{BGI_POSTINGS} bgi
                    INNER JOIN {XWALK_TABLE} xwalk_bgi ON xwalk_bgi.rl_id = bgi.job_id
                    INNER JOIN temporary_data.sswee.naics_full_hierarchy n ON n.naics_2022_6 = bgi.bgi_naics6""",
                "overlap_lc_from": f"""{LC_POSTINGS} lc
                    INNER JOIN {XWALK_TABLE} xwalk_lc ON xwalk_lc.LC_ID = lc.id""",
                # Full (no crosswalk)
                "full_bgi_from": f"""{BGI_POSTINGS} bgi
                    INNER JOIN temporary_data.sswee.naics_full_hierarchy n ON n.naics_2022_6 = bgi.bgi_naics6""",
                "full_lc_from": f"""{LC_POSTINGS} lc""",
            }
        },
        "exclude_values_ilike": ["%Unclassified%"]
    },
    "education": {
        "fields": {
            "MIN_REQUIRED_DEGREE": {
                "bgi_expr": """CASE
                    WHEN UPPER(TRIM(bgi.BGI_MIN_REQUIRED_DEGREE)) = 'NO EDUCATION FOUND'
                         OR UPPER(TRIM(bgi.BGI_MIN_REQUIRED_DEGREE)) = 'NO EDUCATION LISTED'
                    THEN 'NO EDUCATION LISTED'
                    ELSE bgi.BGI_MIN_REQUIRED_DEGREE
                END""",
                "lc_expr": """CASE
                    WHEN UPPER(TRIM(lc.MIN_EDULEVELS_NAME)) = 'HIGH SCHOOL OR GED'
                    THEN 'HIGH SCHOOL/GED'
                    WHEN UPPER(TRIM(lc.MIN_EDULEVELS_NAME)) = 'PH.D. OR PROFESSIONAL DEGREE'
                    THEN 'DOCTORATE DEGREE'
                    ELSE lc.MIN_EDULEVELS_NAME
                END""",
                "overlap_bgi_from": f"""{BGI_POSTINGS} bgi
                    INNER JOIN {XWALK_TABLE} xwalk_bgi ON xwalk_bgi.rl_id = bgi.job_id""",
                "overlap_lc_from": f"""{LC_POSTINGS} lc
                    INNER JOIN {XWALK_TABLE} xwalk_lc ON xwalk_lc.LC_ID = lc.id""",
                "full_bgi_from": f"""{BGI_POSTINGS} bgi""",
                "full_lc_from": f"""{LC_POSTINGS} lc""",
            }
        },
        "exclude_values_ilike": []
    },
    "occupation": {
        "fields": {
            "ONET_NAME": {
                "bgi_expr": "bgi.BGI_ONET_NAME",
                "lc_expr": "lc.ONET_2019_NAME",
                "overlap_bgi_from": f"""{BGI_POSTINGS} bgi
                    INNER JOIN {XWALK_TABLE} xwalk_bgi ON xwalk_bgi.rl_id = bgi.job_id""",
                "overlap_lc_from": f"""{LC_POSTINGS} lc
                    INNER JOIN {XWALK_TABLE} xwalk_lc ON xwalk_lc.LC_ID = lc.id""",
                "full_bgi_from": f"""{BGI_POSTINGS} bgi""",
                "full_lc_from": f"""{LC_POSTINGS} lc""",
            },
            "SOC2": {
                "bgi_expr": "lu.bgi_soc2_name",
                "lc_expr": "lc.soc_2_name",
                "overlap_bgi_from": f"""{BGI_POSTINGS} bgi
                    INNER JOIN {XWALK_TABLE} xwalk_bgi ON xwalk_bgi.rl_id = bgi.job_id
                    INNER JOIN temporary_data.jnania.onet_soc_lookup lu ON lu.bgi_onet = bgi.onet_code""",
                "overlap_lc_from": f"""{LC_POSTINGS} lc
                    INNER JOIN {XWALK_TABLE} xwalk_lc ON xwalk_lc.LC_ID = lc.id""",
                "full_bgi_from": f"""{BGI_POSTINGS} bgi
                    INNER JOIN temporary_data.jnania.onet_soc_lookup lu ON lu.bgi_onet = bgi.onet_code""",
                "full_lc_from": f"""{LC_POSTINGS} lc""",
            }
        },
        "exclude_values_ilike": ["%Unclassified%"]
    },
    "location": {
        "fields": {
            "CITY": {
                "bgi_expr": "bgi.BGI_CITY",
                "lc_expr": "SUBSTR(lc.CITY_NAME, 1, LENGTH(lc.CITY_NAME) - 4)",
                "overlap_bgi_from": f"""{BGI_POSTINGS} bgi
                    INNER JOIN {XWALK_TABLE} xwalk_bgi ON xwalk_bgi.rl_id = bgi.job_id""",
                "overlap_lc_from": f"""{LC_POSTINGS} lc
                    INNER JOIN {XWALK_TABLE} xwalk_lc ON xwalk_lc.LC_ID = lc.id""",
                "full_bgi_from": f"""{BGI_POSTINGS} bgi""",
                "full_lc_from": f"""{LC_POSTINGS} lc""",
            },
            "STATE": {
                "bgi_expr": "bgi.BGI_STATE",
                "lc_expr": "lc.STATE_NAME",
                "overlap_bgi_from": f"""{BGI_POSTINGS} bgi
                    INNER JOIN {XWALK_TABLE} xwalk_bgi ON xwalk_bgi.rl_id = bgi.job_id""",
                "overlap_lc_from": f"""{LC_POSTINGS} lc
                    INNER JOIN {XWALK_TABLE} xwalk_lc ON xwalk_lc.LC_ID = lc.id""",
                "full_bgi_from": f"""{BGI_POSTINGS} bgi""",
                "full_lc_from": f"""{LC_POSTINGS} lc""",
            },
            "MSA": {
                "bgi_expr": "bgi.BGI_MSA",
                "lc_expr": "lc.MSA_NAME",
                "overlap_bgi_from": f"""{BGI_POSTINGS} bgi
                    INNER JOIN {XWALK_TABLE} xwalk_bgi ON xwalk_bgi.rl_id = bgi.job_id""",
                "overlap_lc_from": f"""{LC_POSTINGS} lc
                    INNER JOIN {XWALK_TABLE} xwalk_lc ON xwalk_lc.LC_ID = lc.id""",
                "full_bgi_from": f"""{BGI_POSTINGS} bgi""",
                "full_lc_from": f"""{LC_POSTINGS} lc""",
            },
        },
        "exclude_values_ilike": []
    },
    "titles": {
        "fields": {
            "TITLE_NAME": {
                "bgi_expr": "bgi.BGI_TITLE_NAME",
                "lc_expr": "lc.TITLE_NAME",
                "overlap_bgi_from": f"""{BGI_POSTINGS} bgi
                    INNER JOIN {XWALK_TABLE} xwalk_bgi ON xwalk_bgi.rl_id = bgi.job_id""",
                "overlap_lc_from": f"""{LC_POSTINGS} lc
                    INNER JOIN {XWALK_TABLE} xwalk_lc ON xwalk_lc.LC_ID = lc.id""",
                "full_bgi_from": f"""{BGI_POSTINGS} bgi""",
                "full_lc_from": f"""{LC_POSTINGS} lc""",
            }
        },
        "exclude_values_ilike": ["%Unclassified%"]
    },
}

# ─────────────────────────── SQL HELPERS ────────────────────────────────
def _escape(value: str) -> str:
    return value.replace("'", "''")

def _normalized(expr: str) -> str:
    return f"UPPER(TRIM({expr}))"

def sql_total_counts_yearly() -> str:
    """Yearly totals for all 4 sources (Full sources filtered to >= FULL_MIN_YEAR)"""
    return f"""
    SELECT 'Overlap_BGI' AS source,
           YEAR(bgi.{BGI_DATE_COL}) AS yr,
           COUNT(DISTINCT bgi.job_id) AS cnt
    FROM {BGI_POSTINGS} bgi
    INNER JOIN {XWALK_TABLE} xwalk_bgi ON xwalk_bgi.rl_id = bgi.job_id
    WHERE bgi.bgi_country = 'United States'
    GROUP BY YEAR(bgi.{BGI_DATE_COL})
    UNION ALL
    SELECT 'Overlap_LC' AS source,
           YEAR(lc.{LC_DATE_COL}) AS yr,
           COUNT(DISTINCT lc.id) AS cnt
    FROM {LC_POSTINGS} lc
    INNER JOIN {XWALK_TABLE} xwalk_lc ON xwalk_lc.LC_ID = lc.id
    GROUP BY YEAR(lc.{LC_DATE_COL})
    UNION ALL
    SELECT 'Full_BGI' AS source,
           YEAR(bgi.{BGI_DATE_COL}) AS yr,
           COUNT(DISTINCT bgi.job_id) AS cnt
    FROM {BGI_POSTINGS} bgi
    WHERE bgi.bgi_country = 'United States'
      AND YEAR(bgi.{BGI_DATE_COL}) >= {FULL_MIN_YEAR}
    GROUP BY YEAR(bgi.{BGI_DATE_COL})
    UNION ALL
    SELECT 'Full_LC' AS source,
           YEAR(lc.{LC_DATE_COL}) AS yr,
           COUNT(DISTINCT lc.id) AS cnt
    FROM {LC_POSTINGS} lc
    WHERE YEAR(lc.{LC_DATE_COL}) >= {FULL_MIN_YEAR}
    GROUP BY YEAR(lc.{LC_DATE_COL})
    """

def sql_total_counts_monthly_last12() -> str:
    """Monthly totals for last 12 months for all 4 sources (Full sources filtered to >= FULL_MIN_YEAR)"""
    return f"""
    WITH months AS (
      SELECT DATE_TRUNC('month', DATEADD('month', seq4(),
                 DATEADD('month', -11, DATE_TRUNC('month', CURRENT_DATE())))) AS month_start
      FROM TABLE(GENERATOR(ROWCOUNT => 12))
    )
    -- Overlap BGI
    SELECT 'Overlap_BGI' AS source,
           m.month_start AS month_start,
           COALESCE(b.cnt, 0) AS cnt
    FROM months m
    LEFT JOIN (
      SELECT DATE_TRUNC('month', bgi.{BGI_DATE_COL}) AS month_start,
             COUNT(DISTINCT bgi.job_id) AS cnt
      FROM {BGI_POSTINGS} bgi
      INNER JOIN {XWALK_TABLE} xwalk_bgi ON xwalk_bgi.rl_id = bgi.job_id
      WHERE bgi.{BGI_DATE_COL} >= DATEADD('month', -11, DATE_TRUNC('month', CURRENT_DATE()))
        AND bgi.bgi_country = 'United States'
      GROUP BY 1
    ) b USING (month_start)
    UNION ALL
    -- Overlap LC
    SELECT 'Overlap_LC' AS source,
           m.month_start AS month_start,
           COALESCE(l.cnt, 0) AS cnt
    FROM months m
    LEFT JOIN (
      SELECT DATE_TRUNC('month', lc.{LC_DATE_COL}) AS month_start,
             COUNT(DISTINCT lc.id) AS cnt
      FROM {LC_POSTINGS} lc
      INNER JOIN {XWALK_TABLE} xwalk_lc ON xwalk_lc.LC_ID = lc.id
      WHERE lc.{LC_DATE_COL} >= DATEADD('month', -11, DATE_TRUNC('month', CURRENT_DATE()))
      GROUP BY 1
    ) l USING (month_start)
    UNION ALL
    -- Full BGI (filtered to >= FULL_MIN_YEAR)
    SELECT 'Full_BGI' AS source,
           m.month_start AS month_start,
           COALESCE(b.cnt, 0) AS cnt
    FROM months m
    LEFT JOIN (
      SELECT DATE_TRUNC('month', bgi.{BGI_DATE_COL}) AS month_start,
             COUNT(DISTINCT bgi.job_id) AS cnt
      FROM {BGI_POSTINGS} bgi
      WHERE bgi.{BGI_DATE_COL} >= DATEADD('month', -11, DATE_TRUNC('month', CURRENT_DATE()))
        AND bgi.bgi_country = 'United States'
        AND YEAR(bgi.{BGI_DATE_COL}) >= {FULL_MIN_YEAR}
      GROUP BY 1
    ) b USING (month_start)
    UNION ALL
    -- Full LC (filtered to >= FULL_MIN_YEAR)
    SELECT 'Full_LC' AS source,
           m.month_start AS month_start,
           COALESCE(l.cnt, 0) AS cnt
    FROM months m
    LEFT JOIN (
      SELECT DATE_TRUNC('month', lc.{LC_DATE_COL}) AS month_start,
             COUNT(DISTINCT lc.id) AS cnt
      FROM {LC_POSTINGS} lc
      WHERE lc.{LC_DATE_COL} >= DATEADD('month', -11, DATE_TRUNC('month', CURRENT_DATE()))
        AND YEAR(lc.{LC_DATE_COL}) >= {FULL_MIN_YEAR}
      GROUP BY 1
    ) l USING (month_start)
    """

def sql_generic_compare(
    bgi_expr: str,
    lc_expr: str,
    overlap_bgi_from: str,
    overlap_lc_from: str,
    full_bgi_from: str,
    full_lc_from: str,
    exclude_ilike: list[str] | None = None,
) -> str:
    """Generic comparison for a single field with all 4 sources (Full sources filtered to >= FULL_MIN_YEAR)"""
    bgi_val = _normalized(bgi_expr)
    lc_val = _normalized(lc_expr)

    excl_bgi = ""
    excl_lc = ""
    if exclude_ilike:
        for pat in exclude_ilike:
            patt = _escape(pat).upper()
            excl_bgi += f" AND {bgi_val} NOT LIKE '{patt}'"
            excl_lc += f" AND {lc_val} NOT LIKE '{patt}'"

    return f"""
WITH 
-- Overlap BGI counts (no year filter)
overlap_bgi_ind AS (
    SELECT {bgi_val} AS field_value, COUNT(DISTINCT bgi.job_id) AS overlap_bgi_count
    FROM {overlap_bgi_from}
    WHERE bgi.bgi_country = 'United States'
      AND {bgi_val} IS NOT NULL
      {excl_bgi}
    GROUP BY {bgi_val}
),
overlap_bgi_total AS (
    SELECT COUNT(DISTINCT bgi.job_id) AS total_overlap_bgi
    FROM {BGI_POSTINGS} bgi
    INNER JOIN {XWALK_TABLE} xwalk_bgi ON xwalk_bgi.rl_id = bgi.job_id
    WHERE bgi.bgi_country = 'United States'
),
-- Overlap LC counts (no year filter)
overlap_lc_ind AS (
    SELECT {lc_val} AS field_value, COUNT(DISTINCT lc.id) AS overlap_lc_count
    FROM {overlap_lc_from}
    WHERE {lc_val} IS NOT NULL
      {excl_lc}
    GROUP BY {lc_val}
),
overlap_lc_total AS (
    SELECT COUNT(DISTINCT lc.id) AS total_overlap_lc
    FROM {LC_POSTINGS} lc
    INNER JOIN {XWALK_TABLE} xwalk_lc ON xwalk_lc.LC_ID = lc.id
),
-- Full BGI counts (filtered to >= FULL_MIN_YEAR)
full_bgi_ind AS (
    SELECT {bgi_val} AS field_value, COUNT(DISTINCT bgi.job_id) AS full_bgi_count
    FROM {full_bgi_from}
    WHERE YEAR(bgi.{BGI_DATE_COL}) >= {FULL_MIN_YEAR}
      AND bgi.bgi_country = 'United States'
      AND {bgi_val} IS NOT NULL
      {excl_bgi}
    GROUP BY {bgi_val}
),
full_bgi_total AS (
    SELECT COUNT(DISTINCT bgi.job_id) AS total_full_bgi
    FROM {BGI_POSTINGS} bgi
    WHERE YEAR(bgi.{BGI_DATE_COL}) >= {FULL_MIN_YEAR}
      AND bgi.bgi_country = 'United States'
),
-- Full LC counts (filtered to >= FULL_MIN_YEAR)
full_lc_ind AS (
    SELECT {lc_val} AS field_value, COUNT(DISTINCT lc.id) AS full_lc_count
    FROM {full_lc_from}
    WHERE YEAR(lc.{LC_DATE_COL}) >= {FULL_MIN_YEAR}
      AND {lc_val} IS NOT NULL
      {excl_lc}
    GROUP BY {lc_val}
),
full_lc_total AS (
    SELECT COUNT(DISTINCT lc.id) AS total_full_lc
    FROM {LC_POSTINGS} lc
    WHERE YEAR(lc.{LC_DATE_COL}) >= {FULL_MIN_YEAR}
),
-- Combine all field values
all_values AS (
    SELECT field_value FROM overlap_bgi_ind
    UNION SELECT field_value FROM overlap_lc_ind
    UNION SELECT field_value FROM full_bgi_ind
    UNION SELECT field_value FROM full_lc_ind
)
SELECT
    av.field_value,
    -- Overlap BGI
    COALESCE(overlap_bgi_ind.overlap_bgi_count, 0) AS overlap_bgi_count,
    CASE WHEN overlap_bgi_total.total_overlap_bgi > 0
         THEN COALESCE(overlap_bgi_ind.overlap_bgi_count, 0) / overlap_bgi_total.total_overlap_bgi
         ELSE 0 END AS overlap_bgi_frac,
    -- Overlap LC
    COALESCE(overlap_lc_ind.overlap_lc_count, 0) AS overlap_lc_count,
    CASE WHEN overlap_lc_total.total_overlap_lc > 0
         THEN COALESCE(overlap_lc_ind.overlap_lc_count, 0) / overlap_lc_total.total_overlap_lc
         ELSE 0 END AS overlap_lc_frac,
    -- Full BGI
    COALESCE(full_bgi_ind.full_bgi_count, 0) AS full_bgi_count,
    CASE WHEN full_bgi_total.total_full_bgi > 0
         THEN COALESCE(full_bgi_ind.full_bgi_count, 0) / full_bgi_total.total_full_bgi
         ELSE 0 END AS full_bgi_frac,
    -- Full LC
    COALESCE(full_lc_ind.full_lc_count, 0) AS full_lc_count,
    CASE WHEN full_lc_total.total_full_lc > 0
         THEN COALESCE(full_lc_ind.full_lc_count, 0) / full_lc_total.total_full_lc
         ELSE 0 END AS full_lc_frac
FROM all_values av
LEFT JOIN overlap_bgi_ind ON av.field_value IS NOT DISTINCT FROM overlap_bgi_ind.field_value
LEFT JOIN overlap_lc_ind ON av.field_value IS NOT DISTINCT FROM overlap_lc_ind.field_value
LEFT JOIN full_bgi_ind ON av.field_value IS NOT DISTINCT FROM full_bgi_ind.field_value
LEFT JOIN full_lc_ind ON av.field_value IS NOT DISTINCT FROM full_lc_ind.field_value
CROSS JOIN overlap_bgi_total
CROSS JOIN overlap_lc_total
CROSS JOIN full_bgi_total
CROSS JOIN full_lc_total
ORDER BY full_bgi_count DESC NULLS LAST, overlap_bgi_count DESC NULLS LAST
"""

def sql_generic_kpis(
    bgi_expr: str,
    lc_expr: str,
    overlap_bgi_from: str,
    overlap_lc_from: str,
    full_bgi_from: str,
    full_lc_from: str,
    exclude_ilike: list[str] | None = None,
) -> str:
    """KPI totals + coverage for all 4 sources (Full sources filtered to >= FULL_MIN_YEAR)"""
    bgi_val = _normalized(bgi_expr)
    lc_val = _normalized(lc_expr)

    excl_bgi = ""
    excl_lc = ""
    if exclude_ilike:
        for pat in exclude_ilike:
            patt = _escape(pat).upper()
            excl_bgi += f" AND {bgi_val} NOT LIKE '{patt}'"
            excl_lc += f" AND {lc_val} NOT LIKE '{patt}'"

    return f"""
WITH 
-- Overlap BGI (no year filter)
overlap_bgi_total AS (
    SELECT COUNT(DISTINCT bgi.job_id) AS total_overlap_bgi
    FROM {BGI_POSTINGS} bgi
    INNER JOIN {XWALK_TABLE} xwalk_bgi ON xwalk_bgi.rl_id = bgi.job_id
    WHERE bgi.bgi_country = 'United States'
),
overlap_bgi_cov AS (
    SELECT COUNT(DISTINCT bgi.job_id) AS covered_overlap_bgi
    FROM {overlap_bgi_from}
    WHERE bgi.bgi_country = 'United States'
      AND {bgi_val} IS NOT NULL
      {excl_bgi}
),
-- Overlap LC (no year filter)
overlap_lc_total AS (
    SELECT COUNT(DISTINCT lc.id) AS total_overlap_lc
    FROM {LC_POSTINGS} lc
    INNER JOIN {XWALK_TABLE} xwalk_lc ON xwalk_lc.LC_ID = lc.id
),
overlap_lc_cov AS (
    SELECT COUNT(DISTINCT lc.id) AS covered_overlap_lc
    FROM {overlap_lc_from}
    WHERE {lc_val} IS NOT NULL
      {excl_lc}
),
-- Full BGI (filtered to >= FULL_MIN_YEAR)
full_bgi_total AS (
    SELECT COUNT(DISTINCT bgi.job_id) AS total_full_bgi
    FROM {BGI_POSTINGS} bgi
    WHERE bgi.bgi_country = 'United States'
      AND YEAR(bgi.{BGI_DATE_COL}) >= {FULL_MIN_YEAR}
),
full_bgi_cov AS (
    SELECT COUNT(DISTINCT bgi.job_id) AS covered_full_bgi
    FROM {full_bgi_from}
    WHERE bgi.bgi_country = 'United States'
      AND YEAR(bgi.{BGI_DATE_COL}) >= {FULL_MIN_YEAR}
      AND {bgi_val} IS NOT NULL
      {excl_bgi}
),
-- Full LC (filtered to >= FULL_MIN_YEAR)
full_lc_total AS (
    SELECT COUNT(DISTINCT lc.id) AS total_full_lc
    FROM {LC_POSTINGS} lc
    WHERE YEAR(lc.{LC_DATE_COL}) >= {FULL_MIN_YEAR}
),
full_lc_cov AS (
    SELECT COUNT(DISTINCT lc.id) AS covered_full_lc
    FROM {full_lc_from}
    WHERE YEAR(lc.{LC_DATE_COL}) >= {FULL_MIN_YEAR}
      AND {lc_val} IS NOT NULL
      {excl_lc}
)
SELECT
    total_overlap_bgi, covered_overlap_bgi,
    total_overlap_lc, covered_overlap_lc,
    total_full_bgi, covered_full_bgi,
    total_full_lc, covered_full_lc
FROM overlap_bgi_total, overlap_bgi_cov, 
     overlap_lc_total, overlap_lc_cov,
     full_bgi_total, full_bgi_cov,
     full_lc_total, full_lc_cov
"""

# ─────────────────────────── TABLE NAME HELPERS ─────────────────────────────
def clean_name(s: str) -> str:
    return s.upper().replace(" ", "_").replace("-", "_").replace("'", "")

def make_comparison_table_name(topic: str, field: str) -> str:
    return f"COMP_{clean_name(topic)}_{clean_name(field)}"

def make_kpi_table_name(topic: str, field: str) -> str:
    return f"KPI_{clean_name(topic)}_{clean_name(field)}"

# ─────────────────────────── MAIN SCRIPT ─────────────────────────────
def main():
    print(f"[{datetime.now()}] Starting temp table creation...")

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

    # Create schema if not exists
    try:
        execute_ddl(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}", conn)
        print(f"[{datetime.now()}] Schema {schema} ready")
    except Exception as e:
        print(f"[{datetime.now()}] Schema note: {e}")

    # Total counts yearly
    print(f"\n[{datetime.now()}] Processing: total counts yearly (4 sources)")
    yearly_sql = sql_total_counts_yearly()
    try:
        execute_ddl(f"CREATE OR REPLACE TABLE TOTAL_COUNTS_YEARLY AS\n{yearly_sql}", conn)
        print(f"  ✓ Created TOTAL_COUNTS_YEARLY")
        total_tables += 1
    except Exception as e:
        print(f"  ✗ Failed to create TOTAL_COUNTS_YEARLY: {e}")

    # Total counts monthly (last 12 months)
    print(f"\n[{datetime.now()}] Processing: total counts monthly (4 sources)")
    monthly_sql = sql_total_counts_monthly_last12()
    try:
        execute_ddl(f"CREATE OR REPLACE TABLE TOTAL_COUNTS_MONTHLY AS\n{monthly_sql}", conn)
        print(f"  ✓ Created TOTAL_COUNTS_MONTHLY")
        total_tables += 1
    except Exception as e:
        print(f"  ✗ Failed to create TOTAL_COUNTS_MONTHLY: {e}")

    # Process topic/field combinations (no year loop - single table per field)
    for topic, meta in TOPICS.items():
        exclude_ilike = meta.get("exclude_values_ilike", [])

        for field_name, field_map in meta["fields"].items():
            bgi_expr = field_map["bgi_expr"]
            lc_expr = field_map["lc_expr"]
            overlap_bgi_from = field_map["overlap_bgi_from"]
            overlap_lc_from = field_map["overlap_lc_from"]
            full_bgi_from = field_map["full_bgi_from"]
            full_lc_from = field_map["full_lc_from"]

            print(f"\n[{datetime.now()}] Processing: {topic} | {field_name}")

            # KPI table
            kpi_table = make_kpi_table_name(topic, field_name)
            kpi_sql = sql_generic_kpis(
                bgi_expr, lc_expr,
                overlap_bgi_from, overlap_lc_from,
                full_bgi_from, full_lc_from,
                exclude_ilike
            )
            try:
                execute_ddl(f"CREATE OR REPLACE TABLE {kpi_table} AS\n{kpi_sql}", conn)
                print(f"  ✓ Created {kpi_table}")
                total_tables += 1
            except Exception as e:
                print(f"  ✗ Failed to create {kpi_table}: {e}")

            # Comparison table
            comp_table = make_comparison_table_name(topic, field_name)
            comp_sql = sql_generic_compare(
                bgi_expr, lc_expr,
                overlap_bgi_from, overlap_lc_from,
                full_bgi_from, full_lc_from,
                exclude_ilike
            )
            try:
                execute_ddl(f"CREATE OR REPLACE TABLE {comp_table} AS\n{comp_sql}", conn)
                print(f"  ✓ Created {comp_table}")
                total_tables += 1
            except Exception as e:
                print(f"  ✗ Failed to create {comp_table}: {e}")

    conn.close()
    print(f"\n[{datetime.now()}] ═══════════════════════════════════════")
    print(f"[{datetime.now()}] Complete! Created {total_tables} tables in {database}.{schema}")
    print(f"[{datetime.now()}] ═══════════════════════════════════════")

if __name__ == "__main__":
    main()
