# postings_create_temp_tables.py
# Script to pre-compute BGI vs Lightcast postings comparison queries
# and store as tables in PROJECT_DATA.POSTINGS_RELEASE_COMPARISONS
# Now includes 4 sources: Overlap_BGI, Overlap_LC, Full_BGI, Full_LC

import snowflake.connector as snow
from datetime import datetime
import os
import importlib.util
import pandas as pd

# ─────────────────────────── SOURCE TABLES ───────────────────────────
BGI_POSTINGS = "revelio_clean.v1.bgi_postings"
LC_POSTINGS = "bgi_postings_backups.may_25.us_postings"
XWALK_TABLE = "TEMPORARY_DATA.ARATHI.LC_RL_MAY25_XWALK"

# Date columns
BGI_DATE_COL = "post_date"
LC_DATE_COL = "posted"

# Year filter for full datasets (not applied to overlap)
FULL_MIN_YEAR = 2015

# ─────────────────────────── EXTERNAL / PUBLIC DATA ──────────────────────────
JOLTS_EXCEL_PATH = r"C:\Users\JuliaNania\OneDrive - Burning Glass Institute\Documents\GitHub\release_comparisons\jolts_total_nonfarm_monthly_jobopenings.xlsx"
OEWS_EXCEL_PATH = r"C:\Users\JuliaNania\OneDrive - Burning Glass Institute\Documents\GitHub\release_comparisons\oews_empl_national_M2024_dl.xlsx"
JOLTS_SF_TABLE = "OUTSIDE_DATA.JOLTS.ALL_DATA"

# NAICS2 → JOLTS-compatible sector mapping
NAICS2_SECTOR_MAP = {
    'MINING, QUARRYING, AND OIL AND GAS EXTRACTION': 'Mining, Quarrying, and Oil and Gas Extraction',
    'UTILITIES': 'Transportation, Warehousing, and Utilities',
    'CONSTRUCTION': 'Construction',
    'MANUFACTURING': 'Manufacturing',
    'WHOLESALE TRADE': 'Wholesale Trade',
    'RETAIL TRADE': 'Retail Trade',
    'TRANSPORTATION AND WAREHOUSING': 'Transportation, Warehousing, and Utilities',
    'INFORMATION': 'Information',
    'FINANCE AND INSURANCE': 'Finance and Insurance',
    'REAL ESTATE AND RENTAL AND LEASING': 'Real Estate and Rental and Leasing',
    'PROFESSIONAL, SCIENTIFIC, AND TECHNICAL SERVICES': 'Professional and Business Services',
    'MANAGEMENT OF COMPANIES AND ENTERPRISES': 'Professional and Business Services',
    'ADMINISTRATIVE AND SUPPORT AND WASTE MANAGEMENT AND REMEDIATION SERVICES': 'Professional and Business Services',
    'EDUCATIONAL SERVICES': 'Educational Services',
    'HEALTH CARE AND SOCIAL ASSISTANCE': 'Health Care and Social Assistance',
    'ARTS, ENTERTAINMENT, AND RECREATION': 'Arts, Entertainment, and Recreation',
    'ACCOMMODATION AND FOOD SERVICES': 'Accommodation and Food Services',
    'OTHER SERVICES (EXCEPT PUBLIC ADMINISTRATION)': 'Other Services (except Public Administration)',
    'PUBLIC ADMINISTRATION': 'Public Administration',
}

# JOLTS industry_code → sector mapping
JOLTS_CODE_MAP = {
    '110099': 'Mining, Quarrying, and Oil and Gas Extraction',
    '230000': 'Construction',
    '300000': 'Manufacturing',
    '420000': 'Wholesale Trade',
    '440000': 'Retail Trade',
    '480099': 'Transportation, Warehousing, and Utilities',
    '510000': 'Information',
    '520000': 'Finance and Insurance',
    '530000': 'Real Estate and Rental and Leasing',
    '540099': 'Professional and Business Services',
    '610000': 'Educational Services',
    '620000': 'Health Care and Social Assistance',
    '710000': 'Arts, Entertainment, and Recreation',
    '720000': 'Accommodation and Food Services',
    '810000': 'Other Services (except Public Administration)',
    '900000': 'Public Administration',
}

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

# ─────────────────────────── PUBLIC DATA HELPERS ─────────────────────────────

def _naics2_to_sector(col):
    """CASE expression mapping NAICS2 name → JOLTS-compatible sector"""
    whens = "\n        ".join(f"WHEN '{k}' THEN '{v}'" for k, v in NAICS2_SECTOR_MAP.items())
    return f"CASE UPPER(TRIM({col})) {whens} ELSE NULL END"


def _jolts_code_to_sector():
    """CASE expression mapping JOLTS industry_code → sector"""
    whens = "\n        ".join(f"WHEN '{k}' THEN '{v}'" for k, v in JOLTS_CODE_MAP.items())
    return f'CASE j."industry_code" {whens} END'


# ── JOLTS monthly: load Excel and upload to staging table ────────────────
def load_and_upload_jolts(conn):
    """Load JOLTS Excel (wide format, values in thousands), upload as JOLTS_MONTHLY_RAW"""
    jolts_raw = pd.read_excel(JOLTS_EXCEL_PATH, sheet_name='Sheet1')
    year_col = jolts_raw.columns[0]
    month_cols = jolts_raw.columns[1:]
    # Melt wide → long
    jolts_long = jolts_raw.melt(id_vars=year_col, value_vars=month_cols,
                                var_name='MONTH_NAME', value_name='POSTING_COUNT')
    jolts_long.rename(columns={year_col: 'YEAR'}, inplace=True)
    jolts_long['YEAR'] = pd.to_numeric(jolts_long['YEAR'], errors='coerce')
    jolts_long = jolts_long.dropna(subset=['YEAR'])
    # Map month names → numbers → date
    month_map = {m: i for i, m in enumerate(
        ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
         'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'], 1)}
    jolts_long['MONTH_NUM'] = jolts_long['MONTH_NAME'].map(month_map)
    jolts_long['MONTH'] = pd.to_datetime(
        jolts_long['YEAR'].astype(int).astype(str) + '-' +
        jolts_long['MONTH_NUM'].astype(int).astype(str) + '-01', errors='coerce')
    jolts_long = jolts_long.dropna(subset=['MONTH', 'POSTING_COUNT'])
    jolts_long['POSTING_COUNT'] = (jolts_long['POSTING_COUNT'] * 1000).astype(int)
    result = jolts_long[['MONTH', 'POSTING_COUNT']].sort_values('MONTH').reset_index(drop=True)
    # Upload to Snowflake
    execute_ddl("CREATE OR REPLACE TABLE JOLTS_MONTHLY_RAW (MONTH DATE, POSTING_COUNT NUMBER)", conn)
    cur = conn.cursor()
    try:
        cur.executemany(
            "INSERT INTO JOLTS_MONTHLY_RAW (MONTH, POSTING_COUNT) VALUES (%s, %s)",
            [(r['MONTH'].strftime('%Y-%m-%d'), int(r['POSTING_COUNT'])) for _, r in result.iterrows()])
        conn.commit()
    finally:
        cur.close()


# ── OEWS national: load Excel and upload to staging table ────────────────
def load_and_upload_oews(conn):
    """Load OEWS Excel, keep major occupation groups, upload as OEWS_NATIONAL_RAW"""
    oews_raw = pd.read_excel(OEWS_EXCEL_PATH)
    oews_major = oews_raw[oews_raw['O_GROUP'] == 'major'].copy()
    oews_major['TOT_EMP'] = pd.to_numeric(oews_major['TOT_EMP'], errors='coerce')
    oews_major = oews_major.dropna(subset=['TOT_EMP'])
    result = oews_major[['OCC_TITLE', 'TOT_EMP']].reset_index(drop=True)
    # Upload to Snowflake
    execute_ddl("CREATE OR REPLACE TABLE OEWS_NATIONAL_RAW (OCC_TITLE VARCHAR, TOT_EMP NUMBER)", conn)
    cur = conn.cursor()
    try:
        cur.executemany(
            "INSERT INTO OEWS_NATIONAL_RAW (OCC_TITLE, TOT_EMP) VALUES (%s, %s)",
            [(str(r['OCC_TITLE']), int(r['TOT_EMP'])) for _, r in result.iterrows()])
        conn.commit()
    finally:
        cur.close()


# ── JOLTS monthly comparison SQL (JOLTS + Full_BGI + Full_LC) ────────────
def sql_jolts_monthly_comparison():
    return f"""
    SELECT 'JOLTS' AS SOURCE, MONTH AS MONTH_START, POSTING_COUNT AS CNT
    FROM JOLTS_MONTHLY_RAW
    WHERE MONTH >= '{FULL_MIN_YEAR}-01-01'
    UNION ALL
    SELECT 'Full_BGI' AS SOURCE,
           DATE_TRUNC('month', bgi.{BGI_DATE_COL}) AS MONTH_START,
           COUNT(DISTINCT bgi.job_id) AS CNT
    FROM {BGI_POSTINGS} bgi
    WHERE bgi.bgi_country = 'United States'
      AND YEAR(bgi.{BGI_DATE_COL}) >= {FULL_MIN_YEAR}
    GROUP BY 1, 2
    UNION ALL
    SELECT 'Full_LC' AS SOURCE,
           DATE_TRUNC('month', lc.{LC_DATE_COL}) AS MONTH_START,
           COUNT(DISTINCT lc.id) AS CNT
    FROM {LC_POSTINGS} lc
    WHERE YEAR(lc.{LC_DATE_COL}) >= {FULL_MIN_YEAR}
    GROUP BY 1, 2
    """


# ── JOLTS industry comparison SQL (JOLTS + Full_BGI + Full_LC by sector) ─
def sql_jolts_industry_comparison():
    bgi_sector = _naics2_to_sector("n.naics_2022_2_name")
    lc_sector = _naics2_to_sector("lc.naics_2022_2_name")
    jolts_sector = _jolts_code_to_sector()
    jolts_codes = "'" + "','".join(JOLTS_CODE_MAP.keys()) + "'"
    return f"""
    WITH
    jolts AS (
        SELECT {jolts_sector} AS SECTOR,
               SUM(j."job_openings") AS JOLTS_COUNT
        FROM {JOLTS_SF_TABLE} j
        WHERE j."state_text" = 'Total US'
          AND j."industry_code" IN ({jolts_codes})
          AND j."year" = '2024'
        GROUP BY 1
    ),
    jolts_total AS (SELECT SUM(JOLTS_COUNT) AS total_jolts FROM jolts),
    bgi_by_sector AS (
        SELECT {bgi_sector} AS SECTOR, COUNT(DISTINCT bgi.job_id) AS BGI_COUNT
        FROM {BGI_POSTINGS} bgi
        INNER JOIN temporary_data.sswee.naics_full_hierarchy n
            ON n.naics_2022_6 = bgi.bgi_naics6
        WHERE bgi.bgi_country = 'United States'
          AND YEAR(bgi.{BGI_DATE_COL}) = 2024
          AND {bgi_sector} IS NOT NULL
        GROUP BY 1
    ),
    bgi_total AS (SELECT SUM(BGI_COUNT) AS total_bgi FROM bgi_by_sector),
    lc_by_sector AS (
        SELECT {lc_sector} AS SECTOR, COUNT(DISTINCT lc.id) AS LC_COUNT
        FROM {LC_POSTINGS} lc
        WHERE YEAR(lc.{LC_DATE_COL}) = 2024
          AND {lc_sector} IS NOT NULL
        GROUP BY 1
    ),
    lc_total AS (SELECT SUM(LC_COUNT) AS total_lc FROM lc_by_sector),
    all_sectors AS (
        SELECT SECTOR FROM jolts WHERE SECTOR IS NOT NULL
        UNION SELECT SECTOR FROM bgi_by_sector
        UNION SELECT SECTOR FROM lc_by_sector
    )
    SELECT
        s.SECTOR,
        COALESCE(j.JOLTS_COUNT, 0) AS JOLTS_COUNT,
        CASE WHEN jt.total_jolts > 0
             THEN COALESCE(j.JOLTS_COUNT, 0) / jt.total_jolts ELSE 0 END AS JOLTS_PCT,
        COALESCE(b.BGI_COUNT, 0) AS BGI_COUNT,
        CASE WHEN bt.total_bgi > 0
             THEN COALESCE(b.BGI_COUNT, 0) / bt.total_bgi ELSE 0 END AS BGI_PCT,
        COALESCE(l.LC_COUNT, 0) AS LC_COUNT,
        CASE WHEN lt.total_lc > 0
             THEN COALESCE(l.LC_COUNT, 0) / lt.total_lc ELSE 0 END AS LC_PCT
    FROM all_sectors s
    LEFT JOIN jolts j ON s.SECTOR = j.SECTOR
    LEFT JOIN bgi_by_sector b ON s.SECTOR = b.SECTOR
    LEFT JOIN lc_by_sector l ON s.SECTOR = l.SECTOR
    CROSS JOIN jolts_total jt
    CROSS JOIN bgi_total bt
    CROSS JOIN lc_total lt
    ORDER BY JOLTS_COUNT DESC
    """


# ── OEWS SOC2 comparison SQL (OEWS + Full_BGI + Full_LC by SOC2) ────────
def sql_oews_soc2_comparison():
    return f"""
    WITH
    bgi_soc2 AS (
        SELECT UPPER(TRIM(lu.bgi_soc2_name)) AS SOC2_NAME,
               COUNT(DISTINCT bgi.job_id) AS BGI_COUNT
        FROM {BGI_POSTINGS} bgi
        INNER JOIN temporary_data.jnania.onet_soc_lookup lu
            ON lu.bgi_onet = bgi.onet_code
        WHERE bgi.bgi_country = 'United States'
          AND YEAR(bgi.{BGI_DATE_COL}) >= {FULL_MIN_YEAR}
          AND lu.bgi_soc2_name IS NOT NULL
        GROUP BY 1
    ),
    bgi_total AS (SELECT SUM(BGI_COUNT) AS total_bgi FROM bgi_soc2),
    lc_soc2 AS (
        SELECT UPPER(TRIM(lc.soc_2_name)) AS SOC2_NAME,
               COUNT(DISTINCT lc.id) AS LC_COUNT
        FROM {LC_POSTINGS} lc
        WHERE YEAR(lc.{LC_DATE_COL}) >= {FULL_MIN_YEAR}
          AND lc.soc_2_name IS NOT NULL
        GROUP BY 1
    ),
    lc_total AS (SELECT SUM(LC_COUNT) AS total_lc FROM lc_soc2),
    oews AS (
        SELECT UPPER(TRIM(OCC_TITLE)) AS SOC2_NAME, TOT_EMP AS OEWS_EMPL
        FROM OEWS_NATIONAL_RAW
    ),
    oews_total AS (SELECT SUM(OEWS_EMPL) AS total_oews FROM oews),
    all_soc2 AS (
        SELECT SOC2_NAME FROM bgi_soc2
        UNION SELECT SOC2_NAME FROM lc_soc2
        UNION SELECT SOC2_NAME FROM oews
    )
    SELECT
        s.SOC2_NAME,
        COALESCE(b.BGI_COUNT, 0) AS BGI_COUNT,
        CASE WHEN bt.total_bgi > 0
             THEN COALESCE(b.BGI_COUNT, 0) / bt.total_bgi ELSE 0 END AS BGI_PCT,
        COALESCE(l.LC_COUNT, 0) AS LC_COUNT,
        CASE WHEN lt.total_lc > 0
             THEN COALESCE(l.LC_COUNT, 0) / lt.total_lc ELSE 0 END AS LC_PCT,
        COALESCE(o.OEWS_EMPL, 0) AS OEWS_EMPL,
        CASE WHEN ot.total_oews > 0
             THEN COALESCE(o.OEWS_EMPL, 0) / ot.total_oews ELSE 0 END AS OEWS_PCT
    FROM all_soc2 s
    LEFT JOIN bgi_soc2 b ON s.SOC2_NAME = b.SOC2_NAME
    LEFT JOIN lc_soc2 l ON s.SOC2_NAME = l.SOC2_NAME
    LEFT JOIN oews o ON s.SOC2_NAME = o.SOC2_NAME
    CROSS JOIN bgi_total bt
    CROSS JOIN lc_total lt
    CROSS JOIN oews_total ot
    ORDER BY OEWS_EMPL DESC
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

    # ── 3. Public data comparisons ──────────────────────────────────────

    # Upload JOLTS Excel → staging table
    print(f"\n[{datetime.now()}] Uploading JOLTS monthly data from Excel...")
    try:
        load_and_upload_jolts(conn)
        print(f"  ✓ Uploaded JOLTS_MONTHLY_RAW")
        total_tables += 1
    except Exception as e:
        print(f"  ✗ Failed to upload JOLTS data: {e}")

    # JOLTS monthly comparison (JOLTS + Full_BGI + Full_LC)
    print(f"\n[{datetime.now()}] Creating JOLTS_MONTHLY_COMPARISON...")
    try:
        execute_ddl(f"CREATE OR REPLACE TABLE JOLTS_MONTHLY_COMPARISON AS\n{sql_jolts_monthly_comparison()}", conn)
        print(f"  ✓ Created JOLTS_MONTHLY_COMPARISON")
        total_tables += 1
    except Exception as e:
        print(f"  ✗ Failed to create JOLTS_MONTHLY_COMPARISON: {e}")

    # JOLTS industry comparison (JOLTS + Full_BGI + Full_LC by sector, 2024)
    print(f"\n[{datetime.now()}] Creating JOLTS_INDUSTRY_COMPARISON...")
    try:
        execute_ddl(f"CREATE OR REPLACE TABLE JOLTS_INDUSTRY_COMPARISON AS\n{sql_jolts_industry_comparison()}", conn)
        print(f"  ✓ Created JOLTS_INDUSTRY_COMPARISON")
        total_tables += 1
    except Exception as e:
        print(f"  ✗ Failed to create JOLTS_INDUSTRY_COMPARISON: {e}")

    # Upload OEWS Excel → staging table
    print(f"\n[{datetime.now()}] Uploading OEWS national data from Excel...")
    try:
        load_and_upload_oews(conn)
        print(f"  ✓ Uploaded OEWS_NATIONAL_RAW")
        total_tables += 1
    except Exception as e:
        print(f"  ✗ Failed to upload OEWS data: {e}")

    # OEWS SOC2 comparison (OEWS + Full_BGI + Full_LC by SOC2)
    print(f"\n[{datetime.now()}] Creating OEWS_SOC2_COMPARISON...")
    try:
        execute_ddl(f"CREATE OR REPLACE TABLE OEWS_SOC2_COMPARISON AS\n{sql_oews_soc2_comparison()}", conn)
        print(f"  ✓ Created OEWS_SOC2_COMPARISON")
        total_tables += 1
    except Exception as e:
        print(f"  ✗ Failed to create OEWS_SOC2_COMPARISON: {e}")

    conn.close()
    print(f"\n[{datetime.now()}] ═══════════════════════════════════════")
    print(f"[{datetime.now()}] Complete! Created {total_tables} tables in {database}.{schema}")
    print(f"[{datetime.now()}] ═══════════════════════════════════════")

if __name__ == "__main__":
    main()
