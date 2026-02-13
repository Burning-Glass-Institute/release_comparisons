# postings_release_temp_tables.py
# Pre-compute BGI v1 vs v2 vs Lightcast postings comparison tables
# for a Streamlit release-comparison dashboard.
#
#   v1:  revelio_clean.v1.bgi_postings
#   v2:  revelio_clean.bgi_2026_02.bgi_postings
#   lc:  bgi_postings_backups.dec_25.us_postings
#
# Output: PROJECT_DATA.POSTINGS_RELEASE_COMPARISONS

import snowflake.connector as snow
from datetime import datetime
import os
import importlib.util
import requests
import json

# ─────────────────────────── SOURCE TABLES ───────────────────────────
V1_TABLE = "revelio_clean.v1.bgi_postings"
V2_TABLE = "revelio_clean.bgi_2026_02.bgi_postings"
LC_TABLE = "bgi_postings_backups.dec_25.us_postings"
SALARY_TABLE = "temporary_data.dsexton.postings_salary_feb2026"
ONET_SOC_LOOKUP = "temporary_data.jnania.onet_soc_lookup"

# Date / ID columns
V1_DATE = "post_date"
V2_DATE = "post_date"
LC_DATE = "posted"
V1_ID = "job_id"
V2_ID = "job_id"
LC_ID = "id"

FULL_MIN_YEAR = 2015
TABLE_PREFIX = "BGI_REL"

# ─────────────────────────── BLS JOLTS STATE DATA ────────────────────────
BLS_API_KEY = "3de7ff442de94a6e9b5fd3249b98b077"
BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

STATE_FIPS = {
    "01": "ALABAMA", "02": "ALASKA", "04": "ARIZONA", "05": "ARKANSAS",
    "06": "CALIFORNIA", "08": "COLORADO", "09": "CONNECTICUT", "10": "DELAWARE",
    "11": "DISTRICT OF COLUMBIA", "12": "FLORIDA", "13": "GEORGIA", "15": "HAWAII",
    "16": "IDAHO", "17": "ILLINOIS", "18": "INDIANA", "19": "IOWA",
    "20": "KANSAS", "21": "KENTUCKY", "22": "LOUISIANA", "23": "MAINE",
    "24": "MARYLAND", "25": "MASSACHUSETTS", "26": "MICHIGAN", "27": "MINNESOTA",
    "28": "MISSISSIPPI", "29": "MISSOURI", "30": "MONTANA", "31": "NEBRASKA",
    "32": "NEVADA", "33": "NEW HAMPSHIRE", "34": "NEW JERSEY", "35": "NEW MEXICO",
    "36": "NEW YORK", "37": "NORTH CAROLINA", "38": "NORTH DAKOTA", "39": "OHIO",
    "40": "OKLAHOMA", "41": "OREGON", "42": "PENNSYLVANIA", "44": "RHODE ISLAND",
    "45": "SOUTH CAROLINA", "46": "SOUTH DAKOTA", "47": "TENNESSEE", "48": "TEXAS",
    "49": "UTAH", "50": "VERMONT", "51": "VIRGINIA", "53": "WASHINGTON",
    "54": "WEST VIRGINIA", "55": "WISCONSIN", "56": "WYOMING",
}

# ─────────────────────────── SNOWFLAKE CONNECTION ─────────────────────────
config_file_path = r"C:\Users\JuliaNania\OneDrive - Burning Glass Institute\Documents\Python\config.py"

if not os.path.isfile(config_file_path):
    raise FileNotFoundError(f"Config file not found: {config_file_path}")

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


# ─────────────────────────── HELPERS ────────────────────────────────
def _norm(expr):
    return f"UPPER(TRIM({expr}))"


def _esc(val):
    return val.replace("'", "''")


YEARLY_SALARY_EXPR = """
    CASE
        WHEN LOWER(sal.SALARY_PER) = 'year'  THEN sal.PARSED_SALARY_MIN
        WHEN LOWER(sal.SALARY_PER) = 'hour'  THEN sal.PARSED_SALARY_MIN * 2000
        WHEN LOWER(sal.SALARY_PER) = 'week'  THEN sal.PARSED_SALARY_MIN * 50
        WHEN LOWER(sal.SALARY_PER) = 'day'   THEN sal.PARSED_SALARY_MIN * 260
        WHEN LOWER(sal.SALARY_PER) = 'month' THEN sal.PARSED_SALARY_MIN * 12
        WHEN LOWER(sal.SALARY_PER) = 'piece' THEN NULL
        ELSE sal.PARSED_SALARY_MIN
    END"""


# ─────────────────────────── TOPIC METADATA ─────────────────────────────
# Default FROM clauses (overridden per-field when JOINs are needed)
_V1_FROM = f"{V1_TABLE} v1"
_V2_FROM = f"{V2_TABLE} v2"
_LC_FROM = f"{LC_TABLE} lc"

TOPICS = {
    "employers": {
        "fields": {
            "EMPLOYER_NAME": {
                "v1_expr": "v1.COMPANY_NAME",
                "v2_expr": "v2.COMPANY_NAME",
                "lc_expr": "lc.COMPANY_NAME",
            }
        },
        "exclude_values_ilike": ["%Unclassified%"],
        "extra_where": {
            "v1": "\n      AND v1.BGI_NAICS4 != '5613'",
            "v2": "\n      AND v2.BGI_NAICS4 != '5613'",
            "lc": "",
        }
    },
    "industry_naics2": {
        "fields": {
            "NAICS2_DISTRIBUTION": {
                "v1_expr": "v1.BGI_NAICS2_NAME",
                "v2_expr": "v2.BGI_NAICS2_NAME",
                "lc_expr": "lc.NAICS2_NAME",
            }
        },
        "exclude_values_ilike": ["%Unclassified%"]
    },
    "education": {
        "fields": {
            "MIN_REQUIRED_DEGREE": {
                "v1_expr": "NULLIF(NULLIF(NULLIF(v1.BGI_MIN_REQUIRED_DEGREE, 'No Education Found'), 'None'), 'No Education Listed')",
                "v2_expr": "NULLIF(NULLIF(NULLIF(v2.BGI_MIN_REQUIRED_DEGREE, 'No Education Found'), 'None'), 'No Education Listed')",
                "lc_expr": """CASE
                    WHEN UPPER(TRIM(lc.MIN_EDULEVELS_NAME)) = 'HIGH SCHOOL OR GED'
                    THEN 'HIGH SCHOOL/GED'
                    WHEN UPPER(TRIM(lc.MIN_EDULEVELS_NAME)) = 'PH.D. OR PROFESSIONAL DEGREE'
                    THEN 'DOCTORATE DEGREE'
                    ELSE lc.MIN_EDULEVELS_NAME
                END""",
            }
        },
        "exclude_values_ilike": []
    },
    "occupation": {
        "fields": {
            "ONET_NAME": {
                "v1_expr": "v1.BGI_ONET_NAME",
                "v2_expr": "v2.BGI_ONET_NAME",
                "lc_expr": "lc.ONET_2019_NAME",
            },
            "SOC2": {
                "v1_expr": "lu1.bgi_soc2_name",
                "v2_expr": "lu2.bgi_soc2_name",
                "lc_expr": "lc.soc_2_name",
                "v1_from": f"""{V1_TABLE} v1
                    INNER JOIN {ONET_SOC_LOOKUP} lu1 ON lu1.bgi_onet = v1.onet_code""",
                "v2_from": f"""{V2_TABLE} v2
                    INNER JOIN {ONET_SOC_LOOKUP} lu2 ON lu2.bgi_onet = v2.onet_code""",
            }
        },
        "exclude_values_ilike": ["%Unclassified%"]
    },
    "location": {
        "fields": {
            "CITY": {
                "v1_expr": "v1.BGI_CITY",
                "v2_expr": "v2.BGI_CITY",
                "lc_expr": "SUBSTR(lc.CITY_NAME, 1, LENGTH(lc.CITY_NAME) - 4)",
            },
            "STATE": {
                "v1_expr": "v1.BGI_STATE",
                "v2_expr": "v2.BGI_STATE",
                "lc_expr": "lc.STATE_NAME",
            },
            "MSA": {
                "v1_expr": "v1.BGI_MSA",
                "v2_expr": "v2.BGI_MSA",
                "lc_expr": "lc.MSA_NAME",
            },
        },
        "exclude_values_ilike": []
    },
    "titles": {
        "fields": {
            "TITLE_NAME": {
                "v1_expr": "v1.BGI_TITLE_NAME",
                "v2_expr": "v2.BGI_TITLE_NAME",
                "lc_expr": "lc.TITLE_NAME",
            }
        },
        "exclude_values_ilike": ["%Unclassified%"]
    },
}


# ─────────────────────────── TABLE NAME HELPERS ─────────────────────────
def _clean(s):
    return s.upper().replace(" ", "_").replace("-", "_").replace("'", "")


def make_kpi_name(topic, field):
    return f"{TABLE_PREFIX}_KPI_{_clean(topic)}_{_clean(field)}"


def make_comp_name(topic, field):
    return f"{TABLE_PREFIX}_COMP_{_clean(topic)}_{_clean(field)}"


def make_comp_lc_name(topic, field):
    return f"{TABLE_PREFIX}_COMPLC_{_clean(topic)}_{_clean(field)}"


# ─────────────────────────── TOTAL COUNTS ────────────────────────────────

def sql_total_counts_yearly():
    return f"""
    SELECT 'v1' AS SOURCE, YEAR({V1_DATE}) AS YR, COUNT(DISTINCT {V1_ID}) AS CNT
    FROM {V1_TABLE}
    WHERE bgi_country = 'United States' AND YEAR({V1_DATE}) >= {FULL_MIN_YEAR}
    GROUP BY 2
    UNION ALL
    SELECT 'v2' AS SOURCE, YEAR({V2_DATE}) AS YR, COUNT(DISTINCT {V2_ID}) AS CNT
    FROM {V2_TABLE}
    WHERE bgi_country = 'United States' AND YEAR({V2_DATE}) >= {FULL_MIN_YEAR}
    GROUP BY 2
    UNION ALL
    SELECT 'lc' AS SOURCE, YEAR({LC_DATE}) AS YR, COUNT(DISTINCT {LC_ID}) AS CNT
    FROM {LC_TABLE}
    WHERE YEAR({LC_DATE}) >= {FULL_MIN_YEAR}
    GROUP BY 2
    """


def sql_total_counts_monthly():
    return f"""
    WITH months AS (
      SELECT DATE_TRUNC('month', DATEADD('month', seq4(),
                 DATEADD('month', -11, DATE_TRUNC('month', CURRENT_DATE())))) AS month_start
      FROM TABLE(GENERATOR(ROWCOUNT => 12))
    )
    SELECT 'v1' AS SOURCE, m.month_start, COALESCE(d.cnt, 0) AS CNT
    FROM months m
    LEFT JOIN (
      SELECT DATE_TRUNC('month', {V1_DATE}) AS month_start, COUNT(DISTINCT {V1_ID}) AS cnt
      FROM {V1_TABLE}
      WHERE bgi_country = 'United States'
        AND {V1_DATE} >= DATEADD('month', -11, DATE_TRUNC('month', CURRENT_DATE()))
      GROUP BY 1
    ) d USING (month_start)
    UNION ALL
    SELECT 'v2' AS SOURCE, m.month_start, COALESCE(d.cnt, 0) AS CNT
    FROM months m
    LEFT JOIN (
      SELECT DATE_TRUNC('month', {V2_DATE}) AS month_start, COUNT(DISTINCT {V2_ID}) AS cnt
      FROM {V2_TABLE}
      WHERE bgi_country = 'United States'
        AND {V2_DATE} >= DATEADD('month', -11, DATE_TRUNC('month', CURRENT_DATE()))
      GROUP BY 1
    ) d USING (month_start)
    UNION ALL
    SELECT 'lc' AS SOURCE, m.month_start, COALESCE(d.cnt, 0) AS CNT
    FROM months m
    LEFT JOIN (
      SELECT DATE_TRUNC('month', {LC_DATE}) AS month_start, COUNT(DISTINCT {LC_ID}) AS cnt
      FROM {LC_TABLE}
      WHERE {LC_DATE} >= DATEADD('month', -11, DATE_TRUNC('month', CURRENT_DATE()))
      GROUP BY 1
    ) d USING (month_start)
    """


# ─────────────────────────── GENERIC KPI BUILDER ─────────────────────────

def sql_generic_kpis(v1_expr, v2_expr, lc_expr,
                     v1_from, v2_from, lc_from,
                     exclude_ilike=None, extra_where=None):
    """KPI totals + coverage for v1, v2, lc (filtered to >= FULL_MIN_YEAR)"""
    v1_val, v2_val, lc_val = _norm(v1_expr), _norm(v2_expr), _norm(lc_expr)

    excl_v1 = excl_v2 = excl_lc = ""
    if exclude_ilike:
        for pat in exclude_ilike:
            p = _esc(pat).upper()
            excl_v1 += f"\n      AND {v1_val} NOT LIKE '{p}'"
            excl_v2 += f"\n      AND {v2_val} NOT LIKE '{p}'"
            excl_lc += f"\n      AND {lc_val} NOT LIKE '{p}'"

    ew_v1 = extra_where.get("v1", "") if extra_where else ""
    ew_v2 = extra_where.get("v2", "") if extra_where else ""
    ew_lc = extra_where.get("lc", "") if extra_where else ""

    return f"""
WITH
v1_total AS (
    SELECT COUNT(DISTINCT v1.{V1_ID}) AS total_v1
    FROM {V1_TABLE} v1
    WHERE v1.bgi_country = 'United States' AND YEAR(v1.{V1_DATE}) >= {FULL_MIN_YEAR}{ew_v1}
),
v1_cov AS (
    SELECT COUNT(DISTINCT v1.{V1_ID}) AS covered_v1
    FROM {v1_from}
    WHERE v1.bgi_country = 'United States' AND YEAR(v1.{V1_DATE}) >= {FULL_MIN_YEAR}
      AND {v1_val} IS NOT NULL{excl_v1}{ew_v1}
),
v2_total AS (
    SELECT COUNT(DISTINCT v2.{V2_ID}) AS total_v2
    FROM {V2_TABLE} v2
    WHERE v2.bgi_country = 'United States' AND YEAR(v2.{V2_DATE}) >= {FULL_MIN_YEAR}{ew_v2}
),
v2_cov AS (
    SELECT COUNT(DISTINCT v2.{V2_ID}) AS covered_v2
    FROM {v2_from}
    WHERE v2.bgi_country = 'United States' AND YEAR(v2.{V2_DATE}) >= {FULL_MIN_YEAR}
      AND {v2_val} IS NOT NULL{excl_v2}{ew_v2}
),
lc_total AS (
    SELECT COUNT(DISTINCT lc.{LC_ID}) AS total_lc
    FROM {LC_TABLE} lc
    WHERE YEAR(lc.{LC_DATE}) >= {FULL_MIN_YEAR}{ew_lc}
),
lc_cov AS (
    SELECT COUNT(DISTINCT lc.{LC_ID}) AS covered_lc
    FROM {lc_from}
    WHERE YEAR(lc.{LC_DATE}) >= {FULL_MIN_YEAR}
      AND {lc_val} IS NOT NULL{excl_lc}{ew_lc}
)
SELECT total_v1, covered_v1, total_v2, covered_v2, total_lc, covered_lc
FROM v1_total, v1_cov, v2_total, v2_cov, lc_total, lc_cov
"""


# ─────────────────────────── GENERIC COMP BUILDER ────────────────────────

def sql_generic_compare(v1_expr, v2_expr, lc_expr,
                        v1_from, v2_from, lc_from,
                        exclude_ilike=None, extra_where=None):
    """Distribution comparison for a single field across v1, v2, lc"""
    v1_val, v2_val, lc_val = _norm(v1_expr), _norm(v2_expr), _norm(lc_expr)

    excl_v1 = excl_v2 = excl_lc = ""
    if exclude_ilike:
        for pat in exclude_ilike:
            p = _esc(pat).upper()
            excl_v1 += f"\n      AND {v1_val} NOT LIKE '{p}'"
            excl_v2 += f"\n      AND {v2_val} NOT LIKE '{p}'"
            excl_lc += f"\n      AND {lc_val} NOT LIKE '{p}'"

    ew_v1 = extra_where.get("v1", "") if extra_where else ""
    ew_v2 = extra_where.get("v2", "") if extra_where else ""
    ew_lc = extra_where.get("lc", "") if extra_where else ""

    return f"""
WITH
v1_ind AS (
    SELECT {v1_val} AS field_value, COUNT(DISTINCT v1.{V1_ID}) AS v1_count
    FROM {v1_from}
    WHERE v1.bgi_country = 'United States' AND YEAR(v1.{V1_DATE}) = 2025
      AND {v1_val} IS NOT NULL{excl_v1}{ew_v1}
    GROUP BY 1
),
v1_total AS (
    SELECT COUNT(DISTINCT v1.{V1_ID}) AS total_v1
    FROM {V1_TABLE} v1
    WHERE v1.bgi_country = 'United States' AND YEAR(v1.{V1_DATE}) = 2025{ew_v1}
),
v2_ind AS (
    SELECT {v2_val} AS field_value, COUNT(DISTINCT v2.{V2_ID}) AS v2_count
    FROM {v2_from}
    WHERE v2.bgi_country = 'United States' AND YEAR(v2.{V2_DATE}) = 2025
      AND {v2_val} IS NOT NULL{excl_v2}{ew_v2}
    GROUP BY 1
),
v2_total AS (
    SELECT COUNT(DISTINCT v2.{V2_ID}) AS total_v2
    FROM {V2_TABLE} v2
    WHERE v2.bgi_country = 'United States' AND YEAR(v2.{V2_DATE}) = 2025{ew_v2}
),
lc_ind AS (
    SELECT {lc_val} AS field_value, COUNT(DISTINCT lc.{LC_ID}) AS lc_count
    FROM {lc_from}
    WHERE YEAR(lc.{LC_DATE}) = 2025
      AND {lc_val} IS NOT NULL{excl_lc}{ew_lc}
    GROUP BY 1
),
lc_total AS (
    SELECT COUNT(DISTINCT lc.{LC_ID}) AS total_lc
    FROM {LC_TABLE} lc
    WHERE YEAR(lc.{LC_DATE}) = 2025{ew_lc}
),
all_values AS (
    SELECT field_value FROM v1_ind
    UNION SELECT field_value FROM v2_ind
    UNION SELECT field_value FROM lc_ind
)
SELECT
    av.field_value,
    COALESCE(v1_ind.v1_count, 0) AS v1_count,
    CASE WHEN v1_total.total_v1 > 0
         THEN COALESCE(v1_ind.v1_count, 0) / v1_total.total_v1
         ELSE 0 END AS v1_frac,
    COALESCE(v2_ind.v2_count, 0) AS v2_count,
    CASE WHEN v2_total.total_v2 > 0
         THEN COALESCE(v2_ind.v2_count, 0) / v2_total.total_v2
         ELSE 0 END AS v2_frac,
    COALESCE(lc_ind.lc_count, 0) AS lc_count,
    CASE WHEN lc_total.total_lc > 0
         THEN COALESCE(lc_ind.lc_count, 0) / lc_total.total_lc
         ELSE 0 END AS lc_frac
FROM all_values av
LEFT JOIN v1_ind ON av.field_value IS NOT DISTINCT FROM v1_ind.field_value
LEFT JOIN v2_ind ON av.field_value IS NOT DISTINCT FROM v2_ind.field_value
LEFT JOIN lc_ind ON av.field_value IS NOT DISTINCT FROM lc_ind.field_value
CROSS JOIN v1_total
CROSS JOIN v2_total
CROSS JOIN lc_total
ORDER BY v2_count DESC NULLS LAST, v1_count DESC NULLS LAST
"""


# ─────────────────────── LC-SORTED COMP BUILDER (top 25 by LC) ───────────

def sql_generic_compare_lc(v1_expr, v2_expr, lc_expr,
                           v1_from, v2_from, lc_from,
                           exclude_ilike=None, extra_where=None):
    """Same as sql_generic_compare but sorted by lc_count and limited to top 25."""
    v1_val, v2_val, lc_val = _norm(v1_expr), _norm(v2_expr), _norm(lc_expr)

    excl_v1 = excl_v2 = excl_lc = ""
    if exclude_ilike:
        for pat in exclude_ilike:
            p = _esc(pat).upper()
            excl_v1 += f"\n      AND {v1_val} NOT LIKE '{p}'"
            excl_v2 += f"\n      AND {v2_val} NOT LIKE '{p}'"
            excl_lc += f"\n      AND {lc_val} NOT LIKE '{p}'"

    ew_v1 = extra_where.get("v1", "") if extra_where else ""
    ew_v2 = extra_where.get("v2", "") if extra_where else ""
    ew_lc = extra_where.get("lc", "") if extra_where else ""

    return f"""
WITH
lc_top AS (
    SELECT {lc_val} AS field_value, COUNT(DISTINCT lc.{LC_ID}) AS lc_count
    FROM {lc_from}
    WHERE YEAR(lc.{LC_DATE}) = 2025
      AND {lc_val} IS NOT NULL{excl_lc}{ew_lc}
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 25
),
lc_total AS (
    SELECT COUNT(DISTINCT lc.{LC_ID}) AS total_lc
    FROM {LC_TABLE} lc
    WHERE YEAR(lc.{LC_DATE}) = 2025{ew_lc}
),
v1_ind AS (
    SELECT {v1_val} AS field_value, COUNT(DISTINCT v1.{V1_ID}) AS v1_count
    FROM {v1_from}
    WHERE v1.bgi_country = 'United States' AND YEAR(v1.{V1_DATE}) = 2025
      AND {v1_val} IS NOT NULL{excl_v1}{ew_v1}
    GROUP BY 1
),
v1_total AS (
    SELECT COUNT(DISTINCT v1.{V1_ID}) AS total_v1
    FROM {V1_TABLE} v1
    WHERE v1.bgi_country = 'United States' AND YEAR(v1.{V1_DATE}) = 2025{ew_v1}
),
v2_ind AS (
    SELECT {v2_val} AS field_value, COUNT(DISTINCT v2.{V2_ID}) AS v2_count
    FROM {v2_from}
    WHERE v2.bgi_country = 'United States' AND YEAR(v2.{V2_DATE}) = 2025
      AND {v2_val} IS NOT NULL{excl_v2}{ew_v2}
    GROUP BY 1
),
v2_total AS (
    SELECT COUNT(DISTINCT v2.{V2_ID}) AS total_v2
    FROM {V2_TABLE} v2
    WHERE v2.bgi_country = 'United States' AND YEAR(v2.{V2_DATE}) = 2025{ew_v2}
)
SELECT
    lt.field_value,
    COALESCE(v1_ind.v1_count, 0) AS v1_count,
    CASE WHEN v1_total.total_v1 > 0
         THEN COALESCE(v1_ind.v1_count, 0) / v1_total.total_v1
         ELSE 0 END AS v1_frac,
    COALESCE(v2_ind.v2_count, 0) AS v2_count,
    CASE WHEN v2_total.total_v2 > 0
         THEN COALESCE(v2_ind.v2_count, 0) / v2_total.total_v2
         ELSE 0 END AS v2_frac,
    lt.lc_count,
    CASE WHEN lc_total.total_lc > 0
         THEN lt.lc_count / lc_total.total_lc
         ELSE 0 END AS lc_frac
FROM lc_top lt
LEFT JOIN v1_ind ON lt.field_value IS NOT DISTINCT FROM v1_ind.field_value
LEFT JOIN v2_ind ON lt.field_value IS NOT DISTINCT FROM v2_ind.field_value
CROSS JOIN v1_total
CROSS JOIN v2_total
CROSS JOIN lc_total
ORDER BY lt.lc_count DESC
"""


# ─────────────────────────── ONET CHANGES (v1 vs v2) ────────────────────

def sql_onet_changes():
    return f"""
    SELECT v1.ONET_CODE AS V1_ONET,
           v1.BGI_ONET_NAME AS V1_ONET_NAME,
           v2.ONET_CODE AS V2_ONET,
           v2.BGI_ONET_NAME AS V2_ONET_NAME,
           COUNT(*) AS POSTING_COUNT
    FROM {V1_TABLE} v1
    INNER JOIN {V2_TABLE} v2 ON v2.{V2_ID} = v1.{V1_ID}
    WHERE v1.bgi_country = 'United States'
      AND YEAR(v1.{V1_DATE}) = 2024
      AND v1.ONET_CODE IS NOT NULL
      AND v2.ONET_CODE IS NOT NULL
      AND v1.ONET_CODE != v2.ONET_CODE
    GROUP BY 1, 2, 3, 4
    ORDER BY 5 DESC
    LIMIT 30
    """


def sql_onet_change_summary():
    return f"""
    SELECT
        SUM(CASE WHEN v1.ONET_CODE = v2.ONET_CODE THEN 1 ELSE 0 END) AS SAME_ONET,
        SUM(CASE WHEN v1.ONET_CODE != v2.ONET_CODE THEN 1 ELSE 0 END) AS DIFFERENT_ONET,
        COUNT(*) AS TOTAL_MATCHED
    FROM {V1_TABLE} v1
    INNER JOIN {V2_TABLE} v2 ON v2.{V2_ID} = v1.{V1_ID}
    WHERE v1.bgi_country = 'United States'
      AND YEAR(v1.{V1_DATE}) = 2024
      AND v1.ONET_CODE IS NOT NULL
      AND v2.ONET_CODE IS NOT NULL
    """


# ─────────────────────────── SALARY (v1 vs v2 only) ─────────────────────

def sql_salary_stats():
    return f"""
    SELECT 'v1' AS SOURCE,
           MIN({YEARLY_SALARY_EXPR}) AS MIN_SALARY,
           MAX({YEARLY_SALARY_EXPR}) AS MAX_SALARY,
           AVG({YEARLY_SALARY_EXPR}) AS AVG_SALARY,
           MEDIAN({YEARLY_SALARY_EXPR}) AS MEDIAN_SALARY,
           COUNT(*) AS N_POSTINGS_WITH_SALARY
    FROM {V1_TABLE} p
    INNER JOIN {SALARY_TABLE} sal ON sal.desc_id = p.desc_id
    WHERE sal.PARSED_SALARY_MIN IS NOT NULL AND sal.PARSED_SALARY_MIN > 0
      AND p.bgi_country = 'United States'
    UNION ALL
    SELECT 'v2' AS SOURCE,
           MIN({YEARLY_SALARY_EXPR}) AS MIN_SALARY,
           MAX({YEARLY_SALARY_EXPR}) AS MAX_SALARY,
           AVG({YEARLY_SALARY_EXPR}) AS AVG_SALARY,
           MEDIAN({YEARLY_SALARY_EXPR}) AS MEDIAN_SALARY,
           COUNT(*) AS N_POSTINGS_WITH_SALARY
    FROM {V2_TABLE} p
    INNER JOIN {SALARY_TABLE} sal ON sal.desc_id = p.desc_id
    WHERE sal.PARSED_SALARY_MIN IS NOT NULL AND sal.PARSED_SALARY_MIN > 0
      AND p.bgi_country = 'United States'
    """


def sql_salary_distribution():
    return f"""
    WITH v1_sal AS (
        SELECT {YEARLY_SALARY_EXPR} AS YEARLY_SALARY
        FROM {V1_TABLE} p
        INNER JOIN {SALARY_TABLE} sal ON sal.desc_id = p.desc_id
        WHERE sal.PARSED_SALARY_MIN IS NOT NULL AND sal.PARSED_SALARY_MIN > 0
          AND p.bgi_country = 'United States' AND YEAR(p.{V1_DATE}) = 2025
    ),
    v2_sal AS (
        SELECT {YEARLY_SALARY_EXPR} AS YEARLY_SALARY
        FROM {V2_TABLE} p
        INNER JOIN {SALARY_TABLE} sal ON sal.desc_id = p.desc_id
        WHERE sal.PARSED_SALARY_MIN IS NOT NULL AND sal.PARSED_SALARY_MIN > 0
          AND p.bgi_country = 'United States' AND YEAR(p.{V2_DATE}) = 2025
    )
    SELECT 'v1' AS SOURCE,
           CASE WHEN YEARLY_SALARY > 200000 THEN 210000
                ELSE FLOOR(YEARLY_SALARY / 10000) * 10000 END AS SALARY_BUCKET,
           COUNT(*) AS CNT
    FROM v1_sal WHERE YEARLY_SALARY IS NOT NULL
    GROUP BY 1, 2
    UNION ALL
    SELECT 'v2' AS SOURCE,
           CASE WHEN YEARLY_SALARY > 200000 THEN 210000
                ELSE FLOOR(YEARLY_SALARY / 10000) * 10000 END AS SALARY_BUCKET,
           COUNT(*) AS CNT
    FROM v2_sal WHERE YEARLY_SALARY IS NOT NULL
    GROUP BY 1, 2
    """


def sql_salary_by_soc2():
    return f"""
    SELECT 'v1' AS SOURCE, p.BGI_SOC2_NAME AS SOC2,
           MEDIAN({YEARLY_SALARY_EXPR}) AS MEDIAN_SALARY, COUNT(*) AS N_POSTINGS
    FROM {V1_TABLE} p
    INNER JOIN {SALARY_TABLE} sal ON sal.desc_id = p.desc_id
    WHERE sal.PARSED_SALARY_MIN IS NOT NULL AND sal.PARSED_SALARY_MIN > 0
      AND p.bgi_country = 'United States' AND p.BGI_SOC2_NAME IS NOT NULL
      AND YEAR(p.{V1_DATE}) = 2025
    GROUP BY 1, 2
    UNION ALL
    SELECT 'v2' AS SOURCE, p.BGI_SOC2_NAME AS SOC2,
           MEDIAN({YEARLY_SALARY_EXPR}) AS MEDIAN_SALARY, COUNT(*) AS N_POSTINGS
    FROM {V2_TABLE} p
    INNER JOIN {SALARY_TABLE} sal ON sal.desc_id = p.desc_id
    WHERE sal.PARSED_SALARY_MIN IS NOT NULL AND sal.PARSED_SALARY_MIN > 0
      AND p.bgi_country = 'United States' AND p.BGI_SOC2_NAME IS NOT NULL
      AND YEAR(p.{V2_DATE}) = 2025
    GROUP BY 1, 2
    """


def sql_salary_coverage():
    return f"""
    SELECT 'v1' AS SOURCE,
           CASE WHEN p.bgi_country = 'United States' THEN 'US'
                WHEN p.bgi_country = 'United Kingdom' THEN 'UK'
                WHEN p.bgi_country = 'Hong Kong'      THEN 'HK' END AS COUNTRY,
           COUNT(*) AS TOTAL_POSTINGS,
           SUM(CASE WHEN sal.PARSED_SALARY_MIN IS NOT NULL AND sal.PARSED_SALARY_MIN > 0
                    THEN 1 ELSE 0 END) AS POSTINGS_WITH_SALARY
    FROM {V1_TABLE} p
    LEFT JOIN {SALARY_TABLE} sal ON sal.desc_id = p.desc_id
    WHERE p.bgi_country IN ('United States','United Kingdom','Hong Kong')
      AND YEAR(p.{V1_DATE}) = 2025
    GROUP BY 1, 2
    UNION ALL
    SELECT 'v2' AS SOURCE,
           CASE WHEN p.bgi_country = 'United States' THEN 'US'
                WHEN p.bgi_country = 'United Kingdom' THEN 'UK'
                WHEN p.bgi_country = 'Hong Kong'      THEN 'HK' END AS COUNTRY,
           COUNT(*) AS TOTAL_POSTINGS,
           SUM(CASE WHEN sal.PARSED_SALARY_MIN IS NOT NULL AND sal.PARSED_SALARY_MIN > 0
                    THEN 1 ELSE 0 END) AS POSTINGS_WITH_SALARY
    FROM {V2_TABLE} p
    LEFT JOIN {SALARY_TABLE} sal ON sal.desc_id = p.desc_id
    WHERE p.bgi_country IN ('United States','United Kingdom','Hong Kong')
      AND YEAR(p.{V2_DATE}) = 2025
    GROUP BY 1, 2
    """


# ─────────────────────── JOLTS STATE COMPARISON ──────────────────────────

def fetch_and_upload_jolts_state(conn):
    """Fetch JOLTS state-level job openings (2024) via BLS API and upload to staging table."""
    # Build series IDs: JTS + 000000 (total nonfarm) + {state_fips} + 0000000 + JO + L
    series_ids = {f"JTS000000{code}0000000JOL": name for code, name in STATE_FIPS.items()}
    all_ids = list(series_ids.keys())

    # Fetch in 2 batches (API allows up to 50 per request)
    jolts_rows = []
    for batch in [all_ids[:26], all_ids[26:]]:
        payload = json.dumps({
            "seriesid": batch, "startyear": "2024", "endyear": "2024",
            "annualaverage": True, "registrationkey": BLS_API_KEY,
        })
        resp = requests.post(BLS_URL, data=payload, headers={"Content-type": "application/json"})
        resp.raise_for_status()
        data = resp.json()
        for series in data["Results"]["series"]:
            sid = series["seriesID"]
            state_name = series_ids[sid]
            monthly_vals = [
                float(item["value"]) * 1000
                for item in series["data"]
                if item["year"] == "2024" and item["period"] != "M13"
            ]
            if monthly_vals:
                jolts_rows.append((state_name, round(sum(monthly_vals) / len(monthly_vals))))

    # Upload to staging table
    raw_table = f"{TABLE_PREFIX}_JOLTS_STATE_RAW"
    execute_ddl(f"CREATE OR REPLACE TABLE {raw_table} (STATE VARCHAR, JOLTS_COUNT NUMBER)", conn)
    cur = conn.cursor()
    try:
        cur.executemany(
            f"INSERT INTO {raw_table} (STATE, JOLTS_COUNT) VALUES (%s, %s)",
            jolts_rows,
        )
        conn.commit()
    finally:
        cur.close()
    print(f"  Uploaded {len(jolts_rows)} states to {raw_table}")


def sql_jolts_state_comparison():
    """Join JOLTS state raw data with v1/v2/lc state counts; compute pct shares."""
    raw_table = f"{TABLE_PREFIX}_JOLTS_STATE_RAW"
    return f"""
WITH
jolts AS (SELECT STATE, JOLTS_COUNT FROM {raw_table}),
jolts_total AS (SELECT SUM(JOLTS_COUNT) AS total FROM jolts),
v1_state AS (
    SELECT UPPER(TRIM(v1.BGI_STATE)) AS STATE, COUNT(DISTINCT v1.{V1_ID}) AS V1_COUNT
    FROM {V1_TABLE} v1
    WHERE v1.BGI_STATE IS NOT NULL AND v1.bgi_country = 'United States'
      AND YEAR(v1.{V1_DATE}) = 2024
    GROUP BY 1
),
v1_total AS (SELECT SUM(V1_COUNT) AS total FROM v1_state),
v2_state AS (
    SELECT UPPER(TRIM(v2.BGI_STATE)) AS STATE, COUNT(DISTINCT v2.{V2_ID}) AS V2_COUNT
    FROM {V2_TABLE} v2
    WHERE v2.BGI_STATE IS NOT NULL AND v2.bgi_country = 'United States'
      AND YEAR(v2.{V2_DATE}) = 2024
    GROUP BY 1
),
v2_total AS (SELECT SUM(V2_COUNT) AS total FROM v2_state),
lc_state AS (
    SELECT UPPER(TRIM(lc.STATE_NAME)) AS STATE, COUNT(DISTINCT lc.{LC_ID}) AS LC_COUNT
    FROM {LC_TABLE} lc
    WHERE lc.STATE_NAME IS NOT NULL AND YEAR(lc.{LC_DATE}) = 2024
    GROUP BY 1
),
lc_total AS (SELECT SUM(LC_COUNT) AS total FROM lc_state)
SELECT
    j.STATE,
    j.JOLTS_COUNT,
    CASE WHEN jt.total > 0 THEN ROUND(j.JOLTS_COUNT / jt.total * 100, 1) ELSE 0 END AS JOLTS_PCT,
    COALESCE(v1.V1_COUNT, 0) AS V1_COUNT,
    CASE WHEN v1t.total > 0 THEN ROUND(COALESCE(v1.V1_COUNT, 0) / v1t.total * 100, 1) ELSE 0 END AS V1_PCT,
    COALESCE(v2.V2_COUNT, 0) AS V2_COUNT,
    CASE WHEN v2t.total > 0 THEN ROUND(COALESCE(v2.V2_COUNT, 0) / v2t.total * 100, 1) ELSE 0 END AS V2_PCT,
    COALESCE(lc.LC_COUNT, 0) AS LC_COUNT,
    CASE WHEN lct.total > 0 THEN ROUND(COALESCE(lc.LC_COUNT, 0) / lct.total * 100, 1) ELSE 0 END AS LC_PCT
FROM jolts j
LEFT JOIN v1_state v1 ON j.STATE = v1.STATE
LEFT JOIN v2_state v2 ON j.STATE = v2.STATE
LEFT JOIN lc_state lc ON j.STATE = lc.STATE
CROSS JOIN jolts_total jt
CROSS JOIN v1_total v1t
CROSS JOIN v2_total v2t
CROSS JOIN lc_total lct
ORDER BY JOLTS_PCT DESC
"""


# ─────────────────────────── MAIN ─────────────────────────────
def main():
    print(f"[{datetime.now()}] Starting BGI v1 vs v2 vs Lightcast temp table creation...")
    print(f"  v1: {V1_TABLE}")
    print(f"  v2: {V2_TABLE}")
    print(f"  lc: {LC_TABLE}")

    conn = snow.connect(
        user=user, password=password, account=account,
        warehouse=warehouse, database=database, schema=schema
    )
    print(f"[{datetime.now()}] Connected to {database}.{schema}")

    try:
        execute_ddl(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}", conn)
        print(f"[{datetime.now()}] Schema {schema} ready")
    except Exception as e:
        print(f"  Schema note: {e}")

    total = 0

    # ── 1. Total counts ──────────────────────────────────────────────────
    for name, sql_fn in [
        (f"{TABLE_PREFIX}_TOTAL_COUNTS_YEARLY", sql_total_counts_yearly),
        (f"{TABLE_PREFIX}_TOTAL_COUNTS_MONTHLY", sql_total_counts_monthly),
    ]:
        print(f"\n[{datetime.now()}] Creating {name}...")
        try:
            execute_ddl(f"CREATE OR REPLACE TABLE {name} AS\n{sql_fn()}", conn)
            print(f"  ✓ {name}")
            total += 1
        except Exception as e:
            print(f"  ✗ {name}: {e}")

    # ── 2. KPI + COMP per topic / field ──────────────────────────────────
    for topic, meta in TOPICS.items():
        exclude_ilike = meta.get("exclude_values_ilike", [])
        extra_where = meta.get("extra_where")

        for field_name, fmap in meta["fields"].items():
            v1_from = fmap.get("v1_from", _V1_FROM)
            v2_from = fmap.get("v2_from", _V2_FROM)
            lc_from = fmap.get("lc_from", _LC_FROM)

            print(f"\n[{datetime.now()}] Processing: {topic} | {field_name}")

            # KPI table
            kpi_name = make_kpi_name(topic, field_name)
            kpi_sql = sql_generic_kpis(
                fmap["v1_expr"], fmap["v2_expr"], fmap["lc_expr"],
                v1_from, v2_from, lc_from, exclude_ilike, extra_where
            )
            try:
                execute_ddl(f"CREATE OR REPLACE TABLE {kpi_name} AS\n{kpi_sql}", conn)
                print(f"  ✓ {kpi_name}")
                total += 1
            except Exception as e:
                print(f"  ✗ {kpi_name}: {e}")

            # COMP table
            comp_name = make_comp_name(topic, field_name)
            comp_sql = sql_generic_compare(
                fmap["v1_expr"], fmap["v2_expr"], fmap["lc_expr"],
                v1_from, v2_from, lc_from, exclude_ilike, extra_where
            )
            try:
                execute_ddl(f"CREATE OR REPLACE TABLE {comp_name} AS\n{comp_sql}", conn)
                print(f"  ✓ {comp_name}")
                total += 1
            except Exception as e:
                print(f"  ✗ {comp_name}: {e}")

            # COMP LC table (top 25 by Lightcast, employers only)
            if topic == "employers":
                complc_name = make_comp_lc_name(topic, field_name)
                complc_sql = sql_generic_compare_lc(
                    fmap["v1_expr"], fmap["v2_expr"], fmap["lc_expr"],
                    v1_from, v2_from, lc_from, exclude_ilike, extra_where
                )
                try:
                    execute_ddl(f"CREATE OR REPLACE TABLE {complc_name} AS\n{complc_sql}", conn)
                    print(f"  ✓ {complc_name}")
                    total += 1
                except Exception as e:
                    print(f"  ✗ {complc_name}: {e}")

    # ── 3. ONET changes (v1 vs v2) ──────────────────────────────────────
    for name, sql_fn in [
        (f"{TABLE_PREFIX}_ONET_CHANGES", sql_onet_changes),
        (f"{TABLE_PREFIX}_ONET_CHANGE_SUMMARY", sql_onet_change_summary),
    ]:
        print(f"\n[{datetime.now()}] Creating {name}...")
        try:
            execute_ddl(f"CREATE OR REPLACE TABLE {name} AS\n{sql_fn()}", conn)
            print(f"  ✓ {name}")
            total += 1
        except Exception as e:
            print(f"  ✗ {name}: {e}")

    # ── 4. Salary tables (v1 vs v2 only) ────────────────────────────────
    for name, sql_fn in [
        (f"{TABLE_PREFIX}_SALARY_STATS", sql_salary_stats),
        (f"{TABLE_PREFIX}_SALARY_DISTRIBUTION", sql_salary_distribution),
        (f"{TABLE_PREFIX}_SALARY_BY_SOC2", sql_salary_by_soc2),
        (f"{TABLE_PREFIX}_SALARY_COVERAGE", sql_salary_coverage),
    ]:
        print(f"\n[{datetime.now()}] Creating {name}...")
        try:
            execute_ddl(f"CREATE OR REPLACE TABLE {name} AS\n{sql_fn()}", conn)
            print(f"  ✓ {name}")
            total += 1
        except Exception as e:
            print(f"  ✗ {name}: {e}")

    # ── 5. JOLTS state comparison ──────────────────────────────────────
    print(f"\n[{datetime.now()}] Fetching JOLTS state-level data from BLS API...")
    try:
        fetch_and_upload_jolts_state(conn)
        total += 1
    except Exception as e:
        print(f"  ✗ Failed to upload JOLTS state data: {e}")

    jolts_state_table = f"{TABLE_PREFIX}_JOLTS_STATE_COMPARISON"
    print(f"\n[{datetime.now()}] Creating {jolts_state_table}...")
    try:
        execute_ddl(f"CREATE OR REPLACE TABLE {jolts_state_table} AS\n{sql_jolts_state_comparison()}", conn)
        print(f"  ✓ {jolts_state_table}")
        total += 1
    except Exception as e:
        print(f"  ✗ {jolts_state_table}: {e}")

    conn.close()
    print(f"\n[{datetime.now()}] ═══════════════════════════════════════")
    print(f"[{datetime.now()}] Done! Created {total} tables in {database}.{schema}")
    print(f"[{datetime.now()}] ═══════════════════════════════════════")


if __name__ == "__main__":
    main()
