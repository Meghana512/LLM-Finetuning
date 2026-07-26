# v3: HYBRID SUSGEN-STYLE PLACEHOLDER SFT CONVERTER: KPI + EVIDENCE + METADATA → PLACEHOLDER SECTION-GENERATION SFT DATASET
# =============================================================================
#
# Goal:
#   Fine-tune Llama to generate ESG/BRSR report sections with placeholders,
#   not final numeric values.
#
# Example target:
#   {{company}} disclosed Scope 1 emissions of {{scope1_current}} tCO2e.
#
# Later Python fills:
#   {{company}} → Alicon Castalloy Limited
#   {{scope1_current}} → 14,258.71
#
# This prevents LLM number corruption.
# =============================================================================

import os
import re
import json
import math
import shutil
import hashlib
from pathlib import Path
from collections import Counter, defaultdict

import pandas as pd
from tqdm.auto import tqdm


# =============================================================================
# CONFIG — CHANGE THESE PATHS
# =============================================================================
MAX_MISSING_DATA_ROWS_PER_SECTION = 75

KPI_JSONL = "/kaggle/input/datasets/vaibhavmeena23/final-kpi-and-metadata-merged/merged_esg_kpis_24_25_plus_unique_25_26.jsonl"
METADATA_JSONL = "/kaggle/input/datasets/vaibhavmeena23/final-kpi-and-metadata-merged/merged_metadata_24_25_plus_unique_25_26.jsonl"

# Use your updated map with the 16 company-name fixes appended
COMPANY_NAME_MAP_CSV = "/kaggle/input/datasets/vaibhavmeena23/final-company-name-merged/company_name_map_final.csv"

OUTPUT_DIR = "/kaggle/working/llama_esg_placeholder_sft_24-25"

CLEAR_OUTPUT_DIR = True

if CLEAR_OUTPUT_DIR and os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)

os.makedirs(OUTPUT_DIR, exist_ok=True)

TRAIN_JSONL = f"{OUTPUT_DIR}/llama_esg_placeholder_train_24-25.jsonl"
VALID_JSONL = f"{OUTPUT_DIR}/llama_esg_placeholder_valid_24-25.jsonl"
TEST_JSONL = f"{OUTPUT_DIR}/llama_esg_placeholder_test_24-25.jsonl"

FULL_JSONL = f"{OUTPUT_DIR}/llama_esg_placeholder_all_24-25.jsonl"
AUDIT_CSV = f"{OUTPUT_DIR}/llama_esg_placeholder_audit_24-25.csv"
SAMPLE_JSONL = f"{OUTPUT_DIR}/llama_esg_placeholder_samples_24-25.jsonl"
SECTION_ORDER_JSON = f"{OUTPUT_DIR}/section_generation_order_24-25.json"

RANDOM_SEED = 42
TRAIN_RATIO = 0.85
VALID_RATIO = 0.10
TEST_RATIO = 0.05

DEFAULT_REPORTING_YEAR = "FY 2024-25"

USE_COMPANY_MAP = True
USE_NSE_SYMBOL_AS_FALLBACK_NAME = True

# Include all controlled sections. If no KPI exists for a section,
# target becomes a safe insufficient-data placeholder section.
INCLUDE_MISSING_DATA_SECTIONS = True

# Suppress unsafe extracted YoY percentages.
MAX_SAFE_YOY_PERCENT = 500

MAX_METADATA_JSON_CHARS = 2500
MAX_KPI_JSON_CHARS = 5000
MAX_EVIDENCE_SNIPPETS = 8
MAX_EVIDENCE_CHARS_PER_SNIPPET = 650


# =============================================================================
# SECTION DEFINITIONS
# =============================================================================

SECTION_ORDER = [
    "company_overview",
    "environmental_performance",
    "ghg_emissions",
    "energy_management",
    "water_management",
    "waste_management",
    "social_diversity",
    "governance",
    "targets_transition",
    "disclosure_limitations",
]

SECTION_TITLES = {
    "company_overview": "Company ESG Overview",
    "environmental_performance": "Environmental Performance",
    "ghg_emissions": "GHG Emissions",
    "energy_management": "Energy and Renewable Energy Management",
    "water_management": "Water Management",
    "waste_management": "Waste Management",
    "social_diversity": "Social and Diversity",
    "governance": "Governance",
    "targets_transition": "Targets and Transition Plans",
    "disclosure_limitations": "Disclosure Scope and Limitations",
}

SECTION_INSTRUCTIONS = {
    "company_overview": "Generate the opening ESG overview section for the company.",
    "environmental_performance": "Generate the Environmental Performance section covering emissions, energy, water and waste.",
    "ghg_emissions": "Generate the GHG Emissions section covering Scope 1, Scope 2, Scope 3 and year-on-year movement where available.",
    "energy_management": "Generate the Energy and Renewable Energy Management section.",
    "water_management": "Generate the Water Management section.",
    "waste_management": "Generate the Waste Management section.",
    "social_diversity": "Generate the Social and Diversity section covering workforce, gender diversity, employee welfare and human-rights information where supported.",
    "governance": "Generate the Governance section covering board oversight, ethics, compliance and governance mechanisms where supported.",
    "targets_transition": "Generate the Targets and Transition Plans section using disclosed target-related KPIs and evidence.",
    "disclosure_limitations": "Generate a closing disclosure-scope section explaining that the report is based only on the provided KPI dataset and evidence.",
}

SECTION_KPI_FIELDS = {
    "company_overview": [
        "scope1_emissions_tco2e_current",
        "scope2_emissions_tco2e_current",
        "scope3_emissions_tco2e_current",
        "scope1_scope2_total_tco2e_current",
        "total_energy_consumption_gj",
        "renewable_energy_consumption_gj",
        "renewable_energy_percent",
        "water_consumption_kl_current",
        "water_withdrawal_kl_current",
        "total_waste_generated_current",
        "waste_recycled_current",
        "waste_recycled_percent",
        "female_employee_percent",
        "women_on_board_percent",
        "net_zero_target_year",
    ],
    "environmental_performance": [
        "scope1_emissions_tco2e_current",
        "scope2_emissions_tco2e_current",
        "scope3_emissions_tco2e_current",
        "scope1_scope2_total_tco2e_current",
        "total_energy_consumption_gj",
        "renewable_energy_consumption_gj",
        "renewable_energy_percent",
        "energy_intensity_current",
        "water_consumption_kl_current",
        "water_withdrawal_kl_current",
        "total_waste_generated_current",
        "waste_recycled_current",
        "waste_recycled_percent",
    ],
    "ghg_emissions": [
        "scope1_emissions_tco2e_current",
        "scope1_emissions_tco2e_previous",
        "scope1_emissions_yoy_reduction_percent",
        "scope2_emissions_tco2e_current",
        "scope2_emissions_tco2e_previous",
        "scope2_emissions_yoy_reduction_percent",
        "scope3_emissions_tco2e_current",
        "scope3_emissions_tco2e_previous",
        "scope3_emissions_yoy_reduction_percent",
        "scope1_scope2_total_tco2e_current",
        "scope1_scope2_total_tco2e_previous",
        "scope1_scope2_yoy_reduction_percent",
    ],
    "energy_management": [
        "total_energy_consumption_gj",
        "renewable_energy_consumption_gj",
        "renewable_energy_percent",
        "energy_intensity_current",
        "energy_intensity_previous",
        "energy_intensity_yoy_reduction_percent",
    ],
    "water_management": [
        "water_consumption_kl_current",
        "water_consumption_kl_previous",
        "water_consumption_yoy_reduction_percent",
        "water_withdrawal_kl_current",
        "water_withdrawal_kl_previous",
        "water_withdrawal_yoy_reduction_percent",
    ],
    "waste_management": [
        "total_waste_generated_current",
        "total_waste_generated_previous",
        "waste_recycled_current",
        "waste_recycled_previous",
        "waste_recycled_unit",
        "waste_recycled_percent",
    ],
    "social_diversity": [
        "female_employee_percent",
        "women_on_board_percent",
    ],
    "governance": [
        "women_on_board_percent",
    ],
    "targets_transition": [
        "net_zero_target_year",
        "targets_count",
        "direct_yoy_reductions_count",
        "scope1_emissions_yoy_reduction_percent",
        "scope2_emissions_yoy_reduction_percent",
        "scope3_emissions_yoy_reduction_percent",
        "scope1_scope2_yoy_reduction_percent",
        "energy_intensity_yoy_reduction_percent",
        "water_consumption_yoy_reduction_percent",
        "water_withdrawal_yoy_reduction_percent",
    ],
    "disclosure_limitations": [],
}

SECTION_ALLOWED_EVIDENCE_COLUMNS = {
    "company_overview": [
        "scope1_evidence",
        "scope2_evidence",
        "scope3_evidence",
        "renewable_energy_evidence",
        "total_energy_consumption_evidence",
        "water_consumption_evidence",
        "water_withdrawal_evidence",
        "total_waste_generated_evidence",
        "waste_recycled_evidence",
        "female_employee_evidence",
        "women_on_board_evidence",
        "targets_json",
    ],
    "environmental_performance": [
        "scope1_evidence",
        "scope2_evidence",
        "scope3_evidence",
        "renewable_energy_evidence",
        "total_energy_consumption_evidence",
        "energy_intensity_evidence",
        "water_consumption_evidence",
        "water_withdrawal_evidence",
        "total_waste_generated_evidence",
        "waste_recycled_evidence",
    ],
    "ghg_emissions": [
        "scope1_evidence",
        "scope2_evidence",
        "scope3_evidence",
        "scope1_scope2_total_evidence",
    ],
    "energy_management": [
        "total_energy_consumption_evidence",
        "renewable_energy_evidence",
        "energy_intensity_evidence",
    ],
    "water_management": [
        "water_consumption_evidence",
        "water_withdrawal_evidence",
    ],
    "waste_management": [
        "total_waste_generated_evidence",
        "waste_recycled_evidence",
    ],
    "social_diversity": [
        "female_employee_evidence",
        "women_on_board_evidence",
    ],
    "governance": [
        "women_on_board_evidence",
    ],
    "targets_transition": [
        "targets_json",
        "direct_yoy_reductions_json",
        "net_zero_target_evidence",
    ],
    "disclosure_limitations": [],
}


# =============================================================================
# SYSTEM PROMPT
# =============================================================================

PLACEHOLDER_SECTION_SYSTEM_PROMPT = (
    "You are an ESG reporting analyst. Generate factual ESG/BRSR report sections using "
    "only the provided KPI data, KPI evidence, metadata, requested section, and allowed placeholders. "
    "You must use placeholders such as {{company}} and {{scope1_current}} instead of writing actual "
    "company names, numbers, years, percentages, or units-specific values directly. Do not invent missing "
    "values, policies, awards, TCFD alignment, science-based targets, carbon offsets, committees, "
    "initiatives, or stakeholder engagements. Write in a formal sustainability reporting style suitable "
    "for Indian listed companies."
)


# =============================================================================
# BASIC HELPERS
# =============================================================================

def normalize_text(text):
    if text is None:
        return ""

    text = str(text)

    replacements = {
        "\u00a0": " ",
        "\u200b": " ",
        "\u200c": " ",
        "\u200d": " ",
        "CO₂": "CO2",
        "co₂": "co2",
        "CO²": "CO2",
        "co²": "CO2",
        "–": "-",
        "—": "-",
        "−": "-",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    text = re.sub(r"[\ud800-\udfff]", " ", text)
    text = text.encode("utf-8", "ignore").decode("utf-8", "ignore")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()


def is_missing(v):
    if v is None:
        return True

    if isinstance(v, float) and math.isnan(v):
        return True

    s = str(v).strip().lower()

    return s in {
        "",
        "none",
        "nan",
        "null",
        "na",
        "n.a.",
        "n.a",
        "not available",
        "not disclosed",
        "unknown",
        "not mentioned",
        "nil",
        "-",
    }


def clean_value(v):
    if is_missing(v):
        return None
    return v


def clean_reporting_year_value(v):
    if is_missing(v):
        return None

    s = str(v).strip()
    s = s.replace("_", "-")
    s = s.replace("–", "-")
    s = s.replace("—", "-")

    m = re.search(r"(20\d{2})\s*[-/]\s*(\d{2})", s)
    if m:
        y1 = int(m.group(1))
        y2 = int("20" + m.group(2))

        if y2 == y1 + 1:
            return f"FY {y1}-{str(y2)[-2:]}"

        return None

    m = re.search(r"\b(20\d{2})\b", s)
    if m:
        return f"FY {m.group(1)}"

    return None


def safe_json(obj, indent=None):
    return json.dumps(obj, ensure_ascii=False, indent=indent)


def truncate_text(text, max_chars):
    text = normalize_text(text)

    if len(text) <= max_chars:
        return text

    cut = text[:max_chars]
    period = cut.rfind(".")

    if period > max_chars * 0.65:
        return cut[:period + 1]

    return cut.rstrip() + "..."


def truncate_json_string(text, max_chars):
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "\n...TRUNCATED..."


def normalize_key_part(x):
    if x is None:
        return ""

    x = str(x).strip().lower()
    x = re.sub(r"\.pdf$", "", x)
    x = re.sub(r"[^a-z0-9]+", " ", x)
    x = re.sub(r"\s+", " ", x).strip()

    return x


def normalize_symbol(x):
    if x is None:
        return ""

    x = str(x).upper().strip()
    x = x.replace(".NS", "")
    x = x.replace(".BO", "")
    x = re.sub(r"[^A-Z0-9]+", "", x)

    return x


def stable_split(key):
    h = int(hashlib.sha1((str(key) + str(RANDOM_SEED)).encode("utf-8")).hexdigest(), 16)
    bucket = (h % 10000) / 10000

    if bucket < TRAIN_RATIO:
        return "train"

    if bucket < TRAIN_RATIO + VALID_RATIO:
        return "valid"

    return "test"


def num_value(v):
    if is_missing(v):
        return None

    try:
        if isinstance(v, str):
            v = v.replace(",", "").replace("%", "").strip()
        return float(v)
    except Exception:
        return None


def fmt_num(v, decimals=None):
    if is_missing(v):
        return None

    try:
        x = float(str(v).replace(",", "").replace("%", "").strip())
    except Exception:
        return str(v)

    if decimals is not None:
        return f"{x:,.{decimals}f}".rstrip("0").rstrip(".")

    if abs(x) != 0 and abs(x) < 0.01:
        return f"{x:,.9f}".rstrip("0").rstrip(".")

    if abs(x - round(x)) < 1e-9:
        return f"{int(round(x)):,}"

    return f"{x:,.2f}".rstrip("0").rstrip(".")


def fmt_percent(v, decimals=2):
    x = num_value(v)
    if x is None:
        return None
    return f"{x:,.{decimals}f}".rstrip("0").rstrip(".")


def fmt_yoy_text(v):
    x = num_value(v)

    if x is None:
        return None

    if abs(x) > MAX_SAFE_YOY_PERCENT:
        return (
            "had a year-on-year movement in the structured KPI data, "
            "but the extracted percentage appears unusually high and should be reviewed against the source disclosure"
        )

    pct = fmt_percent(abs(x))

    if x > 0:
        return f"reduced by {pct}% compared with the previous year"

    if x < 0:
        return f"increased by {pct}% compared with the previous year"

    return "remained unchanged compared with the previous year"


def is_positive_number(v):
    x = num_value(v)
    return x is not None and x > 0


def has_valid_net_zero_target(v):
    if is_missing(v):
        return False

    s = str(v).strip().lower()

    if s in {"0", "0.0", "none", "na", "n.a.", "not available", "not disclosed"}:
        return False

    return bool(re.search(r"\b20[2-9]\d\b", s))


def should_add_targets_transition(all_kpis):
    if is_positive_number(all_kpis.get("targets_count")):
        return True

    if is_positive_number(all_kpis.get("direct_yoy_reductions_count")):
        return True

    if has_valid_net_zero_target(all_kpis.get("net_zero_target_year")):
        return True

    reduction_fields = [
        "scope1_emissions_yoy_reduction_percent",
        "scope2_emissions_yoy_reduction_percent",
        "scope3_emissions_yoy_reduction_percent",
        "scope1_scope2_yoy_reduction_percent",
        "energy_intensity_yoy_reduction_percent",
        "water_consumption_yoy_reduction_percent",
        "water_withdrawal_yoy_reduction_percent",
    ]

    for field in reduction_fields:
        x = num_value(all_kpis.get(field))
        if x is not None and x > 0:
            return True

    return False


# =============================================================================
# COMPANY NAME HELPERS
# =============================================================================

def looks_like_dirty_company_name(name):
    if not name:
        return True

    s = str(name).strip()
    low = s.lower()

    dirty_markers = [
        "brsr",
        "brsrpdf",
        "pdf",
        "signed",
        "intimation",
        "seintimation",
        "final",
        "finalsd",
        "annexure",
        "annualreport",
        "businessresponsibility",
        "sustainabilityreport",
        "covering",
        "letter",
        "notice",
        "agm",
    ]

    if any(m in low for m in dirty_markers):
        return True

    if "_" in s:
        return True

    if len(re.findall(r"\d", s)) >= 6:
        return True

    if len(re.sub(r"[^A-Za-z]", "", s)) < 3:
        return True

    return False


def title_clean_company_name(name):
    name = normalize_text(name)

    if not name:
        return ""

    name = re.sub(r"\bLTD\.?\b", "Limited", name, flags=re.I)
    name = re.sub(r"\bLIMITED\b", "Limited", name, flags=re.I)
    name = re.sub(r"\s+", " ", name).strip()

    return name


def load_company_name_map(path):
    symbol_map = {}
    source_key_map = {}

    if not USE_COMPANY_MAP:
        print("Company map disabled.")
        return symbol_map, source_key_map

    if not path or not os.path.exists(path):
        print("Company name map not found:", path)
        return symbol_map, source_key_map

    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]

    if "company_name" not in df.columns:
        raise ValueError("company_name_map.csv must contain a company_name column.")

    for _, row in df.iterrows():
        company_name = title_clean_company_name(row.get("company_name"))

        if not company_name:
            continue

        if "nse_symbol" in df.columns:
            sym = normalize_symbol(row.get("nse_symbol"))
            if sym:
                symbol_map[sym] = company_name

        if "source_key" in df.columns:
            skey = normalize_key_part(row.get("source_key"))
            if skey:
                source_key_map[skey] = company_name

    print("Company symbol mappings loaded:", len(symbol_map))
    print("Company source_key mappings loaded:", len(source_key_map))

    return symbol_map, source_key_map


def infer_symbol_from_source(source_key, raw_company, symbol_map):
    symbol_map = symbol_map or {}

    text = f"{raw_company or ''} {source_key or ''}".upper()
    compact = normalize_symbol(text)

    for sym in sorted(symbol_map.keys(), key=len, reverse=True):
        if len(sym) >= 3 and (compact.startswith(sym) or sym in compact):
            return sym

    candidates = []

    for value in [raw_company, source_key]:
        if not value:
            continue

        s = str(value).upper()
        s = re.sub(r"\.PDF$", "", s)

        first = re.split(r"[_\s\-]+", s)[0]
        first = normalize_symbol(first)

        if 2 <= len(first) <= 25:
            candidates.append(first)

    bad_tokens = {
        "BRSR", "PDF", "SIGNED", "FINAL", "INTIMATION", "SE", "NSE", "BSE",
        "ANNUAL", "REPORT", "COVERING", "LETTER", "NOTICE", "AGM"
    }

    for cand in candidates:
        if cand not in bad_tokens and not cand.isdigit():
            return cand

    return ""


def resolve_company_name(
    raw_company,
    nse_symbol,
    source_key,
    symbol_map,
    source_key_map=None,
    metadata_company=None,
    metadata_symbol=None,
):
    raw_company = normalize_text(raw_company)
    symbol_map = symbol_map or {}
    source_key_map = source_key_map or {}

    skey = normalize_key_part(source_key)
    sym = normalize_symbol(nse_symbol)
    meta_sym = normalize_symbol(metadata_symbol)

    if skey and skey in source_key_map:
        return source_key_map[skey]

    if sym and sym in symbol_map:
        return symbol_map[sym]

    inferred_sym = infer_symbol_from_source(
        source_key=source_key,
        raw_company=raw_company,
        symbol_map=symbol_map,
    )

    if inferred_sym and inferred_sym in symbol_map:
        return symbol_map[inferred_sym]

    if metadata_company and not looks_like_dirty_company_name(metadata_company):
        if not inferred_sym or not meta_sym or inferred_sym == meta_sym:
            return title_clean_company_name(metadata_company)

    if raw_company and not looks_like_dirty_company_name(raw_company):
        return title_clean_company_name(raw_company)

    if USE_NSE_SYMBOL_AS_FALLBACK_NAME:
        if inferred_sym:
            return inferred_sym
        if sym:
            return sym
        if meta_sym:
            return meta_sym

    return "The company"


# =============================================================================
# LOADERS + INDEXING
# =============================================================================

def load_jsonl(path):
    rows = []

    if not path or not os.path.exists(path):
        print("File not found:", path)
        return rows

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()

            if not line:
                continue

            try:
                rows.append(json.loads(line))
            except Exception as e:
                print(f"Skipping bad JSON line {line_no} in {path}: {e}")

    return rows


def get_identity(row):
    flat = row.get("flat_kpis") if isinstance(row.get("flat_kpis"), dict) else {}
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}

    company = (
        row.get("company")
        or flat.get("company")
        or metadata.get("company")
        or row.get("company_name")
        or metadata.get("company_name")
    )

    filename = row.get("filename") or flat.get("filename") or metadata.get("filename")
    file = row.get("file") or flat.get("file") or metadata.get("file")

    reporting_year = (
        row.get("reporting_year")
        or flat.get("reporting_year")
        or metadata.get("reporting_year")
        or row.get("year")
        or metadata.get("year")
        or row.get("financial_year")
        or metadata.get("financial_year")
    )

    return {
        "company": company,
        "filename": filename,
        "file": file,
        "reporting_year": reporting_year,
    }


def make_key(row):
    ids = get_identity(row)

    if ids["filename"]:
        return normalize_key_part(Path(str(ids["filename"])).stem)

    if ids["file"]:
        return normalize_key_part(Path(str(ids["file"])).stem)

    return normalize_key_part(ids["company"])


def index_jsonl_by_key(path):
    rows = load_jsonl(path)
    index = {}
    duplicates = 0

    for row in rows:
        key = make_key(row)

        if not key:
            continue

        if key in index:
            duplicates += 1

            flat_new = row.get("flat_kpis") if isinstance(row.get("flat_kpis"), dict) else {}
            flat_old = index[key].get("flat_kpis") if isinstance(index[key].get("flat_kpis"), dict) else {}

            non_null_new = sum(1 for v in flat_new.values() if not is_missing(v))
            non_null_old = sum(1 for v in flat_old.values() if not is_missing(v))

            if non_null_new > non_null_old:
                index[key] = row
        else:
            index[key] = row

    print(f"{Path(path).name}: rows={len(rows)}, indexed={len(index)}, duplicates={duplicates}")

    return index


def get_flat_kpis(kpi_row):
    flat = kpi_row.get("flat_kpis") if isinstance(kpi_row.get("flat_kpis"), dict) else {}

    out = {}

    for k, v in flat.items():
        if not isinstance(k, str):
            continue

        if is_missing(v):
            continue

        if k in {
            "file",
            "filename",
            "company",
            "reporting_year",
            "review_flags",
            "manual_corrected",
            "manual_correction_note",
            "renewable_energy_review_flag",
        }:
            continue

        if k.endswith("_evidence"):
            continue

        if k in {"targets_json", "direct_yoy_reductions_json"}:
            continue

        out[k] = v

    return out


def get_section_kpis(all_kpis, section_id):
    fields = SECTION_KPI_FIELDS.get(section_id, [])

    return {
        f: all_kpis[f]
        for f in fields
        if f in all_kpis and not is_missing(all_kpis[f])
    }


def extract_metadata(metadata_row, kpi_row, symbol_map=None, source_key_map=None, source_key=None):
    symbol_map = symbol_map or {}
    source_key_map = source_key_map or {}

    flat = kpi_row.get("flat_kpis") if isinstance(kpi_row.get("flat_kpis"), dict) else {}

    raw_company = (
        flat.get("filename")
        or kpi_row.get("filename")
        or flat.get("file")
        or kpi_row.get("file")
        or flat.get("company")
        or kpi_row.get("company")
        or source_key
    )

    source_sym = infer_symbol_from_source(
        source_key=source_key,
        raw_company=raw_company,
        symbol_map=symbol_map,
    )

    kpi_nse_symbol = (
        flat.get("nse_symbol")
        or kpi_row.get("nse_symbol")
        or flat.get("_meta_nse_symbol")
        or kpi_row.get("_meta_nse_symbol")
        or source_sym
    )

    metadata_company = None
    metadata_sym = ""

    if isinstance(metadata_row, dict):
        metadata_company = (
            metadata_row.get("company")
            or metadata_row.get("company_name")
            or metadata_row.get("name")
        )

        metadata_sym = normalize_symbol(
            metadata_row.get("nse_symbol")
            or metadata_row.get("_meta_nse_symbol")
            or metadata_row.get("symbol")
        )

    merged = {}

    if isinstance(metadata_row, dict):
        for field in [
            "sector",
            "industry",
            "market_cap",
            "reporting_year",
            "framework_used",
            "brsr_version",
            "assurance_type",
            "geography",
        ]:
            v = clean_value(metadata_row.get(field))
            if v is not None:
                merged[field] = v

    for source in [kpi_row or {}, flat or {}]:
        if not isinstance(source, dict):
            continue

        for field in [
            "sector",
            "industry",
            "market_cap",
            "reporting_year",
            "framework_used",
            "brsr_version",
            "assurance_type",
            "geography",
        ]:
            v = clean_value(source.get(field))
            if v is not None and field not in merged:
                merged[field] = v

    clean_name = resolve_company_name(
        raw_company=raw_company,
        nse_symbol=kpi_nse_symbol,
        source_key=source_key,
        symbol_map=symbol_map,
        source_key_map=source_key_map,
        metadata_company=metadata_company,
        metadata_symbol=metadata_sym,
    )

    merged["company_raw"] = raw_company
    merged["company"] = clean_name

    final_symbol = source_sym or normalize_symbol(kpi_nse_symbol)
    if final_symbol:
        merged["nse_symbol"] = final_symbol

    raw_reporting_year = (
        merged.get("reporting_year")
        or flat.get("reporting_year")
        or kpi_row.get("reporting_year")
    )

    clean_year = clean_reporting_year_value(raw_reporting_year)

    if clean_year:
        merged["reporting_year"] = clean_year
    else:
        merged["reporting_year"] = DEFAULT_REPORTING_YEAR

    return merged


def collect_kpi_evidence(kpi_row, section_id):
    flat = kpi_row.get("flat_kpis") if isinstance(kpi_row.get("flat_kpis"), dict) else {}

    allowed_cols = set(SECTION_ALLOWED_EVIDENCE_COLUMNS.get(section_id, []))
    evidence_items = []

    for col, val in flat.items():
        if not isinstance(col, str):
            continue

        if col not in allowed_cols:
            continue

        if is_missing(val):
            continue

        text = normalize_text(val)

        if not text:
            continue

        evidence_items.append({
            "source": col,
            "text": truncate_text(text, MAX_EVIDENCE_CHARS_PER_SNIPPET),
        })

    seen = set()
    out = []

    for item in evidence_items:
        key = item["text"].lower()[:300]

        if key in seen:
            continue

        seen.add(key)
        out.append(item)

        if len(out) >= MAX_EVIDENCE_SNIPPETS:
            break

    return out


# =============================================================================
# PLACEHOLDER VALUE HELPERS
# =============================================================================

def base_placeholder_values(metadata):
    company = metadata.get("company") or "The company"
    year = clean_reporting_year_value(metadata.get("reporting_year")) or DEFAULT_REPORTING_YEAR

    out = {
        "{{company}}": company,
        "{{reporting_year}}": year,
    }

    if metadata.get("nse_symbol"):
        out["{{nse_symbol}}"] = metadata.get("nse_symbol")

    if metadata.get("sector"):
        out["{{sector}}"] = metadata.get("sector")

    if metadata.get("industry"):
        out["{{industry}}"] = metadata.get("industry")

    if metadata.get("framework_used"):
        out["{{framework_used}}"] = metadata.get("framework_used")

    if metadata.get("brsr_version"):
        out["{{brsr_version}}"] = metadata.get("brsr_version")

    if metadata.get("assurance_type"):
        out["{{assurance_type}}"] = metadata.get("assurance_type")

    return out


def add_value(ph, placeholder, value, formatter=fmt_num):
    if is_missing(value):
        return False

    formatted = formatter(value) if formatter else str(value)

    if formatted is None or str(formatted).strip() == "":
        return False

    ph[placeholder] = formatted
    return True


def add_yoy_value(ph, placeholder, value):
    text = fmt_yoy_text(value)

    if not text:
        return False

    ph[placeholder] = text
    return True


def placeholder_fact_sheet(placeholder_values):
    lines = []

    for k in sorted(placeholder_values.keys()):
        lines.append(f"{k}: {placeholder_values[k]}")

    return "\n".join(lines)


def fill_placeholders(text, placeholder_values):
    out = text

    for ph, value in sorted(placeholder_values.items(), key=lambda x: len(x[0]), reverse=True):
        out = out.replace(ph, str(value))

    return out


def extract_placeholders(text):
    return sorted(set(re.findall(r"\{\{[A-Za-z0-9_]+\}\}", text or "")))


def validate_template_placeholders(template_text, placeholder_values):
    target_placeholders = extract_placeholders(template_text)
    available = set(placeholder_values.keys())

    missing_values = [p for p in target_placeholders if p not in available]

    return {
        "target_placeholders": target_placeholders,
        "missing_placeholder_values": missing_values,
        "has_missing_placeholder_values": len(missing_values) > 0,
    }


# =============================================================================
# PLACEHOLDER TARGET BUILDERS
# =============================================================================

def build_missing_data_placeholder_target(metadata, section_id):
    title = SECTION_TITLES[section_id]
    ph = base_placeholder_values(metadata)

    target = (
        "{{company}} does not have sufficient structured KPI data in the provided input to generate a detailed "
        + title
        + " section for {{reporting_year}}. No unsupported claims have been added for this section. "
        "Additional verified KPI data or source evidence would be required to describe this topic in greater detail."
    )

    return target, ph, "placeholder_missing_data"

def build_disclosure_limitations_placeholder_target(metadata):
    ph = base_placeholder_values(metadata)

    target = (
        "This ESG report section for {{company}} has been prepared using only the provided KPI dataset, "
        "KPI evidence and metadata. Where information on policies, governance mechanisms, stakeholder engagement, "
        "climate-risk processes, TCFD alignment, science-based targets, carbon offsets, awards, certifications, "
        "or specific initiatives is not present in the input data, no unsupported claim has been made. The narrative "
        "should therefore be read as a KPI- and evidence-supported ESG disclosure, not as a complete reproduction of "
        "the company's statutory BRSR filing."
    )

    return target, ph, "placeholder_disclosure_limitations"


def build_company_overview_placeholder_target(metadata, section_kpis):
    ph = base_placeholder_values(metadata)
    signals = []
    sentences = [
        "{{company}} has prepared this ESG/BRSR section for {{reporting_year}} using the available KPI data and supporting evidence."
    ]

    if any(k in section_kpis for k in [
        "scope1_emissions_tco2e_current",
        "scope2_emissions_tco2e_current",
        "scope3_emissions_tco2e_current",
    ]):
        signals.append("greenhouse gas emissions")

    if any(k in section_kpis for k in [
        "renewable_energy_percent",
        "renewable_energy_consumption_gj",
        "total_energy_consumption_gj",
    ]):
        signals.append("energy and renewable energy use")

    if "water_consumption_kl_current" in section_kpis or "water_withdrawal_kl_current" in section_kpis:
        signals.append("water management")

    if "total_waste_generated_current" in section_kpis or "waste_recycled_current" in section_kpis:
        signals.append("waste management")

    if "female_employee_percent" in section_kpis or "women_on_board_percent" in section_kpis:
        signals.append("workforce and gender diversity")

    if "net_zero_target_year" in section_kpis:
        signals.append("long-term transition target")

    if not signals:
        return None, ph, None

    ph["{{overview_topics}}"] = ", ".join(signals)
    sentences.append("The disclosed ESG information covers {{overview_topics}}.")

    if add_value(ph, "{{scope1_current}}", section_kpis.get("scope1_emissions_tco2e_current")):
        sentences.append("Scope 1 emissions were reported at {{scope1_current}} tCO2e.")

    if add_value(ph, "{{scope2_current}}", section_kpis.get("scope2_emissions_tco2e_current")):
        sentences.append("Scope 2 emissions were reported at {{scope2_current}} tCO2e.")

    if add_value(ph, "{{scope3_current}}", section_kpis.get("scope3_emissions_tco2e_current")):
        sentences.append("Scope 3 emissions were reported at {{scope3_current}} tCO2e.")

    if add_value(ph, "{{total_energy}}", section_kpis.get("total_energy_consumption_gj")):
        sentences.append("Total energy consumption was reported at {{total_energy}} GJ.")

    if add_value(ph, "{{renewable_energy_pct}}", section_kpis.get("renewable_energy_percent")):
        sentences.append("Renewable energy represented {{renewable_energy_pct}}% of the disclosed energy mix.")

    if add_value(ph, "{{water_consumption_current}}", section_kpis.get("water_consumption_kl_current")):
        sentences.append("Water consumption was reported at {{water_consumption_current}} KL.")

    if add_value(ph, "{{water_withdrawal_current}}", section_kpis.get("water_withdrawal_kl_current")):
        sentences.append("Water withdrawal was reported at {{water_withdrawal_current}} KL.")

    if add_value(ph, "{{total_waste_current}}", section_kpis.get("total_waste_generated_current")):
        sentences.append("Total waste generated was reported at {{total_waste_current}} tonnes.")

    if add_value(ph, "{{female_employee_pct}}", section_kpis.get("female_employee_percent")):
        sentences.append("Female employees represented {{female_employee_pct}}% of the disclosed employee base.")

    if add_value(ph, "{{women_on_board_pct}}", section_kpis.get("women_on_board_percent")):
        sentences.append("Women on the Board represented {{women_on_board_pct}}% of the disclosed Board composition.")

    if has_valid_net_zero_target(section_kpis.get("net_zero_target_year")):
        add_value(ph, "{{net_zero_target_year}}", section_kpis.get("net_zero_target_year"), formatter=lambda x: str(int(num_value(x))) if num_value(x) is not None else str(x))
        sentences.append("The company has disclosed a net-zero or carbon-neutrality target year of {{net_zero_target_year}}.")

    sentences.append(
        "This overview is limited to the structured KPIs and evidence available in the input data and does not add unsupported policies, initiatives, awards, committees, or external-framework claims."
    )

    return " ".join(sentences), ph, "placeholder_kpi_grounded_target"


def build_environmental_placeholder_target(metadata, section_kpis):
    ph = base_placeholder_values(metadata)

    has_any = any(k in section_kpis for k in [
        "scope1_emissions_tco2e_current",
        "scope2_emissions_tco2e_current",
        "scope3_emissions_tco2e_current",
        "scope1_scope2_total_tco2e_current",
        "total_energy_consumption_gj",
        "renewable_energy_percent",
        "water_consumption_kl_current",
        "water_withdrawal_kl_current",
        "total_waste_generated_current",
        "waste_recycled_current",
        "waste_recycled_percent",
    ])

    if not has_any:
        return None, ph, None

    sentences = [
        "{{company}} disclosed selected environmental performance indicators for {{reporting_year}}, covering emissions, energy, water and waste where data was available."
    ]

    if add_value(ph, "{{scope1_scope2_current}}", section_kpis.get("scope1_scope2_total_tco2e_current")):
        sentences.append("Combined Scope 1 and Scope 2 emissions stood at {{scope1_scope2_current}} tCO2e.")
    else:
        if add_value(ph, "{{scope1_current}}", section_kpis.get("scope1_emissions_tco2e_current")):
            sentences.append("Scope 1 emissions were {{scope1_current}} tCO2e.")
        if add_value(ph, "{{scope2_current}}", section_kpis.get("scope2_emissions_tco2e_current")):
            sentences.append("Scope 2 emissions were {{scope2_current}} tCO2e.")

    if add_value(ph, "{{scope3_current}}", section_kpis.get("scope3_emissions_tco2e_current")):
        sentences.append("Scope 3 emissions were {{scope3_current}} tCO2e.")

    if add_value(ph, "{{total_energy}}", section_kpis.get("total_energy_consumption_gj")):
        sentences.append("Total energy consumption was {{total_energy}} GJ.")

    if add_value(ph, "{{renewable_energy_pct}}", section_kpis.get("renewable_energy_percent")):
        sentences.append("Renewable energy accounted for {{renewable_energy_pct}}% of disclosed energy consumption.")

    if add_value(ph, "{{water_consumption_current}}", section_kpis.get("water_consumption_kl_current")):
        sentences.append("Water consumption was {{water_consumption_current}} KL.")

    if add_value(ph, "{{water_withdrawal_current}}", section_kpis.get("water_withdrawal_kl_current")):
        sentences.append("Water withdrawal was {{water_withdrawal_current}} KL.")

    if add_value(ph, "{{total_waste_current}}", section_kpis.get("total_waste_generated_current")):
        sentences.append("Total waste generated was {{total_waste_current}} tonnes.")

    if add_value(ph, "{{waste_recycled_current}}", section_kpis.get("waste_recycled_current")):
        sentences.append("Waste recycled or recovered was {{waste_recycled_current}} tonnes.")

    if add_value(ph, "{{waste_recycled_pct}}", section_kpis.get("waste_recycled_percent")):
        sentences.append(
            "The structured KPI data reports a waste recycling or recovery percentage of {{waste_recycled_pct}}%; this percentage should be read together with the underlying waste category disclosures."
        )

    sentences.append(
        "This environmental performance section is limited to the disclosed KPI data and evidence, and does not add unsupported environmental initiatives, certifications, or commitments."
    )

    return " ".join(sentences), ph, "placeholder_kpi_grounded_target"


def build_ghg_placeholder_target(metadata, section_kpis):
    ph = base_placeholder_values(metadata)
    sentences = [
        "{{company}} disclosed its greenhouse gas emissions for {{reporting_year}} based on the available Scope-wise emissions data."
    ]

    has_any = False

    if add_value(ph, "{{scope1_current}}", section_kpis.get("scope1_emissions_tco2e_current")):
        has_any = True
        if add_value(ph, "{{scope1_previous}}", section_kpis.get("scope1_emissions_tco2e_previous")):
            sentences.append("Scope 1 emissions were {{scope1_current}} tCO2e, compared with {{scope1_previous}} tCO2e in the previous year.")
        else:
            sentences.append("Scope 1 emissions were {{scope1_current}} tCO2e.")

        if add_yoy_value(ph, "{{scope1_yoy_text}}", section_kpis.get("scope1_emissions_yoy_reduction_percent")):
            sentences.append("Scope 1 emissions {{scope1_yoy_text}}.")

    if add_value(ph, "{{scope2_current}}", section_kpis.get("scope2_emissions_tco2e_current")):
        has_any = True
        if add_value(ph, "{{scope2_previous}}", section_kpis.get("scope2_emissions_tco2e_previous")):
            sentences.append("Scope 2 emissions were {{scope2_current}} tCO2e, compared with {{scope2_previous}} tCO2e in the previous year.")
        else:
            sentences.append("Scope 2 emissions were {{scope2_current}} tCO2e.")

        if add_yoy_value(ph, "{{scope2_yoy_text}}", section_kpis.get("scope2_emissions_yoy_reduction_percent")):
            sentences.append("Scope 2 emissions {{scope2_yoy_text}}.")

    if add_value(ph, "{{scope3_current}}", section_kpis.get("scope3_emissions_tco2e_current")):
        has_any = True
        if add_value(ph, "{{scope3_previous}}", section_kpis.get("scope3_emissions_tco2e_previous")):
            sentences.append("Scope 3 emissions were {{scope3_current}} tCO2e, compared with {{scope3_previous}} tCO2e in the previous year.")
        else:
            sentences.append("Scope 3 emissions were {{scope3_current}} tCO2e.")

        if add_yoy_value(ph, "{{scope3_yoy_text}}", section_kpis.get("scope3_emissions_yoy_reduction_percent")):
            sentences.append("Scope 3 emissions {{scope3_yoy_text}}.")

    if add_value(ph, "{{scope1_scope2_current}}", section_kpis.get("scope1_scope2_total_tco2e_current")):
        has_any = True
        if add_value(ph, "{{scope1_scope2_previous}}", section_kpis.get("scope1_scope2_total_tco2e_previous")):
            sentences.append(
                "Combined Scope 1 and Scope 2 emissions stood at {{scope1_scope2_current}} tCO2e, compared with {{scope1_scope2_previous}} tCO2e in the previous year."
            )
        else:
            sentences.append("Combined Scope 1 and Scope 2 emissions stood at {{scope1_scope2_current}} tCO2e.")

        if add_yoy_value(ph, "{{scope1_scope2_yoy_text}}", section_kpis.get("scope1_scope2_yoy_reduction_percent")):
            sentences.append("Combined Scope 1 and Scope 2 emissions {{scope1_scope2_yoy_text}}.")

    if not has_any:
        return None, ph, None

    sentences.append(
        "The disclosure is limited to the emissions KPIs and evidence available in the input data, and no unsupported claims have been made on climate targets, offsets, or external frameworks."
    )

    return " ".join(sentences), ph, "placeholder_kpi_grounded_target"


def build_energy_placeholder_target(metadata, section_kpis):
    ph = base_placeholder_values(metadata)
    sentences = [
        "{{company}} disclosed its energy management performance for {{reporting_year}} based on the available energy KPIs."
    ]

    has_any = False

    if add_value(ph, "{{total_energy}}", section_kpis.get("total_energy_consumption_gj")):
        has_any = True
        sentences.append("Total energy consumption was {{total_energy}} GJ.")

    if add_value(ph, "{{renewable_energy}}", section_kpis.get("renewable_energy_consumption_gj")):
        has_any = True
        sentences.append("Renewable energy consumption was {{renewable_energy}} GJ.")

    if add_value(ph, "{{renewable_energy_pct}}", section_kpis.get("renewable_energy_percent")):
        has_any = True
        sentences.append("Renewable energy accounted for {{renewable_energy_pct}}% of disclosed energy consumption.")

    if add_value(ph, "{{energy_intensity_current}}", section_kpis.get("energy_intensity_current")):
        has_any = True
        if add_value(ph, "{{energy_intensity_previous}}", section_kpis.get("energy_intensity_previous")):
            sentences.append("Energy intensity was {{energy_intensity_current}}, compared with {{energy_intensity_previous}} in the previous year.")
        else:
            sentences.append("Energy intensity was {{energy_intensity_current}}.")

        if add_yoy_value(ph, "{{energy_intensity_yoy_text}}", section_kpis.get("energy_intensity_yoy_reduction_percent")):
            sentences.append("Energy intensity {{energy_intensity_yoy_text}}.")

    if not has_any:
        return None, ph, None

    sentences.append(
        "The section is limited to disclosed energy KPIs and evidence, and does not make unsupported claims about energy projects, certifications, or transition initiatives."
    )

    return " ".join(sentences), ph, "placeholder_kpi_grounded_target"


def build_water_placeholder_target(metadata, section_kpis):
    ph = base_placeholder_values(metadata)
    sentences = [
        "{{company}} disclosed its water management performance for {{reporting_year}} based on the available water KPIs."
    ]

    has_any = False

    if add_value(ph, "{{water_consumption_current}}", section_kpis.get("water_consumption_kl_current")):
        has_any = True
        if add_value(ph, "{{water_consumption_previous}}", section_kpis.get("water_consumption_kl_previous")):
            sentences.append("Water consumption was {{water_consumption_current}} KL, compared with {{water_consumption_previous}} KL in the previous year.")
        else:
            sentences.append("Water consumption was {{water_consumption_current}} KL.")

        if add_yoy_value(ph, "{{water_consumption_yoy_text}}", section_kpis.get("water_consumption_yoy_reduction_percent")):
            sentences.append("Water consumption {{water_consumption_yoy_text}}.")

    if add_value(ph, "{{water_withdrawal_current}}", section_kpis.get("water_withdrawal_kl_current")):
        has_any = True
        if add_value(ph, "{{water_withdrawal_previous}}", section_kpis.get("water_withdrawal_kl_previous")):
            sentences.append("Water withdrawal was {{water_withdrawal_current}} KL, compared with {{water_withdrawal_previous}} KL in the previous year.")
        else:
            sentences.append("Water withdrawal was {{water_withdrawal_current}} KL.")

        if add_yoy_value(ph, "{{water_withdrawal_yoy_text}}", section_kpis.get("water_withdrawal_yoy_reduction_percent")):
            sentences.append("Water withdrawal {{water_withdrawal_yoy_text}}.")

    if not has_any:
        return None, ph, None

    sentences.append(
        "The section is limited to disclosed water KPIs and evidence, and does not add unsupported claims on water stewardship programmes, recycling systems, or zero-liquid-discharge practices."
    )

    return " ".join(sentences), ph, "placeholder_kpi_grounded_target"


def build_waste_placeholder_target(metadata, section_kpis):
    ph = base_placeholder_values(metadata)
    unit = section_kpis.get("waste_recycled_unit") or "tonnes"
    ph["{{waste_recycled_unit}}"] = str(unit)

    sentences = [
        "{{company}} disclosed its waste management performance for {{reporting_year}} based on the available waste KPIs."
    ]

    has_any = False

    if add_value(ph, "{{total_waste_current}}", section_kpis.get("total_waste_generated_current")):
        has_any = True
        if add_value(ph, "{{total_waste_previous}}", section_kpis.get("total_waste_generated_previous")):
            sentences.append("Total waste generated was {{total_waste_current}} tonnes, compared with {{total_waste_previous}} tonnes in the previous year.")
        else:
            sentences.append("Total waste generated was {{total_waste_current}} tonnes.")

    if add_value(ph, "{{waste_recycled_current}}", section_kpis.get("waste_recycled_current")):
        has_any = True
        if add_value(ph, "{{waste_recycled_previous}}", section_kpis.get("waste_recycled_previous")):
            sentences.append(
                "Waste recycled or recovered was {{waste_recycled_current}} {{waste_recycled_unit}}, compared with {{waste_recycled_previous}} {{waste_recycled_unit}} in the previous year."
            )
        else:
            sentences.append("Waste recycled or recovered was {{waste_recycled_current}} {{waste_recycled_unit}}.")

    if add_value(ph, "{{waste_recycled_pct}}", section_kpis.get("waste_recycled_percent")):
        has_any = True
        sentences.append(
            "The structured KPI data reports a waste recycling or recovery percentage of {{waste_recycled_pct}}%; this percentage should be read together with the underlying waste category disclosures."
        )

    if not has_any:
        return None, ph, None

    sentences.append(
        "The section is limited to disclosed waste KPIs and evidence, and does not add unsupported claims about waste-reduction initiatives, circularity programmes, or disposal practices."
    )

    return " ".join(sentences), ph, "placeholder_kpi_grounded_target"


def build_social_placeholder_target(metadata, section_kpis):
    ph = base_placeholder_values(metadata)
    sentences = [
        "{{company}} disclosed selected social and diversity indicators for {{reporting_year}} based on the available workforce and governance diversity KPIs."
    ]

    has_any = False

    if add_value(ph, "{{female_employee_pct}}", section_kpis.get("female_employee_percent")):
        has_any = True
        sentences.append("Female employees represented {{female_employee_pct}}% of the disclosed employee base.")

    if add_value(ph, "{{women_on_board_pct}}", section_kpis.get("women_on_board_percent")):
        has_any = True
        sentences.append("Women on the Board represented {{women_on_board_pct}}% of the disclosed Board composition.")

    if not has_any:
        return None, ph, None

    sentences.append(
        "The section is limited to disclosed social and diversity KPIs and evidence, and does not make unsupported claims on employee welfare programmes, human-rights processes, training, safety, or community initiatives."
    )

    return " ".join(sentences), ph, "placeholder_kpi_grounded_target"


def build_governance_placeholder_target(metadata, section_kpis):
    ph = base_placeholder_values(metadata)
    sentences = [
        "{{company}} disclosed selected governance-related information for {{reporting_year}} based on the available metadata and structured indicators."
    ]

    has_any = False

    if metadata.get("framework_used"):
        ph["{{framework_used}}"] = str(metadata.get("framework_used"))
        has_any = True
        sentences.append("The disclosure references the {{framework_used}} reporting framework.")

    if metadata.get("brsr_version"):
        ph["{{brsr_version}}"] = str(metadata.get("brsr_version"))
        has_any = True
        sentences.append("The BRSR version or disclosure basis is recorded as {{brsr_version}}.")

    if metadata.get("assurance_type"):
        ph["{{assurance_type}}"] = str(metadata.get("assurance_type"))
        has_any = True
        sentences.append("The assurance type is recorded as {{assurance_type}}.")

    if add_value(ph, "{{women_on_board_pct}}", section_kpis.get("women_on_board_percent")):
        has_any = True
        sentences.append("Women on the Board represented {{women_on_board_pct}}% of the disclosed Board composition.")

    if not has_any:
        return None, ph, None

    sentences.append(
        "The section does not add unsupported claims on Board committees, ethics mechanisms, whistle-blower systems, compliance processes, or risk-governance structures unless such information is present in the input evidence."
    )

    return " ".join(sentences), ph, "placeholder_kpi_grounded_target"


def build_targets_placeholder_target(metadata, section_kpis):
    ph = base_placeholder_values(metadata)

    if not should_add_targets_transition(section_kpis):
        return None, ph, None

    sentences = [
        "{{company}} disclosed selected target and transition-related indicators for {{reporting_year}} based on the available KPI data and evidence."
    ]

    has_any = False

    if is_positive_number(section_kpis.get("targets_count")):
        ph["{{targets_count}}"] = str(int(num_value(section_kpis.get("targets_count"))))
        has_any = True
        sentences.append("The structured KPI data identifies {{targets_count}} target-related disclosure item(s).")

    if has_valid_net_zero_target(section_kpis.get("net_zero_target_year")):
        ph["{{net_zero_target_year}}"] = str(int(num_value(section_kpis.get("net_zero_target_year"))))
        has_any = True
        sentences.append("The company has disclosed a net-zero or carbon-neutrality target year of {{net_zero_target_year}}.")

    if is_positive_number(section_kpis.get("direct_yoy_reductions_count")):
        ph["{{direct_reductions_count}}"] = str(int(num_value(section_kpis.get("direct_yoy_reductions_count"))))
        has_any = True
        sentences.append("The structured KPI data identifies {{direct_reductions_count}} direct year-on-year reduction-related item(s).")

    reduction_fields = {
        "scope1_emissions_yoy_reduction_percent": ("Scope 1 emissions", "{{scope1_yoy_text}}"),
        "scope2_emissions_yoy_reduction_percent": ("Scope 2 emissions", "{{scope2_yoy_text}}"),
        "scope3_emissions_yoy_reduction_percent": ("Scope 3 emissions", "{{scope3_yoy_text}}"),
        "scope1_scope2_yoy_reduction_percent": ("Combined Scope 1 and Scope 2 emissions", "{{scope1_scope2_yoy_text}}"),
        "energy_intensity_yoy_reduction_percent": ("Energy intensity", "{{energy_intensity_yoy_text}}"),
        "water_consumption_yoy_reduction_percent": ("Water consumption", "{{water_consumption_yoy_text}}"),
        "water_withdrawal_yoy_reduction_percent": ("Water withdrawal", "{{water_withdrawal_yoy_text}}"),
    }

    for field, (label, ph_name) in reduction_fields.items():
        x = num_value(section_kpis.get(field))
        if x is not None and x > 0:
            if add_yoy_value(ph, ph_name, section_kpis.get(field)):
                has_any = True
                sentences.append(f"{label} {ph_name}.")

    if not has_any:
        return None, ph, None

    sentences.append(
        "No additional transition-plan details, offsets, science-based target claims, or external framework alignment have been added unless supported by the provided KPIs or evidence."
    )

    return " ".join(sentences), ph, "placeholder_kpi_grounded_target"


def build_placeholder_target(metadata, section_id, section_kpis):
    if section_id == "disclosure_limitations":
        return build_disclosure_limitations_placeholder_target(metadata)

    builders = {
        "company_overview": build_company_overview_placeholder_target,
        "environmental_performance": build_environmental_placeholder_target,
        "ghg_emissions": build_ghg_placeholder_target,
        "energy_management": build_energy_placeholder_target,
        "water_management": build_water_placeholder_target,
        "waste_management": build_waste_placeholder_target,
        "social_diversity": build_social_placeholder_target,
        "governance": build_governance_placeholder_target,
        "targets_transition": build_targets_placeholder_target,
    }

    builder = builders.get(section_id)

    if not builder:
        return None, base_placeholder_values(metadata), None

    target, ph, source = builder(metadata, section_kpis)

    if target:
        return target, ph, source

    if INCLUDE_MISSING_DATA_SECTIONS:
        return build_missing_data_placeholder_target(metadata, section_id)

    return None, ph, None


# =============================================================================
# PROMPT CREATION
# =============================================================================

def build_base_context(metadata, kpis, evidence_items=None, placeholder_values=None):
    metadata_json = truncate_json_string(
        safe_json(metadata, indent=2),
        MAX_METADATA_JSON_CHARS,
    )

    kpi_json = truncate_json_string(
        safe_json(kpis, indent=2),
        MAX_KPI_JSON_CHARS,
    )

    evidence_text = "No additional KPI evidence snippets available."

    if evidence_items:
        ev_lines = []

        for i, ev in enumerate(evidence_items, start=1):
            ev_lines.append(f"[Evidence {i} | {ev.get('source')}]\n{ev.get('text')}")

        evidence_text = "\n\n".join(ev_lines)

    placeholder_text = "No placeholders available."

    if placeholder_values:
        placeholder_text = placeholder_fact_sheet(placeholder_values)

    return f"""Company metadata:
{metadata_json}

KPI data:
{kpi_json}

Allowed placeholders and exact replacement values:
{placeholder_text}

KPI evidence snippets:
{evidence_text}
"""


def make_section_generation_prompt(metadata, section_id, section_kpis, evidence_items, placeholder_values):
    title = SECTION_TITLES[section_id]
    instruction = SECTION_INSTRUCTIONS[section_id]

    context = build_base_context(
        metadata=metadata,
        kpis=section_kpis,
        evidence_items=evidence_items,
        placeholder_values=placeholder_values,
    )

    allowed_placeholders = ", ".join(sorted(placeholder_values.keys()))

    return f"""Task:
Generate the "{title}" section of a KPI- and evidence-supported ESG/BRSR report.

Section instruction:
{instruction}

{context}

Allowed placeholder tokens:
{allowed_placeholders}

Writing requirements:
- Generate only the requested "{title}" section.
- Use placeholder tokens instead of actual company names, years, numbers, percentages, or KPI values.
- Use only placeholders listed in "Allowed placeholder tokens".
- Do not invent new placeholder names.
- Do not write actual KPI numbers directly in the assistant answer.
- Preserve units in the sentence, such as tCO2e, GJ, KL, tonnes and %.
- Do not invent missing KPI values, policies, awards, TCFD alignment, science-based targets, carbon offsets, committees, initiatives, or stakeholder engagement.
- If a topic is not supported by KPI data or evidence, avoid claiming it.
- Use reporting-year language, not generic future-promise language.
- Avoid repetitive sentence patterns such as "The Company will also disclose".
- Write in formal ESG/BRSR reporting style.
- Do not output JSON, markdown headings, tables, or bullet points.
""".strip()


def make_sft_example(
    system_prompt,
    user_prompt,
    assistant_text,
    key,
    metadata,
    task,
    section_id,
    target_source,
    placeholder_values,
):
    filled_reference = fill_placeholders(assistant_text, placeholder_values)
    validation = validate_template_placeholders(assistant_text, placeholder_values)

    return {
        "messages": [
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": user_prompt,
            },
            {
                "role": "assistant",
                "content": normalize_text(assistant_text),
            },
        ],
        "source_key": key,
        "company": metadata.get("company"),
        "company_raw": metadata.get("company_raw"),
        "nse_symbol": metadata.get("nse_symbol"),
        "reporting_year": metadata.get("reporting_year"),
        "task": task,
        "section": section_id,
        "section_title": SECTION_TITLES.get(section_id),
        "target_source": target_source,
        "placeholder_values": placeholder_values,
        "filled_reference": filled_reference,
        "target_chars": len(normalize_text(assistant_text)),
        "filled_reference_chars": len(filled_reference),
        "user_chars": len(user_prompt),
        "target_placeholders": validation["target_placeholders"],
        "missing_placeholder_values": validation["missing_placeholder_values"],
    }


# =============================================================================
# DATASET BUILDING
# =============================================================================

def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


# =============================================================================
# MISSING-DATA DOWNSAMPLING + FINAL PLACEHOLDER SFT DATASET BUILDER
# =============================================================================

MAX_MISSING_DATA_ROWS_PER_SECTION = 75


def downsample_missing_data_examples(examples, max_missing_per_section=75, seed=42):
    """
    Keep all KPI-grounded and disclosure-limitation examples.
    Downsample placeholder_missing_data examples section-wise so that one section
    like social_diversity does not dominate the training dataset.
    """
    import random
    from collections import defaultdict

    random.seed(seed)

    keep = []
    missing_by_section = defaultdict(list)

    for ex in examples:
        if ex.get("target_source") == "placeholder_missing_data":
            missing_by_section[ex.get("section")].append(ex)
        else:
            keep.append(ex)

    for section, rows in missing_by_section.items():
        if len(rows) > max_missing_per_section:
            rows = random.sample(rows, max_missing_per_section)

        keep.extend(rows)

    return keep


def find_bad_single_brace_placeholders(text):
    """
    Finds bad placeholders like {reporting_year}.
    Valid placeholders should look like {{reporting_year}}.
    """
    return re.findall(r"(?<!\{)\{[A-Za-z0-9_]+\}(?!\})", text or "")


def build_placeholder_sft_dataset():
    symbol_map, source_key_map = load_company_name_map(COMPANY_NAME_MAP_CSV)

    kpi_index = index_jsonl_by_key(KPI_JSONL)
    metadata_index = index_jsonl_by_key(METADATA_JSONL)

    all_examples = []
    skipped = Counter()

    # =========================================================================
    # BUILD RAW EXAMPLES
    # =========================================================================

    for key, kpi_row in tqdm(kpi_index.items(), desc="Building placeholder SFT dataset"):
        all_kpis = get_flat_kpis(kpi_row)

        if not all_kpis:
            skipped["no_flat_kpis"] += 1
            continue

        metadata = extract_metadata(
            metadata_row=metadata_index.get(key),
            kpi_row=kpi_row,
            symbol_map=symbol_map,
            source_key_map=source_key_map,
            source_key=key,
        )

        for section_id in SECTION_ORDER:
            section_kpis = get_section_kpis(all_kpis, section_id)
            evidence_items = collect_kpi_evidence(kpi_row, section_id)

            target_template, placeholder_values, target_source = build_placeholder_target(
                metadata=metadata,
                section_id=section_id,
                section_kpis=section_kpis,
            )

            used_placeholders = set(extract_placeholders(target_template))
            placeholder_values = {
                k: v for k, v in placeholder_values.items()
                if k in used_placeholders
            }

            if not target_template:
                skipped[f"{section_id}:no_target"] += 1
                continue

            validation = validate_template_placeholders(
                target_template,
                placeholder_values,
            )

            if validation["has_missing_placeholder_values"]:
                skipped[f"{section_id}:missing_placeholder_values"] += 1
                continue

            bad_single_placeholders = find_bad_single_brace_placeholders(target_template)

            if bad_single_placeholders:
                skipped[f"{section_id}:bad_single_brace_placeholder"] += 1
                continue

            prompt = make_section_generation_prompt(
                metadata=metadata,
                section_id=section_id,
                section_kpis=section_kpis,
                evidence_items=evidence_items,
                placeholder_values=placeholder_values,
            )

            example = make_sft_example(
                system_prompt=PLACEHOLDER_SECTION_SYSTEM_PROMPT,
                user_prompt=prompt,
                assistant_text=target_template,
                key=key,
                metadata=metadata,
                task="section_generation_placeholder",
                section_id=section_id,
                target_source=target_source,
                placeholder_values=placeholder_values,
            )

            # Store extra audit fields inside the example so audit can be rebuilt
            # after missing-data downsampling.
            example["num_section_kpis"] = len(section_kpis)
            example["num_evidence_items"] = len(evidence_items)
            example["num_placeholders"] = len(placeholder_values)

            all_examples.append(example)

    raw_example_count = len(all_examples)

    # =========================================================================
    # DOWNSAMPLE MISSING-DATA EXAMPLES BEFORE SPLIT / WRITE / AUDIT
    # =========================================================================

    all_examples = downsample_missing_data_examples(
        examples=all_examples,
        max_missing_per_section=MAX_MISSING_DATA_ROWS_PER_SECTION,
        seed=RANDOM_SEED,
    )

    final_example_count = len(all_examples)

    print("Examples before missing-data downsampling:", raw_example_count)
    print("Examples after missing-data downsampling :", final_example_count)

    # =========================================================================
    # SPLIT BY SOURCE KEY
    # =========================================================================

    train_rows, valid_rows, test_rows = [], [], []

    for ex in all_examples:
        split = stable_split(ex["source_key"])

        if split == "train":
            train_rows.append(ex)
        elif split == "valid":
            valid_rows.append(ex)
        else:
            test_rows.append(ex)

    # =========================================================================
    # WRITE DATASETS
    # =========================================================================

    write_jsonl(FULL_JSONL, all_examples)
    write_jsonl(TRAIN_JSONL, train_rows)
    write_jsonl(VALID_JSONL, valid_rows)
    write_jsonl(TEST_JSONL, test_rows)

    # =========================================================================
    # REBUILD AUDIT FROM FINAL DOWNSAMPLED EXAMPLES
    # =========================================================================

    audit_rows_final = []

    for ex in all_examples:
        audit_rows_final.append({
            "source_key": ex.get("source_key"),
            "company": ex.get("company"),
            "company_raw": ex.get("company_raw"),
            "nse_symbol": ex.get("nse_symbol"),
            "reporting_year": ex.get("reporting_year"),
            "section": ex.get("section"),
            "section_title": ex.get("section_title"),
            "target_source": ex.get("target_source"),
            "num_section_kpis": ex.get("num_section_kpis"),
            "num_evidence_items": ex.get("num_evidence_items"),
            "num_placeholders": ex.get("num_placeholders"),
            "target_chars": ex.get("target_chars"),
            "filled_reference_chars": ex.get("filled_reference_chars"),
            "user_chars": ex.get("user_chars"),
            "split": stable_split(ex.get("source_key")),
            "missing_placeholder_values": ", ".join(ex.get("missing_placeholder_values", [])),
            "target_placeholders": ", ".join(ex.get("target_placeholders", [])),
        })

    audit_df = pd.DataFrame(audit_rows_final)
    audit_df.to_csv(AUDIT_CSV, index=False)

    # =========================================================================
    # SAVE BETTER SAMPLE FILE
    # First prefer one KPI-grounded sample per section.
    # If no KPI-grounded sample exists, use any available sample.
    # =========================================================================

    samples = []
    seen_sections = set()

    # Prefer KPI-grounded samples
    for ex in all_examples:
        sec = ex.get("section")

        if sec in seen_sections:
            continue

        if ex.get("target_source") == "placeholder_kpi_grounded_target":
            samples.append(ex)
            seen_sections.add(sec)

    # Fill remaining sections with any sample
    for ex in all_examples:
        sec = ex.get("section")

        if sec in seen_sections:
            continue

        samples.append(ex)
        seen_sections.add(sec)

        if len(seen_sections) == len(SECTION_ORDER):
            break

    write_jsonl(SAMPLE_JSONL, samples)

    with open(SECTION_ORDER_JSON, "w", encoding="utf-8") as f:
        json.dump(
            {
                "section_order": SECTION_ORDER,
                "section_titles": SECTION_TITLES,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    # =========================================================================
    # FINAL VALIDATION PRINTS
    # =========================================================================

    print("\nDONE")
    print("Total examples:", len(all_examples))
    print("Train:", len(train_rows))
    print("Valid:", len(valid_rows))
    print("Test :", len(test_rows))

    print("\nSkipped:")
    for k, v in skipped.most_common():
        print(f"  {k}: {v}")

    print("\nTarget source counts:")
    print(audit_df["target_source"].value_counts(dropna=False))

    print("\nTarget source by section:")
    display(pd.crosstab(audit_df["section"], audit_df["target_source"]))

    print("\nSection counts:")
    print(audit_df["section"].value_counts(dropna=False).sort_index())

    print("\nMissing placeholder values rows:")
    missing_count = (
        audit_df["missing_placeholder_values"]
        .fillna("")
        .astype(str)
        .str.strip()
        .ne("")
        .sum()
    )
    print(missing_count)

    print("\nBad single-brace placeholders check:")
    bad_single = []

    for ex in all_examples:
        target = ex["messages"][2]["content"]
        bad = find_bad_single_brace_placeholders(target)

        if bad:
            bad_single.append({
                "company": ex.get("company"),
                "section": ex.get("section"),
                "target_source": ex.get("target_source"),
                "bad_placeholders": ", ".join(sorted(set(bad))),
                "target_preview": target[:300],
            })

    bad_single_df = pd.DataFrame(bad_single)
    print("Rows with bad single-brace placeholders:", len(bad_single_df))

    if len(bad_single_df):
        display(bad_single_df.head(50))

    print("\nSaved:")
    print("FULL :", FULL_JSONL)
    print("TRAIN:", TRAIN_JSONL)
    print("VALID:", VALID_JSONL)
    print("TEST :", TEST_JSONL)
    print("AUDIT:", AUDIT_CSV)
    print("SAMPLE:", SAMPLE_JSONL)

    return audit_df, all_examples


audit_df, all_examples = build_placeholder_sft_dataset()

# =============================================================================
# SAMPLE INSPECTION
# =============================================================================

def inspect_one_sample_per_section(path=SAMPLE_JSONL):
    print("\n" + "=" * 120)
    print("ONE SAMPLE PER SECTION")
    print("=" * 120)

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)

            print("\n" + "=" * 120)
            print("SECTION:", obj.get("section"))
            print("COMPANY:", obj.get("company"))
            print("TARGET SOURCE:", obj.get("target_source"))

            print("\nASSISTANT TARGET TEMPLATE:\n")
            print(obj["messages"][2]["content"])

            print("\nPLACEHOLDER VALUES:\n")
            print(json.dumps(obj["placeholder_values"], indent=2, ensure_ascii=False))

            print("\nFILLED REFERENCE PREVIEW:\n")
            print(obj["filled_reference"][:1500])


inspect_one_sample_per_section()