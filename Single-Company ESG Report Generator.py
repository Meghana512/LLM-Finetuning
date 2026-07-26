# Single-Company ESG Report Generator — Raw KPI + Metadata Input
# =============================================================================
# Kaggle/Colab usually already has these in your training environment.
# Uncomment only if needed.

# !pip install -q unsloth trl transformers accelerate bitsandbytes peft

# CONFIG
# =============================================================================

import os
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

# Path to your final LoRA adapter
LORA_DIR = "/kaggle/input/datasets/vaibhavmeena23/final-lora-v3"

# Input mode:
#   "raw_json"        -> use RAW_METADATA + RAW_KPIS + RAW_EVIDENCE below, or RAW_INPUT_JSON_PATH
#   "converted_jsonl" -> select company from existing placeholder JSONL
INPUT_MODE = "converted_jsonl"

# Existing placeholder JSONL, only used when INPUT_MODE = "converted_jsonl"
INPUT_SECTION_JSONL = "/kaggle/input/datasets/vaibhavmeena23/final-llama-placeholder/llama_esg_placeholder_all_24-25 (1).jsonl"
COMPANY_QUERY = "Swiggy Limited"
SOURCE_KEY = None

# Raw JSON input option 1: load from a JSON file.
# Expected JSON format:
# {
#   "metadata": {...},
#   "kpis": {...},
#   "evidence": ["...", "..."]
# }
RAW_INPUT_JSON_PATH = None
# RAW_INPUT_JSON_PATH = "/kaggle/input/my-input/company_input.json"

# Raw JSON input option 2: paste metadata/kpis/evidence directly here.
RAW_METADATA = {
    "company": "Alicon Castalloy Limited",
    "nse_symbol": "ALICON",
    "reporting_year": "FY 2024-25",
    "sector": "Industrials",
    "framework_used": "BRSR",
    "brsr_version": "BRSR",
    "assurance_type": "Third Party Assurance",
    "geography": "India",
}

RAW_KPIS = {
    "scope1_emissions_tco2e_current": 14258.71,
    "scope1_emissions_tco2e_previous": 17410.45,
    "scope1_emissions_yoy_reduction_percent": 18.10257632628681,

    "scope2_emissions_tco2e_current": 28662.36,
    "scope2_emissions_tco2e_previous": 33634.55,
    "scope2_emissions_yoy_reduction_percent": 14.782983568978928,

    "scope3_emissions_tco2e_current": 95.0,
    "scope3_emissions_tco2e_previous": 30.0,
    "scope3_emissions_yoy_reduction_percent": -216.66666666666666,

    "scope1_scope2_total_tco2e_current": 42921.07,
    "scope1_scope2_total_tco2e_previous": 51045.0,
    "scope1_scope2_yoy_reduction_percent": 15.915231658340682,

    # Add more KPIs if available:
    # "energy_consumption_gj_current": ...,
    # "energy_consumption_gj_previous": ...,
    # "renewable_energy_consumption_gj_current": ...,
    # "water_consumption_kl_current": ...,
    # "water_consumption_kl_previous": ...,
    # "water_withdrawal_kl_current": ...,
    # "water_withdrawal_kl_previous": ...,
    # "total_waste_generated_tonnes_current": ...,
    # "total_waste_generated_tonnes_previous": ...,
    # "waste_recycled_tonnes_current": ...,
    # "waste_recycled_tonnes_previous": ...,
    # "women_employees_percent_current": ...,
    # "women_workers_percent_current": ...,
    # "target_count": ...,
}

RAW_EVIDENCE = [
    "Parameter Unit | FY 2024-25 | FY2023-24 Total Scope 1 emissions Metric tonnes of CO2 equivalent | 14,258.71 | 17,410.45 Total Scope 2 emissions Metric tonnes of CO2 equivalent | 28,662.36 | 33,634.55 Total Scope 3 emissions | 95 | 30"
]

OUTPUT_DIR = "/kaggle/working/single_company_esg_report_raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

MAX_SEQ_LENGTH = 4096
MAX_NEW_TOKENS = 450

USE_FALLBACK_ON_INVALID = True

# For raw JSON mode, these sections are generated.
# Missing-data sections are created safely when no KPI data exists.
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


# =============================================================================
# LOAD MODEL
# =============================================================================

import re
import json
import math
import torch
import pandas as pd
from pathlib import Path
from tqdm.auto import tqdm
from collections import defaultdict

from unsloth import FastLanguageModel
from unsloth.chat_templates import get_chat_template

model, tokenizer = FastLanguageModel.from_pretrained(
    model_name=LORA_DIR,
    max_seq_length=MAX_SEQ_LENGTH,
    dtype=None,
    load_in_4bit=True,
)

tokenizer = get_chat_template(
    tokenizer,
    chat_template="llama-3.1",
)

FastLanguageModel.for_inference(model)

print("Loaded LoRA:", LORA_DIR)


# =============================================================================
# GENERAL HELPERS
# =============================================================================

def is_missing(v):
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    if isinstance(v, str) and v.strip().lower() in {"", "nan", "none", "null", "na", "n/a", "unknown", "not disclosed"}:
        return True
    return False


def clean_text(x):
    if x is None:
        return ""
    x = str(x)
    x = x.replace("\u00a0", " ").replace("\u200b", " ")
    x = re.sub(r"\s+", " ", x).strip()
    return x


def format_number(v):
    if is_missing(v):
        return None

    try:
        x = float(str(v).replace(",", ""))
    except Exception:
        return clean_text(v)

    if abs(x - round(x)) < 1e-9:
        return f"{int(round(x)):,}"

    # Keep up to 2 decimals for normal KPI values, more for tiny intensities.
    if abs(x) < 0.01:
        return f"{x:.8f}".rstrip("0").rstrip(".")

    return f"{x:,.2f}".rstrip("0").rstrip(".")


def format_percent(v):
    if is_missing(v):
        return None

    try:
        x = float(str(v).replace(",", ""))
    except Exception:
        return clean_text(v)

    return f"{abs(x):.2f}".rstrip("0").rstrip(".") + "%"


def yoy_text_from_reduction_percent(v):
    """
    Your KPI fields use reduction_percent convention:
      positive = reduced
      negative = increased
    """
    if is_missing(v):
        return None

    try:
        x = float(str(v).replace(",", ""))
    except Exception:
        return None

    pct = format_percent(x)

    if x > 0:
        return f"reduced by {pct} compared with the previous year"
    if x < 0:
        return f"increased by {pct} compared with the previous year"

    return "remained unchanged compared with the previous year"


def first_present(d, keys):
    for k in keys:
        if k in d and not is_missing(d[k]):
            return d[k]
    return None


def has_any_key(d, keys):
    return any(k in d and not is_missing(d[k]) for k in keys)


def normalize_source_key(company):
    s = clean_text(company).lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s or "raw_company_input"


def evidence_text(evidence, max_chars=1200):
    if isinstance(evidence, list):
        txt = "\n".join(f"[Evidence {i+1}] {clean_text(x)}" for i, x in enumerate(evidence) if clean_text(x))
    elif isinstance(evidence, dict):
        parts = []
        for k, v in evidence.items():
            if clean_text(v):
                parts.append(f"[{k}] {clean_text(v)}")
        txt = "\n".join(parts)
    else:
        txt = clean_text(evidence)

    return txt[:max_chars]


# =============================================================================
# PLACEHOLDER VALIDATION HELPERS
# =============================================================================

def extract_placeholders(text):
    return sorted(set(re.findall(r"\{\{[A-Za-z0-9_]+\}\}", text or "")))


def find_bad_single_brace_placeholders(text):
    return re.findall(r"(?<!\{)\{[A-Za-z0-9_]+\}(?!\})", text or "")


def find_malformed_placeholder_fragments(text):
    if not isinstance(text, str):
        return ["non_string_output"]

    problems = []
    valid_placeholder_pattern = r"\{\{[A-Za-z0-9_]+\}\}"
    text_without_valid = re.sub(valid_placeholder_pattern, "", text)

    if "{{" in text_without_valid or "}}" in text_without_valid:
        for m in re.finditer(r"\{\{|\}\}", text_without_valid):
            start = max(0, m.start() - 40)
            end = min(len(text_without_valid), m.end() + 80)
            problems.append(text_without_valid[start:end].strip())

    spaced_placeholders = re.findall(r"\{\{[^{}]*\s+[^{}]*\}\}", text)
    problems.extend(spaced_placeholders)

    bad_closing = re.findall(r"\{\{[A-Za-z0-9_\s]+?\)\)", text)
    problems.extend(bad_closing)

    return sorted(set([p for p in problems if p]))


def fill_placeholders(text, placeholder_values):
    out = text or ""

    for ph, value in sorted(
        placeholder_values.items(),
        key=lambda x: len(x[0]),
        reverse=True,
    ):
        out = out.replace(ph, str(value))

    return out


PLACEHOLDER_ALIASES = {
    "{{total_waste_generated_current}}": "{{total_waste_current}}",
    "{{total_waste_generated_previous}}": "{{total_waste_previous}}",
    "{{water_consumption_kl_current}}": "{{water_consumption_current}}",
    "{{water_consumption_kl_previous}}": "{{water_consumption_previous}}",
    "{{water_withdrawal_kl_current}}": "{{water_withdrawal_current}}",
    "{{water_withdrawal_kl_previous}}": "{{water_withdrawal_previous}}",
    "{{renewable_energy_consumption_gj}}": "{{renewable_energy_current}}",
    "{{renewable_energy_consumption_current}}": "{{renewable_energy_current}}",
    "{{energy_consumption_gj_current}}": "{{energy_consumption_current}}",
    "{{energy_consumption_gj_previous}}": "{{energy_consumption_previous}}",
}


def normalize_generated_placeholders(text, placeholder_values):
    out = text or ""
    allowed = set(placeholder_values.keys())

    for bad, good in PLACEHOLDER_ALIASES.items():
        if bad in out and good in allowed:
            out = out.replace(bad, good)

    return out


def find_possible_numeric_leaks(text):
    clean = re.sub(r"\{\{[A-Za-z0-9_]+\}\}", "", text or "")
    nums = re.findall(r"\b\d[\d,]*(?:\.\d+)?\b", clean)

    suspicious = []
    for n in nums:
        if n in {"1", "2", "3"}:
            continue
        suspicious.append(n)

    return suspicious


def validate_generated_template(generated_template, placeholder_values):
    allowed = set(placeholder_values.keys())
    used = set(extract_placeholders(generated_template))

    invalid_placeholders = sorted(used - allowed)
    unused_allowed_placeholders = sorted(allowed - used)
    bad_single_brace = find_bad_single_brace_placeholders(generated_template)
    malformed_fragments = find_malformed_placeholder_fragments(generated_template)

    filled_output = fill_placeholders(generated_template, placeholder_values)
    unfilled_after_replacement = extract_placeholders(filled_output)

    numeric_leaks = find_possible_numeric_leaks(generated_template)

    problems = []

    if invalid_placeholders:
        problems.append("invalid_placeholders")
    if bad_single_brace:
        problems.append("bad_single_brace")
    if malformed_fragments:
        problems.append("malformed_placeholder")
    if unfilled_after_replacement:
        problems.append("unfilled_after_replacement")

    # Numeric leaks are audit warnings only, not hard invalid.

    return {
        "ok": len(problems) == 0,
        "problems": problems,
        "allowed_placeholders": sorted(allowed),
        "used_placeholders": sorted(used),
        "invalid_placeholders": invalid_placeholders,
        "unused_allowed_placeholders": unused_allowed_placeholders,
        "bad_single_brace": bad_single_brace,
        "malformed_fragments": malformed_fragments,
        "unfilled_after_replacement": unfilled_after_replacement,
        "numeric_leaks": numeric_leaks,
        "filled_output": filled_output,
    }


def remove_duplicate_sentences(text):
    if not isinstance(text, str):
        return text

    parts = re.split(r'(?<=[.!?])\s+', text.strip())
    seen = set()
    cleaned = []

    for s in parts:
        s_clean = re.sub(r"\s+", " ", s).strip()
        key = s_clean.lower()

        if not key:
            continue
        if key in seen:
            continue

        cleaned.append(s_clean)
        seen.add(key)

    return " ".join(cleaned)



# =============================================================================
# RAW KPI + METADATA TO PLACEHOLDER SECTION ROWS
# =============================================================================

SYSTEM_PROMPT = (
    "You are an ESG reporting analyst. Generate factual ESG/BRSR report sections using only the provided KPI data, "
    "KPI evidence, metadata, requested section, and allowed placeholders. You must use placeholders such as {{company}} "
    "and KPI placeholders instead of writing actual company names, numbers, years, percentages, or KPI values directly. "
    "Do not invent missing values, policies, awards, TCFD alignment, science-based targets, carbon offsets, committees, "
    "initiatives, or stakeholder engagements. Write in a formal sustainability reporting style suitable for Indian listed companies."
)


def load_raw_input():
    if RAW_INPUT_JSON_PATH:
        with open(RAW_INPUT_JSON_PATH, "r", encoding="utf-8") as f:
            obj = json.load(f)

        metadata = obj.get("metadata", {})
        kpis = obj.get("kpis", {})
        evidence = obj.get("evidence", [])
        return metadata, kpis, evidence

    return RAW_METADATA, RAW_KPIS, RAW_EVIDENCE


def base_placeholder_values(metadata):
    company = clean_text(metadata.get("company") or metadata.get("company_name") or "The company")
    reporting_year = clean_text(metadata.get("reporting_year") or "the reporting year")

    values = {
        "{{company}}": company,
        "{{reporting_year}}": reporting_year,
    }

    optional = {
        "{{sector}}": metadata.get("sector"),
        "{{framework_used}}": metadata.get("framework_used"),
        "{{brsr_version}}": metadata.get("brsr_version"),
        "{{assurance_type}}": metadata.get("assurance_type"),
        "{{geography}}": metadata.get("geography"),
        "{{nse_symbol}}": metadata.get("nse_symbol"),
    }

    for ph, v in optional.items():
        if not is_missing(v):
            values[ph] = clean_text(v)

    return values


def add_value(values, placeholder, value):
    if not is_missing(value):
        fmt = format_number(value)
        if fmt is not None:
            values[placeholder] = fmt


def add_text_value(values, placeholder, value):
    if not is_missing(value):
        values[placeholder] = clean_text(value)


def add_yoy(values, placeholder, value):
    text = yoy_text_from_reduction_percent(value)
    if text:
        values[placeholder] = text


def build_ghg_values(metadata, kpis):
    values = base_placeholder_values(metadata)

    add_value(values, "{{scope1_current}}", first_present(kpis, [
        "scope1_emissions_tco2e_current", "scope1_current", "scope1_emissions_current"
    ]))
    add_value(values, "{{scope1_previous}}", first_present(kpis, [
        "scope1_emissions_tco2e_previous", "scope1_previous", "scope1_emissions_previous"
    ]))
    add_yoy(values, "{{scope1_yoy_text}}", first_present(kpis, [
        "scope1_emissions_yoy_reduction_percent", "scope1_yoy_reduction_percent"
    ]))

    add_value(values, "{{scope2_current}}", first_present(kpis, [
        "scope2_emissions_tco2e_current", "scope2_current", "scope2_emissions_current"
    ]))
    add_value(values, "{{scope2_previous}}", first_present(kpis, [
        "scope2_emissions_tco2e_previous", "scope2_previous", "scope2_emissions_previous"
    ]))
    add_yoy(values, "{{scope2_yoy_text}}", first_present(kpis, [
        "scope2_emissions_yoy_reduction_percent", "scope2_yoy_reduction_percent"
    ]))

    add_value(values, "{{scope3_current}}", first_present(kpis, [
        "scope3_emissions_tco2e_current", "scope3_current", "scope3_emissions_current"
    ]))
    add_value(values, "{{scope3_previous}}", first_present(kpis, [
        "scope3_emissions_tco2e_previous", "scope3_previous", "scope3_emissions_previous"
    ]))
    add_yoy(values, "{{scope3_yoy_text}}", first_present(kpis, [
        "scope3_emissions_yoy_reduction_percent", "scope3_yoy_reduction_percent"
    ]))

    add_value(values, "{{scope1_scope2_current}}", first_present(kpis, [
        "scope1_scope2_total_tco2e_current", "scope1_scope2_total_current", "scope1_scope2_current"
    ]))
    add_value(values, "{{scope1_scope2_previous}}", first_present(kpis, [
        "scope1_scope2_total_tco2e_previous", "scope1_scope2_total_previous", "scope1_scope2_previous"
    ]))
    add_yoy(values, "{{scope1_scope2_yoy_text}}", first_present(kpis, [
        "scope1_scope2_yoy_reduction_percent", "scope1_scope2_total_yoy_reduction_percent"
    ]))

    return values


def build_energy_values(metadata, kpis):
    values = base_placeholder_values(metadata)

    add_value(values, "{{energy_consumption_current}}", first_present(kpis, [
        "energy_consumption_gj_current", "total_energy_consumption_gj_current", "energy_current"
    ]))
    add_value(values, "{{energy_consumption_previous}}", first_present(kpis, [
        "energy_consumption_gj_previous", "total_energy_consumption_gj_previous", "energy_previous"
    ]))
    add_yoy(values, "{{energy_consumption_yoy_text}}", first_present(kpis, [
        "energy_consumption_yoy_reduction_percent", "energy_yoy_reduction_percent"
    ]))

    add_value(values, "{{renewable_energy_current}}", first_present(kpis, [
        "renewable_energy_consumption_gj_current", "renewable_energy_current"
    ]))
    add_value(values, "{{renewable_energy_previous}}", first_present(kpis, [
        "renewable_energy_consumption_gj_previous", "renewable_energy_previous"
    ]))
    add_yoy(values, "{{renewable_energy_yoy_text}}", first_present(kpis, [
        "renewable_energy_yoy_reduction_percent"
    ]))

    add_value(values, "{{energy_intensity_current}}", first_present(kpis, [
        "energy_intensity_current", "energy_intensity_per_rupee_current"
    ]))
    add_value(values, "{{energy_intensity_previous}}", first_present(kpis, [
        "energy_intensity_previous", "energy_intensity_per_rupee_previous"
    ]))
    add_yoy(values, "{{energy_intensity_yoy_text}}", first_present(kpis, [
        "energy_intensity_yoy_reduction_percent"
    ]))

    return values


def build_water_values(metadata, kpis):
    values = base_placeholder_values(metadata)

    add_value(values, "{{water_consumption_current}}", first_present(kpis, [
        "water_consumption_kl_current", "water_consumption_current"
    ]))
    add_value(values, "{{water_consumption_previous}}", first_present(kpis, [
        "water_consumption_kl_previous", "water_consumption_previous"
    ]))
    add_yoy(values, "{{water_consumption_yoy_text}}", first_present(kpis, [
        "water_consumption_yoy_reduction_percent"
    ]))

    add_value(values, "{{water_withdrawal_current}}", first_present(kpis, [
        "water_withdrawal_kl_current", "water_withdrawal_current"
    ]))
    add_value(values, "{{water_withdrawal_previous}}", first_present(kpis, [
        "water_withdrawal_kl_previous", "water_withdrawal_previous"
    ]))
    add_yoy(values, "{{water_withdrawal_yoy_text}}", first_present(kpis, [
        "water_withdrawal_yoy_reduction_percent"
    ]))

    add_value(values, "{{water_recycled_current}}", first_present(kpis, [
        "water_recycled_kl_current", "water_recycled_current"
    ]))

    return values


def build_waste_values(metadata, kpis):
    values = base_placeholder_values(metadata)

    add_value(values, "{{total_waste_current}}", first_present(kpis, [
        "total_waste_generated_tonnes_current", "total_waste_current", "waste_generated_tonnes_current"
    ]))
    add_value(values, "{{total_waste_previous}}", first_present(kpis, [
        "total_waste_generated_tonnes_previous", "total_waste_previous", "waste_generated_tonnes_previous"
    ]))
    add_yoy(values, "{{total_waste_yoy_text}}", first_present(kpis, [
        "total_waste_yoy_reduction_percent", "waste_yoy_reduction_percent"
    ]))

    add_value(values, "{{waste_recycled_current}}", first_present(kpis, [
        "waste_recycled_tonnes_current", "waste_recycled_current", "waste_recovered_tonnes_current"
    ]))
    add_value(values, "{{waste_recycled_previous}}", first_present(kpis, [
        "waste_recycled_tonnes_previous", "waste_recycled_previous", "waste_recovered_tonnes_previous"
    ]))

    add_value(values, "{{waste_recycling_percent}}", first_present(kpis, [
        "waste_recycling_percent_current", "waste_recovery_percent_current"
    ]))

    return values


def build_social_values(metadata, kpis):
    values = base_placeholder_values(metadata)

    add_value(values, "{{women_employees_percent}}", first_present(kpis, [
        "women_employees_percent_current", "women_employees_percent", "female_employees_percent_current"
    ]))
    add_value(values, "{{women_workers_percent}}", first_present(kpis, [
        "women_workers_percent_current", "women_workers_percent", "female_workers_percent_current"
    ]))
    add_value(values, "{{permanent_employees_current}}", first_present(kpis, [
        "permanent_employees_current", "total_permanent_employees_current"
    ]))
    add_value(values, "{{permanent_workers_current}}", first_present(kpis, [
        "permanent_workers_current", "total_permanent_workers_current"
    ]))

    return values


def build_targets_values(metadata, kpis):
    values = base_placeholder_values(metadata)

    add_value(values, "{{target_count}}", first_present(kpis, [
        "target_count", "targets_count", "target_related_disclosure_count"
    ]))

    add_yoy(values, "{{scope1_yoy_text}}", first_present(kpis, [
        "scope1_emissions_yoy_reduction_percent", "scope1_yoy_reduction_percent"
    ]))
    add_yoy(values, "{{scope2_yoy_text}}", first_present(kpis, [
        "scope2_emissions_yoy_reduction_percent", "scope2_yoy_reduction_percent"
    ]))
    add_yoy(values, "{{scope1_scope2_yoy_text}}", first_present(kpis, [
        "scope1_scope2_yoy_reduction_percent", "scope1_scope2_total_yoy_reduction_percent"
    ]))

    return values


def build_overview_values(metadata, kpis):
    values = base_placeholder_values(metadata)
    merged = {}
    for builder in [build_ghg_values, build_energy_values, build_water_values, build_waste_values, build_social_values]:
        merged.update(builder(metadata, kpis))
    values.update(merged)
    return values


def build_environmental_values(metadata, kpis):
    values = base_placeholder_values(metadata)
    merged = {}
    for builder in [build_ghg_values, build_energy_values, build_water_values, build_waste_values]:
        merged.update(builder(metadata, kpis))
    values.update(merged)
    return values


def meaningful_kpi_placeholders(values):
    return [k for k in values.keys() if k not in {"{{company}}", "{{reporting_year}}", "{{sector}}", "{{framework_used}}", "{{brsr_version}}", "{{assurance_type}}", "{{geography}}", "{{nse_symbol}}"}]


def missing_data_template(section, metadata):
    title = SECTION_TITLES.get(section, section)
    return (
        f"{{{{company}}}} does not have sufficient structured KPI data in the provided input to generate a detailed {title} section for {{{{reporting_year}}}}. "
        "No unsupported claims have been added for this section. Additional verified KPI data or source evidence would be required to describe this topic in greater detail."
    )


def disclosure_template(metadata):
    return (
        "This ESG report section for {{company}} has been prepared using only the provided KPI dataset, KPI evidence and metadata. "
        "Where information on policies, governance mechanisms, stakeholder engagement, climate-risk processes, TCFD alignment, "
        "science-based targets, carbon offsets, awards, certifications, or specific initiatives is not present in the input data, "
        "no unsupported claim has been made. The narrative should therefore be read as a KPI- and evidence-supported ESG disclosure, "
        "not as a complete reproduction of the company's statutory BRSR filing."
    )


def reference_template_for_section(section, values):
    # These reference templates are safe fallback templates. LoRA output is used when valid.
    if section == "ghg_emissions":
        parts = ["{{company}} disclosed its greenhouse gas emissions for {{reporting_year}} based on the available Scope-wise emissions data."]
        if "{{scope1_current}}" in values:
            s = "Scope 1 emissions were {{scope1_current}} tCO2e"
            if "{{scope1_previous}}" in values:
                s += ", compared with {{scope1_previous}} tCO2e in the previous year"
            s += "."
            if "{{scope1_yoy_text}}" in values:
                s += " Scope 1 emissions {{scope1_yoy_text}}."
            parts.append(s)
        if "{{scope2_current}}" in values:
            s = "Scope 2 emissions were {{scope2_current}} tCO2e"
            if "{{scope2_previous}}" in values:
                s += ", compared with {{scope2_previous}} tCO2e in the previous year"
            s += "."
            if "{{scope2_yoy_text}}" in values:
                s += " Scope 2 emissions {{scope2_yoy_text}}."
            parts.append(s)
        if "{{scope3_current}}" in values:
            s = "Scope 3 emissions were {{scope3_current}} tCO2e"
            if "{{scope3_previous}}" in values:
                s += ", compared with {{scope3_previous}} tCO2e in the previous year"
            s += "."
            if "{{scope3_yoy_text}}" in values:
                s += " Scope 3 emissions {{scope3_yoy_text}}."
            parts.append(s)
        if "{{scope1_scope2_current}}" in values:
            s = "Combined Scope 1 and Scope 2 emissions stood at {{scope1_scope2_current}} tCO2e"
            if "{{scope1_scope2_previous}}" in values:
                s += ", compared with {{scope1_scope2_previous}} tCO2e in the previous year"
            s += "."
            if "{{scope1_scope2_yoy_text}}" in values:
                s += " Combined Scope 1 and Scope 2 emissions {{scope1_scope2_yoy_text}}."
            parts.append(s)
        parts.append("The disclosure is limited to the emissions KPIs and evidence available in the input data, and no unsupported claims have been made on climate targets, offsets, or external frameworks.")
        return " ".join(parts)

    if section == "energy_management":
        parts = ["{{company}} disclosed its energy management performance for {{reporting_year}} based on the available energy KPIs."]
        if "{{energy_consumption_current}}" in values:
            parts.append("Total energy consumption was {{energy_consumption_current}} GJ.")
        if "{{renewable_energy_current}}" in values:
            parts.append("Renewable energy consumption was {{renewable_energy_current}} GJ.")
        if "{{energy_intensity_current}}" in values:
            s = "Energy intensity was {{energy_intensity_current}}"
            if "{{energy_intensity_previous}}" in values:
                s += ", compared with {{energy_intensity_previous}} in the previous year"
            s += "."
            if "{{energy_intensity_yoy_text}}" in values:
                s += " Energy intensity {{energy_intensity_yoy_text}}."
            parts.append(s)
        parts.append("The section is limited to disclosed energy KPIs and evidence, and does not make unsupported claims about energy projects, certifications, or transition initiatives.")
        return " ".join(parts)

    if section == "water_management":
        parts = ["{{company}} disclosed its water management performance for {{reporting_year}} based on the available water KPIs."]
        if "{{water_consumption_current}}" in values:
            s = "Water consumption was {{water_consumption_current}} KL"
            if "{{water_consumption_previous}}" in values:
                s += ", compared with {{water_consumption_previous}} KL in the previous year"
            s += "."
            if "{{water_consumption_yoy_text}}" in values:
                s += " Water consumption {{water_consumption_yoy_text}}."
            parts.append(s)
        if "{{water_withdrawal_current}}" in values:
            s = "Water withdrawal was {{water_withdrawal_current}} KL"
            if "{{water_withdrawal_previous}}" in values:
                s += ", compared with {{water_withdrawal_previous}} KL in the previous year"
            s += "."
            parts.append(s)
        parts.append("The section is limited to disclosed water KPIs and evidence, and does not add unsupported claims on water stewardship programmes, recycling systems, or zero-liquid-discharge practices.")
        return " ".join(parts)

    if section == "waste_management":
        parts = ["{{company}} disclosed its waste management performance for {{reporting_year}} based on the available waste KPIs."]
        if "{{total_waste_current}}" in values:
            s = "Total waste generated was {{total_waste_current}} tonnes"
            if "{{total_waste_previous}}" in values:
                s += ", compared with {{total_waste_previous}} tonnes in the previous year"
            s += "."
            if "{{total_waste_yoy_text}}" in values:
                s += " Total waste generated {{total_waste_yoy_text}}."
            parts.append(s)
        if "{{waste_recycled_current}}" in values:
            parts.append("Waste recycled or recovered was {{waste_recycled_current}} tonnes.")
        if "{{waste_recycling_percent}}" in values:
            parts.append("The structured KPI data reports a waste recycling or recovery percentage of {{waste_recycling_percent}}%.")
        parts.append("The disclosure is limited to the waste KPIs present in the input data, and no unsupported claims have been made on waste-reduction initiatives, circularity programmes, or disposal practices.")
        return " ".join(parts)

    if section == "social_diversity":
        parts = ["{{company}} disclosed selected social and diversity indicators for {{reporting_year}} based on the available structured KPI data."]
        if "{{women_employees_percent}}" in values:
            parts.append("Women employees represented {{women_employees_percent}}% of employees based on the available disclosure.")
        if "{{women_workers_percent}}" in values:
            parts.append("Women workers represented {{women_workers_percent}}% of workers based on the available disclosure.")
        if "{{permanent_employees_current}}" in values:
            parts.append("Permanent employees were reported at {{permanent_employees_current}}.")
        if "{{permanent_workers_current}}" in values:
            parts.append("Permanent workers were reported at {{permanent_workers_current}}.")
        parts.append("The section does not add unsupported claims on employee benefits, human rights programmes, training systems, or community initiatives unless such evidence is present in the input.")
        return " ".join(parts)

    if section == "governance":
        parts = ["{{company}} disclosed selected governance-related information for {{reporting_year}} based on the available metadata and structured indicators."]
        if "{{framework_used}}" in values:
            parts.append("The disclosure references the {{framework_used}} reporting framework.")
        if "{{brsr_version}}" in values:
            parts.append("The BRSR version or disclosure basis is recorded as {{brsr_version}}.")
        if "{{assurance_type}}" in values:
            parts.append("The assurance type is recorded as {{assurance_type}}.")
        parts.append("The section does not add unsupported claims on Board committees, ethics mechanisms, whistle-blower systems, compliance processes, or risk-governance structures unless such information is present in the input evidence.")
        return " ".join(parts)

    if section == "targets_transition":
        parts = ["{{company}} disclosed selected target and transition-related indicators for {{reporting_year}} based on the available KPI data and evidence."]
        if "{{target_count}}" in values:
            parts.append("The structured KPI data identifies {{target_count}} target-related disclosure item(s).")
        if "{{scope1_yoy_text}}" in values:
            parts.append("Scope 1 emissions {{scope1_yoy_text}}.")
        if "{{scope2_yoy_text}}" in values:
            parts.append("Scope 2 emissions {{scope2_yoy_text}}.")
        if "{{scope1_scope2_yoy_text}}" in values:
            parts.append("Combined Scope 1 and Scope 2 emissions {{scope1_scope2_yoy_text}}.")
        parts.append("No additional transition-plan details, offsets, science-based target claims, or external framework alignment have been added unless supported by the provided KPIs or evidence.")
        return " ".join(parts)

    if section == "environmental_performance":
        parts = ["{{company}} disclosed selected environmental performance indicators for {{reporting_year}}, covering emissions, energy, water and waste where data was available."]
        if "{{scope1_scope2_current}}" in values:
            parts.append("Combined Scope 1 and Scope 2 emissions stood at {{scope1_scope2_current}} tCO2e.")
        if "{{scope3_current}}" in values:
            parts.append("Scope 3 emissions were {{scope3_current}} tCO2e.")
        if "{{energy_consumption_current}}" in values:
            parts.append("Total energy consumption was {{energy_consumption_current}} GJ.")
        if "{{water_consumption_current}}" in values:
            parts.append("Water consumption was {{water_consumption_current}} KL.")
        if "{{water_withdrawal_current}}" in values:
            parts.append("Water withdrawal was {{water_withdrawal_current}} KL.")
        if "{{total_waste_current}}" in values:
            parts.append("Total waste generated was {{total_waste_current}} tonnes.")
        if "{{waste_recycled_current}}" in values:
            parts.append("Waste recycled or recovered was {{waste_recycled_current}} tonnes.")
        parts.append("This environmental performance section is limited to the disclosed KPI data and evidence, and does not add unsupported environmental initiatives, certifications, or commitments.")
        return " ".join(parts)

    if section == "company_overview":
        parts = ["{{company}} has prepared this ESG/BRSR section for {{reporting_year}} using the available KPI data and supporting evidence."]
        if "{{framework_used}}" in values:
            parts.append("The disclosure is prepared with reference to the {{framework_used}} framework.")
        if "{{assurance_type}}" in values:
            parts.append("The assurance type is recorded as {{assurance_type}}.")
        if "{{scope1_current}}" in values:
            parts.append("Scope 1 emissions were reported at {{scope1_current}} tCO2e.")
        if "{{scope2_current}}" in values:
            parts.append("Scope 2 emissions were reported at {{scope2_current}} tCO2e.")
        if "{{scope3_current}}" in values:
            parts.append("Scope 3 emissions were reported at {{scope3_current}} tCO2e.")
        if "{{water_consumption_current}}" in values:
            parts.append("Water consumption was reported at {{water_consumption_current}} KL.")
        if "{{total_waste_current}}" in values:
            parts.append("Total waste generated was reported at {{total_waste_current}} tonnes.")
        parts.append("This overview is limited to the structured KPIs and evidence available in the input data and does not add unsupported policies, initiatives, awards, committees, or external-framework claims.")
        return " ".join(parts)

    return missing_data_template(section, {})


def section_instruction(section):
    instructions = {
        "company_overview": "Generate the Company ESG Overview section using available metadata and high-level KPI disclosures.",
        "environmental_performance": "Generate the Environmental Performance section covering emissions, energy, water and waste KPIs where available.",
        "ghg_emissions": "Generate the GHG Emissions section covering Scope 1, Scope 2, Scope 3 and year-on-year movement where available.",
        "energy_management": "Generate the Energy and Renewable Energy Management section covering energy consumption, renewable energy and energy intensity where available.",
        "water_management": "Generate the Water Management section covering water consumption, withdrawal, recycling and year-on-year movement where available.",
        "waste_management": "Generate the Waste Management section covering total waste, recycled/recovered waste and waste movement where available.",
        "social_diversity": "Generate the Social and Diversity section covering employee and workforce diversity KPIs where available.",
        "governance": "Generate the Governance section using available governance metadata and structured indicators.",
        "targets_transition": "Generate the Targets and Transition Plans section using available targets, transition and emissions movement indicators.",
        "disclosure_limitations": "Generate the Disclosure Scope and Limitations section explaining that the report is limited to provided KPIs, evidence and metadata.",
    }
    return instructions.get(section, f"Generate the {SECTION_TITLES.get(section, section)} section.")


def user_prompt_for_section(section, metadata, kpis, values, evidence):
    allowed_tokens = ", ".join(sorted(values.keys()))
    value_lines = "\n".join(f"{k}: {v}" for k, v in sorted(values.items()))

    return f"""Task:
Generate the \"{SECTION_TITLES.get(section, section)}\" section of a KPI- and evidence-supported ESG/BRSR report.

Section instruction:
{section_instruction(section)}

Company metadata:
{json.dumps(metadata, ensure_ascii=False, indent=2)}

KPI data:
{json.dumps(kpis, ensure_ascii=False, indent=2)}

Allowed placeholders and exact replacement values:
{value_lines}

KPI evidence snippets:
{evidence_text(evidence)}

Allowed placeholder tokens:
{allowed_tokens}

Writing requirements:
- Generate only the requested \"{SECTION_TITLES.get(section, section)}\" section.
- Use placeholder tokens instead of actual company names, years, numbers, percentages, or KPI values.
- Use only placeholders listed in \"Allowed placeholder tokens\".
- Do not invent new placeholder names.
- Do not write actual KPI numbers directly in the assistant answer.
- Preserve units in the sentence, such as tCO2e, GJ, KL, tonnes and %.
- Do not invent missing KPI values, policies, awards, TCFD alignment, science-based targets, carbon offsets, committees, initiatives, or stakeholder engagement.
- Use reporting-year language, not generic future-promise language.
- Avoid repetitive sentence patterns.
- Write in formal ESG/BRSR reporting style.
- Do not output JSON, markdown headings, tables, or bullet points."""


def build_values_for_section(section, metadata, kpis):
    builders = {
        "company_overview": build_overview_values,
        "environmental_performance": build_environmental_values,
        "ghg_emissions": build_ghg_values,
        "energy_management": build_energy_values,
        "water_management": build_water_values,
        "waste_management": build_waste_values,
        "social_diversity": build_social_values,
        "governance": lambda m, k: base_placeholder_values(m),
        "targets_transition": build_targets_values,
        "disclosure_limitations": lambda m, k: base_placeholder_values(m),
    }
    return builders.get(section, lambda m, k: base_placeholder_values(m))(metadata, kpis)


def build_section_rows_from_raw(metadata, kpis, evidence):
    rows = []
    company = clean_text(metadata.get("company") or metadata.get("company_name") or "The company")
    source_key = normalize_source_key(company)

    for section in SECTION_ORDER:
        values = build_values_for_section(section, metadata, kpis)
        meaningful = meaningful_kpi_placeholders(values)

        if section == "disclosure_limitations":
            target_source = "placeholder_disclosure_limitations"
            reference_template = disclosure_template(metadata)
        elif section == "governance":
            # Governance can be metadata-based.
            target_source = "placeholder_kpi_grounded_target" if any(k in values for k in ["{{framework_used}}", "{{brsr_version}}", "{{assurance_type}}"] ) else "placeholder_missing_data"
            reference_template = reference_template_for_section(section, values) if target_source != "placeholder_missing_data" else missing_data_template(section, metadata)
        elif meaningful:
            target_source = "placeholder_kpi_grounded_target"
            reference_template = reference_template_for_section(section, values)
        else:
            target_source = "placeholder_missing_data"
            reference_template = missing_data_template(section, metadata)

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt_for_section(section, metadata, kpis, values, evidence)},
            {"role": "assistant", "content": reference_template},
        ]

        row = {
            "messages": messages,
            "source_key": source_key,
            "company": company,
            "nse_symbol": metadata.get("nse_symbol"),
            "reporting_year": metadata.get("reporting_year"),
            "task": "section_generation_placeholder",
            "section": section,
            "section_title": SECTION_TITLES.get(section),
            "target_source": target_source,
            "placeholder_values": values,
            "filled_reference": fill_placeholders(reference_template, values),
        }
        rows.append(row)

    return rows

# =============================================================================
# CONVERTED JSONL MODE HELPERS
# =============================================================================

def load_section_rows(path):
    rows = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            obj = json.loads(line)

            if obj.get("task") != "section_generation_placeholder":
                continue

            if obj.get("target_source") not in {
                "placeholder_kpi_grounded_target",
                "placeholder_missing_data",
                "placeholder_disclosure_limitations",
            }:
                continue

            rows.append(obj)

    return rows


def group_rows_by_company(rows):
    groups = defaultdict(list)

    for row in rows:
        key = row.get("source_key") or row.get("company") or "unknown_company"
        groups[key].append(row)

    return groups


def select_company_rows_from_jsonl(path, company_query=None, source_key=None):
    rows = load_section_rows(path)
    groups = group_rows_by_company(rows)

    if source_key:
        if source_key not in groups:
            raise ValueError(f"SOURCE_KEY not found: {source_key}")
        return source_key, groups[source_key]

    if not company_query:
        raise ValueError("Provide COMPANY_QUERY or SOURCE_KEY.")

    cq = company_query.lower().strip()
    matches = []

    for key, group in groups.items():
        company = str(group[0].get("company") or "").lower().strip()
        if cq in company or company in cq:
            matches.append((key, group))

    if not matches:
        available = sorted({str(r.get("company")) for r in rows if r.get("company")})[:50]
        raise ValueError(f"No company matched: {company_query}. Available sample: {available}")

    if len(matches) > 1:
        print("Multiple matches found. Using first. Matches:")
        for k, g in matches[:10]:
            print("-", k, "|", g[0].get("company"))

    return matches[0]


# =============================================================================
# GENERATION + REPORT BUILDING
# =============================================================================

def generate_template_from_sample(sample):
    messages = sample["messages"][:2]

    prompt = tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True,
    )

    inputs = tokenizer(
        prompt,
        return_tensors="pt",
        truncation=True,
        max_length=MAX_SEQ_LENGTH,
    ).to("cuda")

    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=MAX_NEW_TOKENS,
            do_sample=False,
            repetition_penalty=1.0,
            eos_token_id=tokenizer.eos_token_id,
            pad_token_id=tokenizer.eos_token_id,
        )

    completion_ids = outputs[0][inputs["input_ids"].shape[-1]:]

    generated_template = tokenizer.decode(
        completion_ids,
        skip_special_tokens=True,
    ).strip()

    del inputs, outputs
    torch.cuda.empty_cache()

    return generated_template


def section_sort_key(row):
    sec = row.get("section")
    try:
        return SECTION_ORDER.index(sec)
    except ValueError:
        return 999


def dedupe_sections(rows):
    priority = {
        "placeholder_kpi_grounded_target": 0,
        "placeholder_disclosure_limitations": 1,
        "placeholder_missing_data": 2,
    }

    by_section = {}

    for row in rows:
        sec = row.get("section")
        if not sec:
            continue

        if sec not in by_section:
            by_section[sec] = row
            continue

        old = by_section[sec]
        old_p = priority.get(old.get("target_source"), 99)
        new_p = priority.get(row.get("target_source"), 99)

        if new_p < old_p:
            by_section[sec] = row

    return [by_section[s] for s in SECTION_ORDER if s in by_section]


def merge_sections(section_outputs):
    parts = []

    for sec in SECTION_ORDER:
        if sec not in section_outputs:
            continue

        text = section_outputs[sec].strip()
        if not text:
            continue

        title = SECTION_TITLES.get(sec, sec)
        parts.append(f"{title}\n\n{text}")

    separator = "\n\n" + "=" * 80 + "\n\n"
    return separator.join(parts)


def build_single_company_report(group_key, rows):
    rows = dedupe_sections(rows)
    rows = sorted(rows, key=section_sort_key)

    company = rows[0].get("company") if rows else "Unknown"
    nse_symbol = rows[0].get("nse_symbol") if rows else None
    reporting_year = rows[0].get("reporting_year") if rows else None

    section_outputs = {}
    section_records = []
    bad_sections = []

    for row in tqdm(rows, desc=f"Generating sections for {company}"):
        sec = row.get("section")
        target_source = row.get("target_source")
        placeholder_values = row.get("placeholder_values", {})
        reference_template = row["messages"][2]["content"]

        if target_source in {
            "placeholder_missing_data",
            "placeholder_disclosure_limitations",
        }:
            generated_template = reference_template
        else:
            generated_template = generate_template_from_sample(row)
        
        generated_template = normalize_generated_placeholders(
            generated_template,
            placeholder_values,
        )
        
        validation = validate_generated_template(
            generated_template,
            placeholder_values,
        )        

        used_fallback = False

        if validation["ok"]:
            final_section_text = validation["filled_output"]
            final_template = generated_template
        else:
            if USE_FALLBACK_ON_INVALID:
                used_fallback = True
                final_template = reference_template
                final_section_text = fill_placeholders(reference_template, placeholder_values)
            else:
                final_template = generated_template
                final_section_text = validation["filled_output"]

        final_section_text = remove_duplicate_sentences(final_section_text)

        section_outputs[sec] = final_section_text

        section_record = {
            "section": sec,
            "section_title": SECTION_TITLES.get(sec),
            "target_source": target_source,
            "ok": validation["ok"],
            "used_fallback": used_fallback,
            "problems": validation["problems"],
            "invalid_placeholders": validation["invalid_placeholders"],
            "malformed_fragments": validation["malformed_fragments"],
            "bad_single_brace": validation["bad_single_brace"],
            "unfilled_after_replacement": validation["unfilled_after_replacement"],
            "numeric_leaks": validation["numeric_leaks"],
            "allowed_placeholders": validation["allowed_placeholders"],
            "used_placeholders": validation["used_placeholders"],
            "reference_template": reference_template,
            "generated_template": generated_template,
            "final_template": final_template,
            "final_section_text": final_section_text,
        }

        section_records.append(section_record)

        if (not validation["ok"]) or used_fallback:
            bad_sections.append(section_record)

    final_report = merge_sections(section_outputs)

    company_record = {
        "source_key": group_key,
        "company": company,
        "nse_symbol": nse_symbol,
        "reporting_year": reporting_year,
        "num_sections": len(section_outputs),
        "sections": list(section_outputs.keys()),
        "section_outputs": section_outputs,
        "section_records": section_records,
        "num_invalid_sections": sum(1 for s in section_records if not s["ok"]),
        "num_fallback_sections": sum(1 for s in section_records if s["used_fallback"]),
        "final_report": final_report,
        "final_report_chars": len(final_report),
    }

    return company_record, bad_sections


# =============================================================================
# RUN SINGLE COMPANY GENERATION
# =============================================================================

if INPUT_MODE == "raw_json":
    metadata, kpis, evidence = load_raw_input()
    group_key = normalize_source_key(metadata.get("company") or metadata.get("company_name") or "raw_company_input")
    rows = build_section_rows_from_raw(metadata, kpis, evidence)
elif INPUT_MODE == "converted_jsonl":
    group_key, rows = select_company_rows_from_jsonl(
        INPUT_SECTION_JSONL,
        company_query=COMPANY_QUERY,
        source_key=SOURCE_KEY,
    )
else:
    raise ValueError("INPUT_MODE must be 'raw_json' or 'converted_jsonl'.")

record, bad_sections = build_single_company_report(group_key, rows)

safe_company = re.sub(r"[^A-Za-z0-9_\-]+", "_", record["company"]).strip("_") or "company"

REPORT_TXT = f"{OUTPUT_DIR}/{safe_company}_esg_report.txt"
REPORT_JSON = f"{OUTPUT_DIR}/{safe_company}_esg_report.json"
SECTION_AUDIT_CSV = f"{OUTPUT_DIR}/{safe_company}_section_audit.csv"
BAD_SECTIONS_JSONL = f"{OUTPUT_DIR}/{safe_company}_bad_sections.jsonl"

with open(REPORT_TXT, "w", encoding="utf-8") as f:
    f.write(record["final_report"])

with open(REPORT_JSON, "w", encoding="utf-8") as f:
    json.dump(record, f, ensure_ascii=False, indent=2)

section_audit_df = pd.DataFrame([
    {
        "company": record["company"],
        "section": s["section"],
        "target_source": s["target_source"],
        "ok": s["ok"],
        "used_fallback": s["used_fallback"],
        "problems": "; ".join(s["problems"]),
        "invalid_placeholders": ", ".join(s["invalid_placeholders"]),
        "numeric_leaks": ", ".join(s["numeric_leaks"][:20]),
    }
    for s in record["section_records"]
])
section_audit_df.to_csv(SECTION_AUDIT_CSV, index=False)

with open(BAD_SECTIONS_JSONL, "w", encoding="utf-8") as f:
    for s in bad_sections:
        f.write(json.dumps(s, ensure_ascii=False) + "\n")

print("DONE")
print("Company:", record["company"])
print("Sections:", record["sections"])
print("Invalid sections:", record["num_invalid_sections"])
print("Fallback sections:", record["num_fallback_sections"])
print("Report chars:", record["final_report_chars"])
print("Saved report txt:", REPORT_TXT)
print("Saved report json:", REPORT_JSON)
print("Saved section audit:", SECTION_AUDIT_CSV)
print("Saved bad sections:", BAD_SECTIONS_JSONL)

display(section_audit_df)

print("\n" + "=" * 120)
print(record["final_report"][:10000])

