#final complete prd esg dataset 
# =============================================================================
# COMPLETE PRD ESG DATASET MERGER
# =============================================================================
# Input:
#   1. Metadata JSONL
#   2. ESG section classification JSONL
#   3. KPI extraction JSONL
#   4. Paragraph intent labelling JSONL
#
# Output:
#   1. esg_prd_master_dataset.jsonl  -> one structured row per company/report
#   2. esg_prd_master_dataset.csv    -> flat version for checking
#
# Designed for LLM training / fine-tuning dataset preparation.
# =============================================================================

import os
import re
import json
from pathlib import Path
from collections import defaultdict, Counter

import pandas as pd
from tqdm.auto import tqdm


# =============================================================================
# CONFIG - CHANGE THESE PATHS
# =============================================================================

METADATA_JSONL = "/kaggle/input/datasets/vaibhavmeena23/prd-brsr-24-25-jsonl/metadata_dataset_24-25.jsonl"
USE_YEAR_IN_KEY = False

SECTION_JSONL = "/kaggle/input/datasets/vaibhavmeena23/prd-brsr-24-25-jsonl/esg_section_classification_24-25.jsonl"

KPI_JSONL = "/kaggle/input/datasets/vaibhavmeena23/prd-brsr-24-25-jsonl/esg_kpis_24-25.jsonl"

INTENT_JSONL = "/kaggle/input/datasets/vaibhavmeena23/prd-brsr-24-25-jsonl/esg_paragraph_intents_24-25.jsonl"

OUTPUT_DIR = "/kaggle/working/esg_prd_master_24-25"

OUTPUT_JSONL = f"{OUTPUT_DIR}/esg_prd_master_dataset.jsonl"
OUTPUT_CSV = f"{OUTPUT_DIR}/esg_prd_master_dataset.csv"
OUTPUT_AUDIT_CSV = f"{OUTPUT_DIR}/esg_prd_master_merge_audit.csv"

os.makedirs(OUTPUT_DIR, exist_ok=True)


# If your dataset has one row per company per year, keep this True.
# If company names repeat across years, this prevents wrong merging.
USE_YEAR_IN_KEY = True

# For LLM training, paragraph intent labels are useful.
# But storing all paragraphs can make JSONL very large.
INCLUDE_ALL_INTENT_PARAGRAPHS = True

# If INCLUDE_ALL_INTENT_PARAGRAPHS = False, only top examples are saved.
MAX_INTENT_EXAMPLES_PER_LABEL = 5

# Use this if you want a compact training-ready text field.
CREATE_LLM_TRAINING_TEXT = True


# =============================================================================
# JSONL HELPERS
# =============================================================================

def load_jsonl(path):
    rows = []

    if not path or not os.path.exists(path):
        print(f"WARNING: file not found: {path}")
        return rows

    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()

            if not line:
                continue

            try:
                rows.append(json.loads(line))
            except Exception as e:
                print(f"Skipping bad JSON line {line_no} in {path}: {e}")

    return rows


def safe_json(obj):
    return json.dumps(obj, ensure_ascii=False)


def normalize_key_part(x):
    if x is None:
        return ""

    x = str(x).strip().lower()
    x = re.sub(r"\.pdf$", "", x)
    x = re.sub(r"[^a-z0-9]+", " ", x)
    x = re.sub(r"\s+", " ", x).strip()

    return x


def clean_year(y):
    if y is None:
        return ""

    s = str(y)

    m = re.search(r"\b(20\d{2})\b", s)

    if m:
        return m.group(1)

    return normalize_key_part(s)


def get_nested(obj, *keys):
    cur = obj

    for key in keys:
        if not isinstance(cur, dict):
            return None

        cur = cur.get(key)

    return cur


def get_best_identity_fields(row):
    flat = row.get("flat_kpis") if isinstance(row.get("flat_kpis"), dict) else {}
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}

    company = (
        row.get("company")
        or flat.get("company")
        or metadata.get("company")
        or row.get("company_name")
        or metadata.get("company_name")
        or row.get("name")
    )

    filename = (
        row.get("filename")
        or flat.get("filename")
        or metadata.get("filename")
    )

    file = (
        row.get("file")
        or flat.get("file")
        or metadata.get("file")
    )

    reporting_year = (
        row.get("reporting_year")
        or flat.get("reporting_year")
        or metadata.get("reporting_year")
        or row.get("year")
        or metadata.get("year")
        or row.get("financial_year")
        or metadata.get("financial_year")
    )

    company_id = (
        row.get("company_id")
        or metadata.get("company_id")
        or row.get("cin")
        or metadata.get("cin")
        or row.get("isin")
        or metadata.get("isin")
        or row.get("nse_symbol")
        or metadata.get("nse_symbol")
        or row.get("bse_code")
        or metadata.get("bse_code")
    )

    # Important for your metadata JSONL
    metadata_report_key = row.get("company") if row.get("source_folder") else metadata.get("metadata_report_key")

    return {
        "company": company,
        "filename": filename,
        "file": file,
        "reporting_year": reporting_year,
        "company_id": company_id,
        "metadata_report_key": metadata_report_key,
    }

def make_merge_key(row):
    ids = get_best_identity_fields(row)

    # Metadata file: company is actually report/file stem
    if ids.get("metadata_report_key"):
        base = normalize_key_part(ids["metadata_report_key"])

    elif ids.get("filename"):
        base = normalize_key_part(ids["filename"])

    elif ids.get("file"):
        base = normalize_key_part(Path(str(ids["file"])).stem)

    elif ids.get("company"):
        base = normalize_key_part(ids["company"])

    elif ids.get("company_id"):
        base = normalize_key_part(ids["company_id"])

    else:
        base = ""

    return base

def flatten_dict(d, prefix="", max_depth=3, current_depth=0):
    """
    Flattens nested dict for CSV output only.
    JSONL keeps nested structures.
    """

    out = {}

    if not isinstance(d, dict):
        return out

    for k, v in d.items():
        key = f"{prefix}_{k}" if prefix else str(k)
        key = re.sub(r"[^a-zA-Z0-9_]+", "_", key)

        if isinstance(v, dict) and current_depth < max_depth:
            out.update(flatten_dict(v, key, max_depth, current_depth + 1))
        elif isinstance(v, list):
            out[key] = safe_json(v)
        else:
            out[key] = v

    return out


# =============================================================================
# LOAD INPUT FILES
# =============================================================================

metadata_rows = load_jsonl(METADATA_JSONL)
section_rows = load_jsonl(SECTION_JSONL)
kpi_rows = load_jsonl(KPI_JSONL)
intent_rows = load_jsonl(INTENT_JSONL)

print("Metadata rows:", len(metadata_rows))
print("Section rows :", len(section_rows))
print("KPI rows     :", len(kpi_rows))
print("Intent rows  :", len(intent_rows))


# =============================================================================
# BUILD INDEXES
# =============================================================================

def index_one_per_company(rows, source_name):
    index = {}
    duplicates = defaultdict(list)

    for row in rows:
        key = make_merge_key(row)

        if not key:
            continue

        if key in index:
            duplicates[key].append(row)
        else:
            index[key] = row

    print(f"{source_name} indexed:", len(index))
    print(f"{source_name} duplicate keys:", len(duplicates))

    return index, duplicates


metadata_index, metadata_dups = index_one_per_company(metadata_rows, "metadata")
section_index, section_dups = index_one_per_company(section_rows, "section")
kpi_index, kpi_dups = index_one_per_company(kpi_rows, "kpi")


# Intent JSONL has many rows per company, so group it.
intent_group = defaultdict(list)

for row in intent_rows:
    key = make_merge_key(row)

    if key:
        intent_group[key].append(row)

print("intent companies indexed:", len(intent_group))


# Master keys from all sources
all_keys = set()
all_keys.update(metadata_index.keys())
all_keys.update(section_index.keys())
all_keys.update(kpi_index.keys())
all_keys.update(intent_group.keys())

print("Total master keys:", len(all_keys))


# =============================================================================
# INTENT AGGREGATION
# =============================================================================

def aggregate_intents(rows):
    if not rows:
        return {
            "paragraph_count": 0,
            "intent_counts": {},
            "intent_percentages": {},
            "section_label_counts": {},
            "top_intents": [],
            "intent_examples": {},
            "paragraph_intents": [] if INCLUDE_ALL_INTENT_PARAGRAPHS else None,
        }

    intent_counter = Counter()
    section_counter = Counter()
    examples = defaultdict(list)

    paragraph_intents = []

    for row in rows:
        primary_intent = row.get("primary_intent") or "unclassified"
        intent_counter[primary_intent] += 1

        section_labels = row.get("section_labels") or ""

        for label in str(section_labels).split(","):
            label = label.strip()

            if label:
                section_counter[label] += 1

        para_text = row.get("paragraph_text") or row.get("text") or ""

        compact_row = {
            "paragraph_id": row.get("paragraph_id"),
            "page": row.get("page"),
            "primary_intent": primary_intent,
            "primary_intent_score": row.get("primary_intent_score"),
            "section_labels": section_labels,
            "paragraph_text": para_text,
        }

        if INCLUDE_ALL_INTENT_PARAGRAPHS:
            paragraph_intents.append(compact_row)

        if len(examples[primary_intent]) < MAX_INTENT_EXAMPLES_PER_LABEL:
            examples[primary_intent].append(compact_row)

    total = sum(intent_counter.values())

    percentages = {
        k: round(v / total * 100, 4)
        for k, v in intent_counter.items()
    } if total else {}

    top_intents = [
        {
            "intent": k,
            "count": v,
            "percentage": percentages.get(k),
        }
        for k, v in intent_counter.most_common()
    ]

    return {
        "paragraph_count": total,
        "intent_counts": dict(intent_counter),
        "intent_percentages": percentages,
        "section_label_counts": dict(section_counter),
        "top_intents": top_intents,
        "intent_examples": dict(examples),
        "paragraph_intents": paragraph_intents if INCLUDE_ALL_INTENT_PARAGRAPHS else None,
    }


# =============================================================================
# KPI STRUCTURE HELPERS
# =============================================================================

def extract_kpi_struct(kpi_row):
    if not kpi_row:
        return {
            "flat_kpis": {},
            "nested_kpis": {},
        }

    flat = kpi_row.get("flat_kpis")

    if not isinstance(flat, dict):
        flat = flatten_dict(kpi_row.get("kpis", {}), prefix="kpi")

    nested = kpi_row.get("kpis")

    if not isinstance(nested, dict):
        nested = {}

    return {
        "flat_kpis": flat,
        "nested_kpis": nested,
        "kpi_error": kpi_row.get("error"),
    }


def extract_section_struct(section_row):
    if not section_row:
        return {
            "section_presence": {},
            "section_scores": {},
            "top_sections": [],
            "section_evidence": {},
        }

    return {
        "section_presence": section_row.get("section_presence") or {},
        "section_scores": section_row.get("section_scores") or {},
        "top_sections": section_row.get("top_sections") or [],
        "section_evidence": section_row.get("section_evidence") or {},
        "section_error": section_row.get("error"),
    }


def clean_metadata(metadata_row):
    if not metadata_row:
        return {}

    # Keep full metadata, but remove very large raw text fields if accidentally present.
    cleaned = dict(metadata_row)

    for heavy_col in [
        "esg_text",
        "raw_text",
        "full_text",
        "text",
        "paragraphs",
        "page_texts",
    ]:
        if heavy_col in cleaned:
            cleaned.pop(heavy_col)

    return cleaned


def create_llm_training_text(master_record):
    """
    Creates one compact training target field.
    You can later convert this to instruction-tuning format.
    """

    company = master_record.get("company")
    year = master_record.get("reporting_year")

    metadata = master_record.get("metadata") or {}
    sections = master_record.get("esg_classification") or {}
    flat_kpis = master_record.get("kpi_extraction", {}).get("flat_kpis") or {}
    intent_summary = master_record.get("intent_labelling", {}).get("intent_summary") or {}

    lines = []

    lines.append(f"Company: {company}")
    lines.append(f"Reporting year: {year}")
    lines.append("")

    if metadata:
        sector = metadata.get("sector") or metadata.get("industry")
        market_cap = metadata.get("market_cap")

        if sector:
            lines.append(f"Sector: {sector}")

        if market_cap:
            lines.append(f"Market cap: {market_cap}")

    top_sections = sections.get("top_sections") or []

    if top_sections:
        lines.append("ESG sections present: " + ", ".join(top_sections))

    key_kpis = {
        "Scope 1 emissions tCO2e": flat_kpis.get("scope1_emissions_tco2e_current"),
        "Scope 2 emissions tCO2e": flat_kpis.get("scope2_emissions_tco2e_current"),
        "Scope 3 emissions tCO2e": flat_kpis.get("scope3_emissions_tco2e_current"),
        "Renewable energy percent": flat_kpis.get("renewable_energy_percent"),
        "Water consumption KL": flat_kpis.get("water_consumption_kl_current"),
        "Waste recycled": flat_kpis.get("waste_recycled_current"),
        "Total waste generated": flat_kpis.get("total_waste_generated_current"),
        "Female employee percent": flat_kpis.get("female_employee_percent"),
        "Energy intensity": flat_kpis.get("energy_intensity_current"),
        "Net zero target year": flat_kpis.get("net_zero_target_year"),
    }

    lines.append("")
    lines.append("Extracted ESG KPIs:")

    for k, v in key_kpis.items():
        if v is not None and str(v).lower() != "nan":
            lines.append(f"- {k}: {v}")

    top_intents = intent_summary.get("top_intents") or []

    if top_intents:
        lines.append("")
        lines.append("Narrative intent distribution:")

        for item in top_intents[:8]:
            lines.append(
                f"- {item.get('intent')}: {item.get('count')} paragraphs "
                f"({item.get('percentage')}%)"
            )

    return "\n".join(lines)


# =============================================================================
# MERGE INTO ONE STRUCTURED ROW PER COMPANY
# =============================================================================

master_records = []
audit_rows = []

for key in tqdm(sorted(all_keys), desc="Merging PRD dataset"):
    metadata_row = metadata_index.get(key)
    section_row = section_index.get(key)
    kpi_row = kpi_index.get(key)
    intent_rows_for_key = intent_group.get(key, [])

    ids = (
        get_best_identity_fields(metadata_row or {})
        if metadata_row
        else get_best_identity_fields(section_row or kpi_row or (intent_rows_for_key[0] if intent_rows_for_key else {}))
    )

    company = ids.get("company")
    filename = ids.get("filename")
    file = ids.get("file")
    reporting_year = ids.get("reporting_year")
    company_id = ids.get("company_id")

    # Fill identity from other sources if missing.
    for candidate in [section_row, kpi_row, intent_rows_for_key[0] if intent_rows_for_key else None]:
        if not candidate:
            continue

        cids = get_best_identity_fields(candidate)

        company = company or cids.get("company")
        filename = filename or cids.get("filename")
        file = file or cids.get("file")
        reporting_year = reporting_year or cids.get("reporting_year")
        company_id = company_id or cids.get("company_id")

    section_struct = extract_section_struct(section_row)
    kpi_struct = extract_kpi_struct(kpi_row)
    intent_summary = aggregate_intents(intent_rows_for_key)

    record = {
        "merge_key": key,

        "company_id": company_id,
        "company": company,
        "filename": filename,
        "file": file,
        "reporting_year": reporting_year,

        "metadata": clean_metadata(metadata_row),

        "esg_classification": section_struct,

        "kpi_extraction": kpi_struct,

        "intent_labelling": {
            "intent_summary": {
                "paragraph_count": intent_summary["paragraph_count"],
                "intent_counts": intent_summary["intent_counts"],
                "intent_percentages": intent_summary["intent_percentages"],
                "section_label_counts": intent_summary["section_label_counts"],
                "top_intents": intent_summary["top_intents"],
                "intent_examples": intent_summary["intent_examples"],
            },
            "paragraph_intents": intent_summary["paragraph_intents"],
        },

        "source_presence": {
            "has_metadata": metadata_row is not None,
            "has_esg_classification": section_row is not None,
            "has_kpi_extraction": kpi_row is not None,
            "has_intent_labelling": len(intent_rows_for_key) > 0,
        },
    }

    if CREATE_LLM_TRAINING_TEXT:
        record["llm_training_summary"] = create_llm_training_text(record)

    master_records.append(record)

    audit_rows.append({
        "merge_key": key,
        "company": company,
        "filename": filename,
        "reporting_year": reporting_year,
        "has_metadata": metadata_row is not None,
        "has_esg_classification": section_row is not None,
        "has_kpi_extraction": kpi_row is not None,
        "has_intent_labelling": len(intent_rows_for_key) > 0,
        "intent_paragraph_count": intent_summary["paragraph_count"],
        "section_count": len(section_struct.get("top_sections") or []),
        "kpi_field_count": sum(
            1 for v in (kpi_struct.get("flat_kpis") or {}).values()
            if v is not None and str(v).lower() not in ["", "nan", "none"]
        ),
    })


# =============================================================================
# SAVE STRUCTURED JSONL
# =============================================================================

with open(OUTPUT_JSONL, "w", encoding="utf-8") as f:
    for rec in master_records:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

print("Saved JSONL:", OUTPUT_JSONL)


# =============================================================================
# SAVE FLAT CSV FOR CHECKING
# =============================================================================

flat_rows = []

for rec in master_records:
    row = {
        "merge_key": rec.get("merge_key"),
        "company_id": rec.get("company_id"),
        "company": rec.get("company"),
        "filename": rec.get("filename"),
        "file": rec.get("file"),
        "reporting_year": rec.get("reporting_year"),
    }

    row.update(flatten_dict(rec.get("metadata") or {}, prefix="meta", max_depth=2))

    section = rec.get("esg_classification") or {}
    row["top_sections"] = ", ".join(section.get("top_sections") or [])

    for sec, val in (section.get("section_presence") or {}).items():
        row[f"has_{normalize_key_part(sec).replace(' ', '_')}"] = val

    for sec, val in (section.get("section_scores") or {}).items():
        row[f"score_{normalize_key_part(sec).replace(' ', '_')}"] = val

    flat_kpis = get_nested(rec, "kpi_extraction", "flat_kpis") or {}

    for k, v in flat_kpis.items():
        if isinstance(v, (dict, list)):
            row[f"kpi_{k}"] = safe_json(v)
        else:
            row[f"kpi_{k}"] = v

    intent_summary = get_nested(rec, "intent_labelling", "intent_summary") or {}

    row["intent_paragraph_count"] = intent_summary.get("paragraph_count")
    row["intent_counts_json"] = safe_json(intent_summary.get("intent_counts") or {})
    row["intent_percentages_json"] = safe_json(intent_summary.get("intent_percentages") or {})

    for intent, count in (intent_summary.get("intent_counts") or {}).items():
        col = f"intent_count_{normalize_key_part(intent).replace(' ', '_')}"
        row[col] = count

    row.update(rec.get("source_presence") or {})

    if CREATE_LLM_TRAINING_TEXT:
        row["llm_training_summary"] = rec.get("llm_training_summary")

    flat_rows.append(row)

flat_df = pd.DataFrame(flat_rows)
flat_df.to_csv(OUTPUT_CSV, index=False)

audit_df = pd.DataFrame(audit_rows)
audit_df.to_csv(OUTPUT_AUDIT_CSV, index=False)

print("Saved CSV   :", OUTPUT_CSV)
print("Saved audit :", OUTPUT_AUDIT_CSV)

print("\nMaster records:", len(master_records))
print("Rows with metadata      :", audit_df["has_metadata"].sum())
print("Rows with classification:", audit_df["has_esg_classification"].sum())
print("Rows with KPI extraction:", audit_df["has_kpi_extraction"].sum())
print("Rows with intent labels :", audit_df["has_intent_labelling"].sum())

display(audit_df.head(20))
display(flat_df.head(10))