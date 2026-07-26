# FAST COMPANY-LEVEL ESG REPORT GENERATION USING PLACEHOLDER LoRA ADAPTER
# Hybrid SusGen-style:
#   LoRA generates section templates with placeholders
#   Python validates placeholders
#   Python fills exact KPI values
#   Python merges sections into full ESG report
#
# Speed improvements:
#   1. Batched LoRA inference
#   2. Optional skip LoRA for missing-data/disclosure sections
#   3. Optional template cache
#   4. Numeric leaks treated as audit warning, not hard invalid
#   5. Duplicate sentence cleanup
# =============================================================================

import os
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"
import re
import json
import time
import torch
import pandas as pd
from tqdm.auto import tqdm
from collections import defaultdict

from unsloth import FastLanguageModel
from unsloth.chat_templates import get_chat_template


# =============================================================================
# CONFIG
# =============================================================================

LORA_DIR = "/kaggle/input/datasets/vaibhavmeena23/final-lora-v3"

# For final generation, use all JSONL, not valid JSONL.
# For testing, you can use validation JSONL.
INPUT_SECTION_JSONL = "/kaggle/input/datasets/vaibhavmeena23/final-llama-placeholder/llama_esg_placeholder_all_24-25 (1).jsonl"
# INPUT_SECTION_JSONL = "/kaggle/working/llama_esg_placeholder_sft_v1/llama_esg_placeholder_valid.jsonl"

OUTPUT_DIR = "/kaggle/working/company_level_placeholder_esg_reports_fast"
os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT_JSONL = f"{OUTPUT_DIR}/company_level_esg_reports.jsonl"
OUTPUT_CSV = f"{OUTPUT_DIR}/company_level_esg_reports_audit.csv"
BAD_SECTIONS_JSONL = f"{OUTPUT_DIR}/bad_sections_fallback_used.jsonl"
SAMPLE_REPORT_TXT = f"{OUTPUT_DIR}/sample_company_report.txt"

MAX_SEQ_LENGTH = 4096
MAX_NEW_TOKENS = 500

# Batch generation speedup.
# If CUDA OOM occurs, reduce to 2.
BATCH_GENERATION_SIZE = 2

# For quick test, use 20 or 100.
# For final all-company generation, set None.
MAX_COMPANIES = 20
# MAX_COMPANIES = None

PREFERRED_COMPANIES = []

USE_FALLBACK_ON_INVALID = True

# These improve speed without hurting final safety.
SKIP_LORA_FOR_MISSING_DATA = True
SKIP_LORA_FOR_DISCLOSURE_LIMITATIONS = True

# Keep False for closest output to your current version.
# Set True only if you want extra speed and accept repeated templates for same placeholder pattern.
USE_TEMPLATE_CACHE = False
TEMPLATE_CACHE = {}

STORE_FULL_SECTION_RECORDS = True

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

start_time = time.time()

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

tokenizer.padding_side = "left"
tokenizer.pad_token = tokenizer.eos_token

FastLanguageModel.for_inference(model)

try:
    torch.backends.cuda.matmul.allow_tf32 = True
except Exception:
    pass

print("Loaded LoRA:", LORA_DIR)


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


def find_possible_numeric_leaks(text):
    clean = re.sub(r"\{\{[A-Za-z0-9_]+\}\}", "", text or "")
    nums = re.findall(r"\b\d[\d,]*(?:\.\d+)?\b", clean)

    suspicious = []
    for n in nums:
        # Allow Scope 1 / Scope 2 / Scope 3 references.
        if n in {"1", "2", "3"}:
            continue
        suspicious.append(n)

    return suspicious


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
    """
    Fix common placeholder spelling variants only if the corrected placeholder
    is allowed for the current sample.
    """
    out = text or ""
    allowed = set(placeholder_values.keys())

    for bad, good in PLACEHOLDER_ALIASES.items():
        if bad in out and good in allowed:
            out = out.replace(bad, good)

    return out


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

    # IMPORTANT:
    # Numeric leaks are audit warnings only, not hard invalids.
    # This reduces unnecessary fallback sections.
    # if numeric_leaks:
    #     problems.append("numeric_leak")

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
# LOAD PLACEHOLDER SECTION ROWS
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


def choose_companies(groups):
    items = list(groups.items())

    if PREFERRED_COMPANIES:
        preferred = []
        preferred_set = {x.lower().strip() for x in PREFERRED_COMPANIES}

        for key, rows in items:
            company = str(rows[0].get("company") or "").lower().strip()

            if company in preferred_set:
                preferred.append((key, rows))

        return preferred

    def score_group(kv):
        key, rows = kv

        kpi_grounded_count = sum(
            1 for r in rows
            if r.get("target_source") == "placeholder_kpi_grounded_target"
        )

        missing_count = sum(
            1 for r in rows
            if r.get("target_source") == "placeholder_missing_data"
        )

        total_sections = len({r.get("section") for r in rows})

        return (
            kpi_grounded_count,
            total_sections,
            -missing_count,
        )

    items = sorted(items, key=score_group, reverse=True)

    if MAX_COMPANIES is not None:
        items = items[:MAX_COMPANIES]

    return items


# =============================================================================
# BATCH GENERATION HELPERS
# =============================================================================

def should_skip_lora(row):
    target_source = row.get("target_source")
    section = row.get("section")

    if SKIP_LORA_FOR_MISSING_DATA and target_source == "placeholder_missing_data":
        return True

    if SKIP_LORA_FOR_DISCLOSURE_LIMITATIONS and (
        target_source == "placeholder_disclosure_limitations"
        or section == "disclosure_limitations"
    ):
        return True

    return False


def make_template_cache_key(row):
    section = row.get("section")
    target_source = row.get("target_source")
    placeholder_keys = tuple(sorted((row.get("placeholder_values") or {}).keys()))

    return (
        section,
        target_source,
        placeholder_keys,
    )


def make_prompt_from_sample(sample):
    messages = sample["messages"][:2]

    return tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True,
    )


def generate_templates_batch(samples, batch_size=4):
    """
    Batch LoRA generation. Returns templates in the same order as samples.
    """
    outputs_all = []

    for start in tqdm(range(0, len(samples), batch_size), desc="Batch LoRA generation"):
        batch_samples = samples[start:start + batch_size]
        prompts = [make_prompt_from_sample(s) for s in batch_samples]

        try:
            inputs = tokenizer(
                prompts,
                return_tensors="pt",
                truncation=True,
                max_length=MAX_SEQ_LENGTH,
                padding=True,
                pad_to_multiple_of=8,
            ).to("cuda")

            prompt_len = inputs["input_ids"].shape[1]

            with torch.inference_mode():
                outputs = model.generate(
                    **inputs,
                    max_new_tokens=MAX_NEW_TOKENS,
                    do_sample=False,
                    repetition_penalty=1.0,
                    eos_token_id=tokenizer.eos_token_id,
                    pad_token_id=tokenizer.eos_token_id,
                )

            for i in range(len(batch_samples)):
                completion_ids = outputs[i][prompt_len:]
                completion = tokenizer.decode(
                    completion_ids,
                    skip_special_tokens=True,
                ).strip()
                outputs_all.append(completion)

            del inputs, outputs
            torch.cuda.empty_cache()

        except RuntimeError as e:
            # Fallback for CUDA OOM: generate samples one by one.
            if "out of memory" not in str(e).lower():
                raise e

            print("\nCUDA OOM in batch. Falling back to single-sample generation for this batch.")
            torch.cuda.empty_cache()

            for sample in batch_samples:
                prompt = make_prompt_from_sample(sample)

                inputs = tokenizer(
                    prompt,
                    return_tensors="pt",
                    truncation=True,
                    max_length=MAX_SEQ_LENGTH,
                ).to("cuda")

                prompt_len = inputs["input_ids"].shape[1]

                with torch.inference_mode():
                    outputs = model.generate(
                        **inputs,
                        max_new_tokens=MAX_NEW_TOKENS,
                        do_sample=False,
                        repetition_penalty=1.0,
                        eos_token_id=tokenizer.eos_token_id,
                        pad_token_id=tokenizer.eos_token_id,
                    )

                completion_ids = outputs[0][prompt_len:]
                completion = tokenizer.decode(
                    completion_ids,
                    skip_special_tokens=True,
                ).strip()

                outputs_all.append(completion)

                del inputs, outputs
                torch.cuda.empty_cache()

    return outputs_all


# =============================================================================
# LOAD + SELECT COMPANIES
# =============================================================================

all_rows = load_section_rows(INPUT_SECTION_JSONL)
groups = group_rows_by_company(all_rows)
selected_groups = choose_companies(groups)

print("Total section rows:", len(all_rows))
print("Total company/report groups:", len(groups))
print("Selected company/report groups:", len(selected_groups))

if len(selected_groups) == 0:
    raise ValueError("No company groups selected. Check INPUT_SECTION_JSONL or PREFERRED_COMPANIES.")


# =============================================================================
# PRE-GENERATE LORA TEMPLATES IN BATCHES
# =============================================================================

PREGENERATED_TEMPLATES = {}
PREGENERATED_META = {}

rows_to_generate = []
row_keys_to_generate = []

for group_key, rows in selected_groups:
    rows = dedupe_sections(rows)
    rows = sorted(rows, key=section_sort_key)

    for row in rows:
        sec = row.get("section")
        reference_template = row["messages"][2]["content"]
        placeholder_values = row.get("placeholder_values", {})

        key = (group_key, sec)

        if should_skip_lora(row):
            PREGENERATED_TEMPLATES[key] = reference_template
            PREGENERATED_META[key] = {
                "skipped_lora": True,
                "used_cache": False,
            }
            continue

        cache_key = make_template_cache_key(row)

        if USE_TEMPLATE_CACHE and cache_key in TEMPLATE_CACHE:
            PREGENERATED_TEMPLATES[key] = TEMPLATE_CACHE[cache_key]
            PREGENERATED_META[key] = {
                "skipped_lora": False,
                "used_cache": True,
            }
            continue

        rows_to_generate.append(row)
        row_keys_to_generate.append((group_key, sec, cache_key))

print("Rows needing LoRA generation:", len(rows_to_generate))
print("Rows skipped LoRA:", sum(1 for v in PREGENERATED_META.values() if v["skipped_lora"]))
print("Rows from cache:", sum(1 for v in PREGENERATED_META.values() if v["used_cache"]))

batch_start_time = time.time()

generated_templates = generate_templates_batch(
    rows_to_generate,
    batch_size=BATCH_GENERATION_SIZE,
)

for item, template, row in zip(row_keys_to_generate, generated_templates, rows_to_generate):
    group_key, sec, cache_key = item

    template = normalize_generated_placeholders(
        template,
        row.get("placeholder_values", {}),
    )

    PREGENERATED_TEMPLATES[(group_key, sec)] = template
    PREGENERATED_META[(group_key, sec)] = {
        "skipped_lora": False,
        "used_cache": False,
    }

    if USE_TEMPLATE_CACHE:
        TEMPLATE_CACHE[cache_key] = template

print("Pregenerated templates:", len(PREGENERATED_TEMPLATES))
print("Batch generation time minutes:", round((time.time() - batch_start_time) / 60, 2))


# =============================================================================
# REPORT MERGE HELPERS
# =============================================================================

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


def validate_reference_template(reference_template, placeholder_values):
    filled_output = fill_placeholders(reference_template, placeholder_values)

    return {
        "ok": True,
        "problems": [],
        "allowed_placeholders": sorted(set(placeholder_values.keys())),
        "used_placeholders": extract_placeholders(reference_template),
        "invalid_placeholders": [],
        "unused_allowed_placeholders": [],
        "bad_single_brace": [],
        "malformed_fragments": [],
        "unfilled_after_replacement": extract_placeholders(filled_output),
        "numeric_leaks": [],
        "filled_output": filled_output,
    }


def build_company_report(group_key, rows):
    rows = dedupe_sections(rows)
    rows = sorted(rows, key=section_sort_key)

    company = rows[0].get("company") if rows else "Unknown"
    nse_symbol = rows[0].get("nse_symbol") if rows else None
    reporting_year = rows[0].get("reporting_year") if rows else None

    section_outputs = {}
    section_records = []
    bad_sections = []

    for row in rows:
        sec = row.get("section")
        target_source = row.get("target_source")
        placeholder_values = row.get("placeholder_values", {})
        reference_template = row["messages"][2]["content"]

        meta = PREGENERATED_META.get(
            (group_key, sec),
            {"skipped_lora": False, "used_cache": False},
        )

        skipped_lora = meta.get("skipped_lora", False)
        used_cache = meta.get("used_cache", False)

        generated_template = PREGENERATED_TEMPLATES.get((group_key, sec))

        if generated_template is None:
            # Emergency fallback: should rarely happen.
            generated_template = reference_template
            skipped_lora = True

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
                final_section_text = fill_placeholders(
                    reference_template,
                    placeholder_values,
                )
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
            "used_cache": used_cache,
            "skipped_lora": skipped_lora,
            "problems": validation["problems"],
            "invalid_placeholders": validation["invalid_placeholders"],
            "malformed_fragments": validation["malformed_fragments"],
            "bad_single_brace": validation["bad_single_brace"],
            "unfilled_after_replacement": validation["unfilled_after_replacement"],
            "numeric_leaks": validation["numeric_leaks"],
            "allowed_placeholders": validation["allowed_placeholders"],
            "used_placeholders": validation["used_placeholders"],
            "reference_template": reference_template if STORE_FULL_SECTION_RECORDS else "",
            "generated_template": generated_template if STORE_FULL_SECTION_RECORDS else "",
            "final_template": final_template if STORE_FULL_SECTION_RECORDS else "",
            "final_section_text": final_section_text if STORE_FULL_SECTION_RECORDS else "",
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
        "section_records": section_records if STORE_FULL_SECTION_RECORDS else [],
        "num_invalid_sections": sum(1 for s in section_records if not s["ok"]),
        "num_fallback_sections": sum(1 for s in section_records if s["used_fallback"]),
        "num_cached_sections": sum(1 for s in section_records if s["used_cache"]),
        "num_skipped_lora_sections": sum(1 for s in section_records if s["skipped_lora"]),
        "final_report": final_report,
        "final_report_chars": len(final_report),
    }

    return company_record, bad_sections


# =============================================================================
# RUN COMPANY-LEVEL GENERATION
# =============================================================================

company_records = []
all_bad_sections = []
audit_rows = []

with open(OUTPUT_JSONL, "w", encoding="utf-8") as out_f, \
     open(BAD_SECTIONS_JSONL, "w", encoding="utf-8") as bad_f:

    for group_key, rows in tqdm(selected_groups, desc="Building company reports"):
        record, bad_sections = build_company_report(group_key, rows)

        company_records.append(record)

        out_f.write(json.dumps(record, ensure_ascii=False) + "\n")

        for bad in bad_sections:
            bad_record = {
                "source_key": group_key,
                "company": record["company"],
                "section": bad["section"],
                "target_source": bad["target_source"],
                "problems": bad["problems"],
                "generated_template": bad["generated_template"],
                "final_template": bad["final_template"],
                "used_fallback": bad["used_fallback"],
                "used_cache": bad["used_cache"],
                "skipped_lora": bad["skipped_lora"],
                "numeric_leaks": bad["numeric_leaks"],
            }
            bad_f.write(json.dumps(bad_record, ensure_ascii=False) + "\n")
            all_bad_sections.append(bad_record)

        audit_rows.append({
            "source_key": record["source_key"],
            "company": record["company"],
            "nse_symbol": record["nse_symbol"],
            "reporting_year": record["reporting_year"],
            "num_sections": record["num_sections"],
            "sections": ", ".join(record["sections"]),
            "num_invalid_sections": record["num_invalid_sections"],
            "num_fallback_sections": record["num_fallback_sections"],
            "num_cached_sections": record["num_cached_sections"],
            "num_skipped_lora_sections": record["num_skipped_lora_sections"],
            "final_report_chars": record["final_report_chars"],
        })

audit_df = pd.DataFrame(audit_rows)
audit_df.to_csv(OUTPUT_CSV, index=False)

if company_records:
    with open(SAMPLE_REPORT_TXT, "w", encoding="utf-8") as f:
        f.write(company_records[0]["final_report"])


# =============================================================================
# SUMMARY
# =============================================================================

total_time = time.time() - start_time

print("\nDONE")
print("Company reports generated:", len(company_records))
print("Total bad/invalid sections:", len(all_bad_sections))
print("Saved reports JSONL:", OUTPUT_JSONL)
print("Saved audit CSV:", OUTPUT_CSV)
print("Saved bad sections JSONL:", BAD_SECTIONS_JSONL)
print("Saved sample report:", SAMPLE_REPORT_TXT)
print("Total runtime minutes:", round(total_time / 60, 2))

print("\nAudit:")
display(audit_df)

print("\nFallback section count:")
print(int(audit_df["num_fallback_sections"].sum()))

print("\nInvalid section count:")
print(int(audit_df["num_invalid_sections"].sum()))

print("\nSkipped LoRA section count:")
print(int(audit_df["num_skipped_lora_sections"].sum()))

print("\nCached section count:")
print(int(audit_df["num_cached_sections"].sum()))

if len(all_bad_sections):
    bad_df = pd.DataFrame(all_bad_sections)
    print("\nBad sections preview:")
    display(bad_df.head(30))

    print("\nBad sections by section:")
    display(bad_df["section"].value_counts())
else:
    print("\nNo invalid sections found.")


# =============================================================================
# PRINT FIRST REPORT PREVIEW
# =============================================================================

if company_records:
    print("\n" + "=" * 120)
    print("SAMPLE COMPANY REPORT")
    print("=" * 120)
    print("COMPANY:", company_records[0]["company"])
    print("SECTIONS:", company_records[0]["sections"])
    print("\n")
    print(company_records[0]["final_report"][:8000])