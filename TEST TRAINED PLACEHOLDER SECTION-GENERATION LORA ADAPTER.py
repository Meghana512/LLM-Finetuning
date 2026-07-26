# TEST TRAINED PLACEHOLDER SECTION-GENERATION LORA ADAPTER
# =============================================================================

import json
import re
import torch
from unsloth import FastLanguageModel
from unsloth.chat_templates import get_chat_template

LORA_DIR = "/kaggle/working/llama_esg_placeholder_lora_v2/final_lora"
VALID_JSONL = "/kaggle/input/datasets/vaibhavmeena23/final-llama-placeholder/llama_esg_placeholder_valid_24-25 (1).jsonl"

MAX_SEQ_LENGTH = 4096

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


# =============================================================================
# HELPERS
# =============================================================================

def extract_placeholders(text):
    return sorted(set(re.findall(r"\{\{[A-Za-z0-9_]+\}\}", text or "")))


def find_bad_single_brace_placeholders(text):
    return re.findall(r"(?<!\{)\{[A-Za-z0-9_]+\}(?!\})", text or "")


def fill_placeholders(text, placeholder_values):
    out = text

    for ph, value in sorted(placeholder_values.items(), key=lambda x: len(x[0]), reverse=True):
        out = out.replace(ph, str(value))

    return out


# =============================================================================
# LOAD ONLY PLACEHOLDER SECTION_GENERATION SAMPLE
# =============================================================================

def load_section_generation_sample(
    path,
    preferred_section="ghg_emissions",
    preferred_target_source="placeholder_kpi_grounded_target",
):
    fallback = None

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

            if fallback is None:
                fallback = obj

            if (
                obj.get("section") == preferred_section
                and obj.get("target_source") == preferred_target_source
            ):
                return obj

    return fallback


sample = load_section_generation_sample(
    VALID_JSONL,
    preferred_section="ghg_emissions",
    preferred_target_source="placeholder_kpi_grounded_target",
)

if sample is None:
    raise ValueError("No section_generation_placeholder sample found in validation file.")

print("TASK:", sample.get("task"))
print("SECTION:", sample.get("section"))
print("SECTION TITLE:", sample.get("section_title"))
print("COMPANY:", sample.get("company"))
print("TARGET SOURCE:", sample.get("target_source"))

messages = sample["messages"][:2]  # system + user only


# =============================================================================
# GENERATE PLACEHOLDER TEMPLATE
# =============================================================================

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

outputs = model.generate(
    **inputs,
    max_new_tokens=500,
    do_sample=False,
    repetition_penalty=1.0,
    eos_token_id=tokenizer.eos_token_id,
    pad_token_id=tokenizer.eos_token_id,
)

completion_ids = outputs[0][inputs["input_ids"].shape[-1]:]
generated_template = tokenizer.decode(completion_ids, skip_special_tokens=True).strip()


# =============================================================================
# VALIDATE PLACEHOLDER OUTPUT
# =============================================================================

placeholder_values = sample.get("placeholder_values", {})

allowed_placeholders = set(placeholder_values.keys())
used_placeholders = set(extract_placeholders(generated_template))

invalid_placeholders = sorted(used_placeholders - allowed_placeholders)
unused_allowed_placeholders = sorted(allowed_placeholders - used_placeholders)
bad_single_brace = find_bad_single_brace_placeholders(generated_template)

filled_output = fill_placeholders(
    generated_template,
    placeholder_values,
)

unfilled_after_replacement = extract_placeholders(filled_output)


def find_malformed_placeholder_fragments(text):
    """
    Detects truly malformed placeholders.
    Does NOT falsely flag valid placeholders like {{company}}.
    """
    if not isinstance(text, str):
        return ["non_string_output"]

    problems = []

    valid_placeholder_pattern = r"\{\{[A-Za-z0-9_]+\}\}"

    # Remove all valid placeholders first
    text_without_valid = re.sub(valid_placeholder_pattern, "", text)

    # If any double-brace fragments remain, they are malformed
    if "{{" in text_without_valid or "}}" in text_without_valid:
        # Capture nearby snippets around remaining malformed braces
        for m in re.finditer(r"\{\{|\}\}", text_without_valid):
            start = max(0, m.start() - 40)
            end = min(len(text_without_valid), m.end() + 80)
            problems.append(text_without_valid[start:end].strip())

    # Placeholders with spaces inside are malformed, e.g. {{scope1 scope2_current}}
    spaced_placeholders = re.findall(r"\{\{[^{}]*\s+[^{}]*\}\}", text)
    problems.extend(spaced_placeholders)

    # Wrong closing pattern, e.g. {{scope1_current))
    bad_closing = re.findall(r"\{\{[A-Za-z0-9_\s]+?\)\)", text)
    problems.extend(bad_closing)

    return sorted(set([p for p in problems if p]))

malformed_fragments = find_malformed_placeholder_fragments(generated_template)

print("\nMALFORMED PLACEHOLDER FRAGMENTS:")
print(malformed_fragments)


# =============================================================================
# PRINT RESULTS
# =============================================================================

print("\nREFERENCE TARGET TEMPLATE:\n")
print(sample["messages"][2]["content"])

print("\nGENERATED TARGET TEMPLATE:\n")
print(generated_template)

print("\nALLOWED PLACEHOLDERS:\n")
print(sorted(allowed_placeholders))

print("\nUSED PLACEHOLDERS:\n")
print(sorted(used_placeholders))

print("\nINVALID PLACEHOLDERS:")
print(invalid_placeholders)

print("\nUNUSED ALLOWED PLACEHOLDERS:")
print(unused_allowed_placeholders)

print("\nBAD SINGLE-BRACE PLACEHOLDERS:")
print(bad_single_brace)

print("\nUNFILLED PLACEHOLDERS AFTER REPLACEMENT:")
print(unfilled_after_replacement)

print("\nFILLED ESG SECTION:\n")
print(filled_output)


# =============================================================================
# SIMPLE VERDICT
# =============================================================================

print("\n" + "=" * 80)
print("VALIDATION VERDICT")
print("=" * 80)

if invalid_placeholders:
    print("❌ Invalid placeholders found.")
elif malformed_fragments:
    print("❌ Malformed placeholder fragments found.")
elif bad_single_brace:
    print("❌ Bad single-brace placeholders found.")
elif unfilled_after_replacement:
    print("❌ Some placeholders were not filled.")
else:
    print("✅ Placeholder output looks valid. Python replacement worked.")