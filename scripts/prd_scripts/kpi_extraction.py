#final kpi extractor from esg dataset jsonl
### =============================================================================
# ESG KPI EXTRACTOR V3 - ACCURACY FIXED VERSION
# Kaggle Notebook Version
# =============================================================================
# Input : esg_dataset_complete.jsonl
# Output:
#   /kaggle/working/esg_kpi_v3_output/esg_kpis_v3.csv
#   /kaggle/working/esg_kpi_v3_output/esg_kpis_v3.jsonl
#
# Extracts:
#   - Scope 1, Scope 2, Scope 3 emissions
#   - Renewable energy %
#   - Water consumption
#   - Waste recycled
#   - Total waste generated
#   - Female employee %
#   - Women on board %
#   - Energy intensity
#   - Net zero / ESG targets
#   - YoY reductions
# =============================================================================

import os
import re
import json
import traceback
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

import pandas as pd
from tqdm.auto import tqdm


# =============================================================================
# CONFIG
# =============================================================================

INPUT_JSONL = "/kaggle/working/esg_dataset_v3/esg_dataset_24-25_v3.jsonl"

OUTPUT_DIR = "/kaggle/working/esg_kpi_output"
OUTPUT_CSV = f"{OUTPUT_DIR}/esg_kpis_24-25.csv"
OUTPUT_JSONL = f"{OUTPUT_DIR}/esg_kpis_24-25.jsonl"

MAX_WORKERS = 14
USE_PROCESS_POOL = True
INCLUDE_EVIDENCE = True

os.makedirs(OUTPUT_DIR, exist_ok=True)

# QUALITY LIMITS
MAX_REASONABLE_ENERGY_GJ = 10_000_000_000
MAX_REASONABLE_ENERGY_INTENSITY = 1_000_000
MIN_REASONABLE_NET_ZERO_YEAR = 2030
MAX_REASONABLE_NET_ZERO_YEAR = 2100

MIN_REASONABLE_RENEWABLE_PERCENT = 0
MAX_REASONABLE_RENEWABLE_PERCENT = 100


# =============================================================================
# REGEX
# =============================================================================

NUM_RE = re.compile(
    r"(?<![A-Za-z0-9])"
    r"\(?-?(?:\d{1,3}(?:,\d{2,3})+|\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?\)?"
    r"\s*%?",
    re.I,
)

FY_RE = re.compile(
    r"\b(?:FY|financial\s+year)?\s*20\d{2}\s*[-/]\s*(?:\d{2}|20\d{2})\b",
    re.I,
)

EMISSION_UNIT_RE = re.compile(
    r"metric\s+ton(?:ne|nes|s)?\s+(?:of\s+)?co\s*2(?:.{0,80})?(?:equivalent|e|eq)|"
    r"t\s*co\s*2\s*e|tco2e|tco2\s*eq|mtco2e|mt\s*co\s*2\s*e|"
    r"co\s*2(?:.{0,40})?equivalent|co2e|co2\s*eq|kg\s*co\s*2\s*e|kgco2e",
    re.I | re.S,
)

KG_CO2_RE = re.compile(r"kg\s*co\s*2\s*e|kgco2e", re.I)

ENERGY_UNIT_RE = re.compile(
    r"giga\s*joules?|\bgj\b|mega\s*joules?|\bmj\b|"
    r"\bkwh\b|kwhr|kilowatt\s*hour|\bmwh\b|megawatt\s*hour",
    re.I,
)

WATER_UNIT_RE = re.compile(
    r"kilolit(?:re|er)s?|\bkl\b|kilo\s*lit(?:re|er)s?|"
    r"megalit(?:re|er)s?|\bml\b",
    re.I,
)

WASTE_UNIT_RE = re.compile(
    r"metric\s+ton(?:ne|nes|s)?|\bmt\b|ton(?:ne|nes|s)?",
    re.I,
)


# =============================================================================
# BASIC HELPERS
# =============================================================================

def normalize_text(text):
    if not isinstance(text, str):
        return ""

    replacements = {
        "\u00a0": " ",
        "\u200b": " ",
        "\u200c": " ",
        "\u200d": " ",
        "CO₂": "CO2",
        "co₂": "co2",
        "CO²": "CO2",
        "co²": "co2",
        "–": "-",
        "—": "-",
        "−": "-",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n{4,}", "\n\n\n", text)

    return text.strip()


def compact(text, n=900):
    return re.sub(r"\s+", " ", text or "").strip()[:n]


def search_text(text):
    return re.sub(r"\s+", " ", normalize_text(text)).lower()


def get_lines(text):
    return [line.strip() for line in normalize_text(text).splitlines() if line.strip()]


def clean_num(raw):
    if raw is None:
        return None

    s = str(raw).strip().replace("%", "").strip()
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()").replace(",", "")

    try:
        val = float(s)
        return -val if neg else val
    except Exception:
        return None


def remove_years(text):
    text = FY_RE.sub(" ", text)
    text = re.sub(r"\b20\d{2}\s*[-/]\s*20\d{2}\b", " ", text)
    text = re.sub(r"\b20\d{2}\s*[-/]\s*\d{2}\b", " ", text)
    return text


def is_yearish(value, raw):
    if value is None:
        return True

    raw = str(raw).strip()

    if raw.endswith("%"):
        return False

    if 1900 <= value <= 2100 and "." not in raw and "," not in raw:
        return True

    return False


def extract_numbers(text, allow_percent=False):
    text = remove_years(text)
    out = []

    for m in NUM_RE.finditer(text):
        raw = m.group(0).strip()
        val = clean_num(raw)

        if val is None:
            continue

        if raw.endswith("%") and not allow_percent:
            continue

        if is_yearish(val, raw):
            continue

        local_left = text[max(0, m.start() - 20):m.start()].lower()
        local_right = text[m.end():m.end() + 20].lower()

        # Remove leaked Scope/Principle serial numbers
        if val in {1, 2, 3, 4, 5, 6, 7, 8, 9}:
            if (
                "scope" in local_left or "scope" in local_right or
                "principle" in local_left or "principle" in local_right or
                "pdf_page" in local_left
            ):
                continue

        out.append((raw, val, m.start(), m.end()))

    return out


def detect_unit(text, kind):
    if kind == "emissions":
        if KG_CO2_RE.search(text):
            return "kgCO2e"
        if EMISSION_UNIT_RE.search(text):
            return "tCO2e"
        return None

    if kind == "energy":
        m = ENERGY_UNIT_RE.search(text)
        if not m:
            return None

        u = m.group(0).lower()

        if "gj" in u or "giga" in u:
            return "GJ"
        if "mj" in u or "mega" in u:
            return "MJ"
        if "mwh" in u or "megawatt" in u:
            return "MWh"
        if "kwh" in u or "kwhr" in u or "kilowatt" in u:
            return "kWh"

        return None

    if kind == "water":
        m = WATER_UNIT_RE.search(text)
        if not m:
            return None

        u = m.group(0).lower()

        if u == "ml" or "mega" in u:
            return "ML"

        return "KL"

    if kind == "waste":
        if WASTE_UNIT_RE.search(text):
            return "tonnes"
        return None

    return None


def assign_current_previous(nums, context):
    if not nums:
        return None, None

    values = [x[1] for x in nums]

    current = values[0] if len(values) >= 1 else None
    previous = values[1] if len(values) >= 2 else None

    c = search_text(context)

    prev_pos = c.find("previous financial year")
    curr_pos = c.find("current financial year")

    if len(values) >= 2 and prev_pos != -1 and curr_pos != -1 and prev_pos < curr_pos:
        previous = values[0]
        current = values[1]

    return current, previous


def emission_to_tco2e(value, unit):
    if value is None:
        return None
    if unit == "kgCO2e":
        return value / 1000.0
    return value


def energy_to_gj(value, unit):
    if value is None:
        return None
    if unit == "GJ":
        return value
    if unit == "MJ":
        return value / 1000.0
    if unit == "kWh":
        return value * 0.0036
    if unit == "MWh":
        return value * 3.6
    return value


def water_to_kl(value, unit):
    if value is None:
        return None
    if unit == "KL":
        return value
    if unit == "ML":
        return value * 1000.0
    return value


def pct_change(current, previous, reduction=True):
    if current is None or previous is None or previous == 0:
        return None

    if reduction:
        return (previous - current) / abs(previous) * 100.0

    return (current - previous) / abs(previous) * 100.0


# =============================================================================
# BLOCK EXTRACTION
# =============================================================================

def candidate_blocks(text, label_patterns, before=1, after=6, max_blocks=200):
    lines = get_lines(text)
    blocks = []

    for i, line in enumerate(lines):
        if any(re.search(p, line, flags=re.I) for p in label_patterns):
            start = max(0, i - before)
            end = min(len(lines), i + after + 1)

            block = "\n".join(lines[start:end])

            blocks.append({
                "line_index": i,
                "primary": line,
                "block": block,
            })

    return blocks[:max_blocks]


def extract_values_after_label(block, label_patterns, allow_percent=False):
    """
    Extract numbers only AFTER the KPI label.
    This prevents serial numbers like:
      4. Energy intensity ...
    from being selected as KPI values.
    """
    best_match = None

    for p in label_patterns:
        m = re.search(p, block, flags=re.I)
        if m and (best_match is None or m.start() < best_match.start()):
            best_match = m

    if best_match:
        segment = block[best_match.end():]
    else:
        segment = block

    # Remove table headers that sometimes appear after label
    segment = re.sub(r"current\s+financial\s+year", " ", segment, flags=re.I)
    segment = re.sub(r"previous\s+financial\s+year", " ", segment, flags=re.I)
    segment = re.sub(r"unit\s+fy\s*20\d{2}", " ", segment, flags=re.I)

    return extract_numbers(segment, allow_percent=allow_percent)


def best_block_metric(
    text,
    label_patterns,
    kind=None,
    exclude_primary=None,
    require_unit=False,
    allow_percent=False,
    exact_bonus_patterns=None,
    min_value=None,
    max_value=None,
    reject_integer_serial=True,
    before=1,
    after=6,
):
    exclude_primary = exclude_primary or []
    exact_bonus_patterns = exact_bonus_patterns or []

    candidates = []

    for item in candidate_blocks(text, label_patterns, before=before, after=after):
        primary = item["primary"]
        block = item["block"]

        if any(re.search(p, primary, flags=re.I) for p in exclude_primary):
            continue

        unit = detect_unit(block, kind) if kind else None

        if require_unit and not unit:
            continue

        nums = extract_values_after_label(block, label_patterns, allow_percent=allow_percent)

        filtered = []

        for raw, val, s, e in nums:
            if min_value is not None and val < min_value:
                continue

            if max_value is not None and val > max_value:
                continue

            if reject_integer_serial and val in {1, 2, 3, 4, 5, 6, 7, 8, 9}:
                if "." not in raw and "," not in raw and not raw.endswith("%"):
                    continue

            filtered.append((raw, val, s, e))

        nums = filtered

        if not nums:
            continue

        score = 0

        if "|" in block:
            score += 15

        if unit:
            score += 30

        if kind == "emissions" and EMISSION_UNIT_RE.search(block):
            score += 60

        if kind == "waste" and WASTE_UNIT_RE.search(block):
            score += 25

        if kind == "water" and WATER_UNIT_RE.search(block):
            score += 25

        if kind == "energy" and ENERGY_UNIT_RE.search(block):
            score += 25

        for p in label_patterns:
            if re.search(p, primary, flags=re.I):
                score += 35
            elif re.search(p, block, flags=re.I):
                score += 15

        for p in exact_bonus_patterns:
            if re.search(p, primary, flags=re.I):
                score += 50
            elif re.search(p, block, flags=re.I):
                score += 20

        score += min(len(nums), 4) * 4

        # Penalize very large messy blocks
        if len(block) > 2000:
            score -= 15

        candidates.append({
            "score": score,
            "primary": primary,
            "block": block,
            "unit": unit,
            "nums": nums,
        })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["score"], reverse=True)
    best = candidates[0]

    current, previous = assign_current_previous(best["nums"], best["block"])

    result = {
        "current": current,
        "previous": previous,
        "unit": best["unit"],
        "raw_values": [x[0] for x in best["nums"][:8]],
        "confidence": best["score"],
    }

    if INCLUDE_EVIDENCE:
        result["evidence"] = compact(best["block"])

    return result

# =============================================================================
# QUALITY HELPERS
# =============================================================================


def is_reasonable_energy(value):
    if value is None:
        return False

    return 0 <= value <= MAX_REASONABLE_ENERGY_GJ



def is_reasonable_energy_intensity(value):
    if value is None:
        return False

    return 0 <= value <= MAX_REASONABLE_ENERGY_INTENSITY



def safe_percent(numerator, denominator):
    if numerator is None or denominator in [None, 0]:
        return None

    val = numerator / denominator * 100.0

    if not (MIN_REASONABLE_RENEWABLE_PERCENT <= val <= MAX_REASONABLE_RENEWABLE_PERCENT):
        return None

    return val



def choose_best_total_energy_candidate(candidates, renewable_gj=None):
    """
    Choose the most plausible total-energy row.

    Fixes:
    - renewable > total issue
    - wrong nearby subtotal rows
    - intensity rows being selected
    """

    if not candidates:
        return None

    filtered = []

    for c in candidates:
        val = c.get("current")

        if not is_reasonable_energy(val):
            continue

        evidence = (c.get("evidence") or "").lower()

        # reject intensity rows
        if "intensity" in evidence:
            continue

        # reject renewable rows accidentally selected as total
        if "renewable" in evidence and "non-renewable" not in evidence:
            continue

        if renewable_gj is not None and val is not None:
            if val < renewable_gj:
                continue

        filtered.append(c)

    if not filtered:
        return None

    # Prefer rows that explicitly contain Total(A+B+C)
    def score(x):
        s = 0

        evidence = (x.get("evidence") or "").lower()

        if "total" in evidence:
            s += 100

        if "a+b+c" in evidence:
            s += 120

        if "a+b" in evidence:
            s += 90

        if "energy consumed" in evidence:
            s += 40

        if "from renewable" in evidence:
            s -= 80

        if "non-renewable" in evidence:
            s += 20

        return s

    filtered.sort(key=score, reverse=True)

    return filtered[0]

# =============================================================================
# KPI EXTRACTORS
# =============================================================================

def extract_emissions(text):
    configs = {
        "scope1": {
            "labels": [
                r"\btotal\s+scope\s*1\s+emissions?\b",
                r"\bscope\s*1\s+emissions?\b",
                r"\bdirect\s+ghg\s+emissions?\b",
            ],
            "exact": [r"\btotal\s+scope\s*1\s+emissions?\b"],
        },
        "scope2": {
            "labels": [
                r"\btotal\s+scope\s*2\s+emissions?\b",
                r"\bscope\s*2\s+emissions?\b",
                r"\bindirect\s+ghg\s+emissions?\b",
            ],
            "exact": [r"\btotal\s+scope\s*2\s+emissions?\b"],
        },
        "scope3": {
            "labels": [
                r"\btotal\s+scope\s*3\s+emissions?\b",
                r"\bscope\s*3\s+emissions?\b",
                r"\bvalue\s+chain\s+emissions?\b",
            ],
            "exact": [r"\btotal\s+scope\s*3\s+emissions?\b"],
        },
    }

    out = {}

    exclude = [
        r"intensity",
        r"per\s+rupee",
        r"per\s+crore",
        r"turnover",
        r"scope\s*1\s*(?:and|&|\+)\s*scope\s*2",
    ]

    for scope, cfg in configs.items():
        row = best_block_metric(
            text,
            label_patterns=cfg["labels"],
            kind="emissions",
            exclude_primary=exclude,
            require_unit=True,
            exact_bonus_patterns=cfg["exact"] + [
                r"metric\s+ton(?:ne|nes|s)?.{0,80}co\s*2.{0,80}equivalent",
                r"co2e",
            ],
            min_value=0,
            reject_integer_serial=True,
            before=1,
            after=8,
        )

        if row:
            row["current_tco2e"] = emission_to_tco2e(row["current"], row["unit"])
            row["previous_tco2e"] = emission_to_tco2e(row["previous"], row["unit"])
            row["yoy_reduction_percent"] = pct_change(
                row["current_tco2e"],
                row["previous_tco2e"],
                reduction=True,
            )

        out[scope] = row

    return out


def extract_percent_near_anchor(text, anchor_patterns, exclude_patterns=None, max_value=100, window=320):
    exclude_patterns = exclude_patterns or []
    flat = re.sub(r"\s+", " ", normalize_text(text))

    candidates = []

    for p in anchor_patterns:
        for m in re.finditer(p, flat, flags=re.I):
            start = max(0, m.start() - window)
            end = min(len(flat), m.end() + window)
            snippet = flat[start:end]

            if any(re.search(ex, snippet, flags=re.I) for ex in exclude_patterns):
                continue

            # Fix: from 13% in FY 2024 to 14.6% in FY 2025 -> choose 14.6
            from_to = re.search(
                r"from\s+(\d{1,3}(?:\.\d+)?)\s*%[^.]{0,120}?\bto\s+(\d{1,3}(?:\.\d+)?)\s*%",
                snippet,
                flags=re.I,
            )

            if from_to:
                val = float(from_to.group(2))
                if 0 <= val <= max_value:
                    candidates.append((150, val, snippet))
                    continue

            increased_to = re.search(
                r"(?:increased|improved|rose|grown|grew|reached)\s+(?:from\s+\d{1,3}(?:\.\d+)?\s*%[^.]{0,120}?\bto\s+)?(\d{1,3}(?:\.\d+)?)\s*%",
                snippet,
                flags=re.I,
            )

            if increased_to:
                val = float(increased_to.group(1))
                if 0 <= val <= max_value:
                    candidates.append((130, val, snippet))
                    continue

            for pm in re.finditer(r"(\d{1,3}(?:\.\d+)?)\s*%", snippet):
                val = float(pm.group(1))

                if not (0 <= val <= max_value):
                    continue

                abs_pos = start + pm.start()
                distance = abs(abs_pos - m.start())
                after_bonus = 25 if abs_pos >= m.start() else 0

                score = 100 - min(distance, 100) + after_bonus

                candidates.append((score, val, snippet))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)

    result = {
        "value": candidates[0][1],
        "unit": "%",
        "confidence": candidates[0][0],
    }

    if INCLUDE_EVIDENCE:
        result["evidence"] = compact(candidates[0][2])

    return result

# =============================================================================
# TOTAL ENERGY EXTRACTOR (V5)
# =============================================================================


def extract_total_energy_candidates(text):
    """
    Collect multiple total-energy candidates.
    We later choose the best plausible row.
    """

    candidates = []

    blocks = candidate_blocks(
        text,
        label_patterns=[
            r"\btotal\s+energy\s+consumed\b",
            r"\btotal\s+energy\s+consumption\b",
            r"\btotal\s*\(a\s*\+\s*b\s*\+\s*c\)\b",
            r"\btotal\s*\(a\s*\+\s*b\)\b",
        ],
        before=1,
        after=6,
        max_blocks=50,
    )

    for item in blocks:
        block = item["block"]

        nums = extract_numbers(block)

        nums = [
            n for n in nums
            if n[1] not in {1,2,3,4,5,6,7,8,9}
        ]

        if not nums:
            continue

        current, previous = assign_current_previous(nums, block)

        unit = detect_unit(block, "energy")

        current_gj = energy_to_gj(current, unit)
        previous_gj = energy_to_gj(previous, unit)

        candidates.append({
            "current": current_gj,
            "previous": previous_gj,
            "unit": "GJ",
            "evidence": compact(block),
        })

    return candidates

# =============================================================================
# RENEWABLE ENERGY EXTRACTOR V5
# =============================================================================


def extract_renewable_energy(text):
    renewable_energy = best_block_metric(
        text,
        label_patterns=[
            r"\btotal\s+energy\s+consumed\s+from\s+renewable\s+sources?\b",
            r"\benergy\s+consumed\s+from\s+renewable\s+sources?\b",
            r"\bfrom\s+renewable\s+sources?\b",
        ],
        kind="energy",
        exclude_primary=[r"intensity"],
        require_unit=False,
        exact_bonus_patterns=[
            r"\btotal\s+energy\s+consumed\s+from\s+renewable\s+sources?\b",
        ],
        min_value=0,
        reject_integer_serial=True,
        before=1,
        after=5,
    )

    renewable_gj = None

    if renewable_energy:
        renewable_gj = energy_to_gj(
            renewable_energy.get("current"),
            renewable_energy.get("unit"),
        )

    total_candidates = extract_total_energy_candidates(text)

    total_energy = choose_best_total_energy_candidate(
        total_candidates,
        renewable_gj=renewable_gj,
    )

    total_gj = None

    if total_energy:
        total_gj = total_energy.get("current")

    computed_percent = safe_percent(
        renewable_gj,
        total_gj,
    )

    direct_percent = extract_percent_near_anchor(
        text,
        anchor_patterns=[
            r"\bshare\s+of\s+renewable\s+energy\b",
            r"\brenewable\s+energy\s+share\b",
            r"\brenewable\s+energy\s+percentage\b",
        ],
        exclude_patterns=[
            r"female",
            r"women",
            r"water",
            r"waste",
        ],
        max_value=100,
    )

    final_percent = None
    source = None
    review_flag = None

    # Prefer computed percentage.
    if computed_percent is not None:
        final_percent = computed_percent
        source = "computed_from_energy_table"

    elif direct_percent is not None:
        final_percent = direct_percent["value"]
        source = "direct_text_percent"

    # sanity check
    if (
        computed_percent is not None
        and direct_percent is not None
    ):
        diff = abs(computed_percent - direct_percent["value"])

        if diff > 5:
            review_flag = "renewable_direct_computed_mismatch"

    return {
        "renewable_energy_percent": final_percent,
        "renewable_energy_percent_source": source,
        "renewable_energy_review_flag": review_flag,
        "renewable_energy_consumption": renewable_energy,
        "renewable_energy_consumption_gj": renewable_gj,
        "total_energy_consumption": total_energy,
        "total_energy_consumption_gj": total_gj,
        "renewable_energy_percent_computed": computed_percent,
        "renewable_energy_percent_direct": direct_percent,
    }
    
def extract_water(text):
    consumption = best_block_metric(
        text,
        label_patterns=[
            r"\btotal\s+volume\s+of\s+water\s+consumption\b",
            r"\btotal\s+water\s+consumption\b",
            r"\bwater\s+consumption\b",
        ],
        kind="water",
        exclude_primary=[
            r"intensity",
            r"per\s+rupee",
            r"per\s+crore",
        ],
        require_unit=False,
        exact_bonus_patterns=[
            r"\btotal\s+volume\s+of\s+water\s+consumption\b",
            r"\btotal\s+water\s+consumption\b",
        ],
        min_value=0,
        reject_integer_serial=True,
        before=1,
        after=6,
    )

    withdrawal = best_block_metric(
        text,
        label_patterns=[
            r"\btotal\s+water\s+withdrawal\b",
            r"\bwater\s+withdrawal\b",
        ],
        kind="water",
        exclude_primary=[
            r"intensity",
            r"per\s+rupee",
            r"per\s+crore",
        ],
        require_unit=False,
        exact_bonus_patterns=[
            r"\btotal\s+water\s+withdrawal\b",
        ],
        min_value=0,
        reject_integer_serial=True,
        before=1,
        after=6,
    )

    if consumption:
        consumption["current_kl"] = water_to_kl(consumption["current"], consumption["unit"])
        consumption["previous_kl"] = water_to_kl(consumption["previous"], consumption["unit"])
        consumption["yoy_reduction_percent"] = pct_change(
            consumption["current_kl"],
            consumption["previous_kl"],
            reduction=True,
        )

    if withdrawal:
        withdrawal["current_kl"] = water_to_kl(withdrawal["current"], withdrawal["unit"])
        withdrawal["previous_kl"] = water_to_kl(withdrawal["previous"], withdrawal["unit"])
        withdrawal["yoy_reduction_percent"] = pct_change(
            withdrawal["current_kl"],
            withdrawal["previous_kl"],
            reduction=True,
        )

    return {
        "water_consumption": consumption,
        "water_withdrawal": withdrawal,
    }


# =============================================================================
# KPI V3.1 WASTE FIX
# Replace your V3 extract_waste() with this version.
# Also add the helper functions below before extract_waste().
# =============================================================================

TOTAL_WASTE_FORMULA_RE = re.compile(
    r"\btotal\s*[\(\[]?\s*"
    r"a\s*\+\s*b\s*\+\s*c\s*\+\s*d\s*\+\s*e\s*\+\s*f\s*\+\s*g\s*\+\s*h"
    r"\s*[\)\]]?",
    re.I,
)

TOTAL_WASTE_TEXT_RE = re.compile(
    r"\btotal\s+waste\s+generated\b",
    re.I,
)

WASTE_CATEGORY_RE = re.compile(
    r"\bplastic\s+waste\b|"
    r"\be[-\s]?waste\b|"
    r"\bbio[-\s]?medical\s+waste\b|"
    r"\bconstruction\s+and\s+demolition\s+waste\b|"
    r"\bbattery\s+waste\b|"
    r"\bradioactive\s+waste\b|"
    r"\bother\s+hazardous\s+waste\b|"
    r"\bother\s+non[-\s]?hazardous\s+waste\b|"
    r"\bhazardous\s+waste\b|"
    r"\bnon[-\s]?hazardous\s+waste\b",
    re.I,
)

WASTE_RECOVERY_SECTION_RE = re.compile(
    r"\bwaste\s+recovered\b|"
    r"\bwaste\s+recycled\b|"
    r"\bfor\s+each\s+category\s+of\s+waste\s+generated.*recovered\b|"
    r"\(i\)\s*recycled\b|"
    r"\bi\.\s*recycled\b",
    re.I,
)


def extract_total_waste_generated_strict(text, recycled_current=None):
    """
    Strict extractor for total waste generated.

    Main fix:
    - Prefer exact row: Total (A+B+C+D+E+F+G+H)
    - Do NOT take Plastic waste / E-waste category rows.
    - If total is less than recycled waste, reject it.
    """

    lines = get_lines(text)
    candidates = []

    for i, line in enumerate(lines):
        if "pdf_page" in line.lower():
            continue

        exact_formula = TOTAL_WASTE_FORMULA_RE.search(line)
        total_text = TOTAL_WASTE_TEXT_RE.search(line)

        if not exact_formula and not total_text:
            continue

        # Case 1: exact Total (A+B+C+D+E+F+G+H) row.
        # This is the best source.
        if exact_formula:
            block = "\n".join(lines[max(0, i - 1): min(len(lines), i + 4)])
            match = TOTAL_WASTE_FORMULA_RE.search(block)

            segment = block[match.end():] if match else block
            nums = extract_numbers(segment, allow_percent=False)

            nums = [
                n for n in nums
                if n[1] not in {1, 2, 3, 4, 5, 6, 7, 8, 9}
            ]

            if not nums:
                continue

            current, previous = assign_current_previous(nums, block)

            if recycled_current is not None and current is not None:
                if current < recycled_current:
                    continue

            unit = detect_unit(block, "waste") or "tonnes"

            score = 300

            if "|" in block:
                score += 20

            if unit:
                score += 20

            candidates.append({
                "score": score,
                "current": current,
                "previous": previous,
                "unit": unit,
                "raw_values": [x[0] for x in nums[:8]],
                "evidence": compact(block),
            })

            continue

        # Case 2: same-line "Total waste generated | unit | value | value"
        # Use only same line unless the next rows are not category rows.
        if total_text:
            # Avoid headings like "Total waste generated by category..."
            # if the next rows are Plastic waste, E-waste, etc.
            next_block = "\n".join(lines[i: min(len(lines), i + 8)])

            if WASTE_CATEGORY_RE.search(next_block):
                # This is likely just the heading above category rows.
                # Do not extract Plastic waste as total.
                same_line_segment = line[total_text.end():]
                same_line_nums = extract_numbers(same_line_segment, allow_percent=False)

                same_line_nums = [
                    n for n in same_line_nums
                    if n[1] not in {1, 2, 3, 4, 5, 6, 7, 8, 9}
                ]

                if not same_line_nums:
                    continue

                nums = same_line_nums
                block = line
            else:
                block = "\n".join(lines[i: min(len(lines), i + 3)])
                match = TOTAL_WASTE_TEXT_RE.search(block)
                segment = block[match.end():] if match else block
                nums = extract_numbers(segment, allow_percent=False)

                nums = [
                    n for n in nums
                    if n[1] not in {1, 2, 3, 4, 5, 6, 7, 8, 9}
                ]

            if not nums:
                continue

            current, previous = assign_current_previous(nums, block)

            if recycled_current is not None and current is not None:
                if current < recycled_current:
                    continue

            unit = detect_unit(block, "waste") or "tonnes"

            candidates.append({
                "score": 180,
                "current": current,
                "previous": previous,
                "unit": unit,
                "raw_values": [x[0] for x in nums[:8]],
                "evidence": compact(block),
            })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["score"], reverse=True)

    best = candidates[0]

    result = {
        "current": best["current"],
        "previous": best["previous"],
        "unit": best["unit"],
        "raw_values": best["raw_values"],
        "confidence": best["score"],
    }

    if INCLUDE_EVIDENCE:
        result["evidence"] = best["evidence"]

    return result


def extract_total_waste_generated_fallback_max(text, recycled_current=None):
    """
    Fallback only.

    Looks inside the waste generated table and selects the largest plausible value.
    This is useful when the Total row exists but text layout is broken.
    """

    lines = get_lines(text)
    candidates = []

    start_indexes = []

    for i, line in enumerate(lines):
        low = line.lower()

        if "waste generated" in low or "total waste generated" in low:
            start_indexes.append(i)

    for start in start_indexes:
        block_lines = []

        for j in range(start, min(len(lines), start + 90)):
            line = lines[j]

            if j > start + 3 and WASTE_RECOVERY_SECTION_RE.search(line):
                break

            block_lines.append(line)

        block = "\n".join(block_lines)

        # Do not use fallback on unrelated sections.
        if not re.search(r"waste\s+generated|plastic\s+waste|e[-\s]?waste", block, re.I):
            continue

        line_candidates = []

        for line in block_lines:
            if "pdf_page" in line.lower():
                continue

            if WASTE_RECOVERY_SECTION_RE.search(line):
                break

            nums = extract_numbers(line, allow_percent=False)

            nums = [
                n for n in nums
                if n[1] not in {1, 2, 3, 4, 5, 6, 7, 8, 9}
            ]

            if not nums:
                continue

            current, previous = assign_current_previous(nums, line)
            unit = detect_unit(line, "waste") or detect_unit(block, "waste") or "tonnes"

            if current is None:
                continue

            if recycled_current is not None and current < recycled_current:
                continue

            # Prefer exact total formula if it appears.
            score = current

            if TOTAL_WASTE_FORMULA_RE.search(line):
                score += 10_000_000

            if TOTAL_WASTE_TEXT_RE.search(line):
                score += 5_000_000

            line_candidates.append({
                "score": score,
                "current": current,
                "previous": previous,
                "unit": unit,
                "raw_values": [x[0] for x in nums[:8]],
                "evidence": compact(line),
            })

        if line_candidates:
            line_candidates.sort(key=lambda x: x["score"], reverse=True)
            candidates.append(line_candidates[0])

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["score"], reverse=True)
    best = candidates[0]

    result = {
        "current": best["current"],
        "previous": best["previous"],
        "unit": best["unit"],
        "raw_values": best["raw_values"],
        "confidence": 90,
    }

    if INCLUDE_EVIDENCE:
        result["evidence"] = best["evidence"]

    return result

# =============================================================================
# KPI V3.1 WASTE FIX
# Replace your V3 extract_waste() with this version.
# Also add the helper functions below before extract_waste().
# =============================================================================

TOTAL_WASTE_FORMULA_RE = re.compile(
    r"\btotal\s*[\(\[]?\s*"
    r"a\s*\+\s*b\s*\+\s*c\s*\+\s*d\s*\+\s*e\s*\+\s*f\s*\+\s*g\s*\+\s*h"
    r"\s*[\)\]]?",
    re.I,
)

TOTAL_WASTE_TEXT_RE = re.compile(
    r"\btotal\s+waste\s+generated\b",
    re.I,
)

WASTE_CATEGORY_RE = re.compile(
    r"\bplastic\s+waste\b|"
    r"\be[-\s]?waste\b|"
    r"\bbio[-\s]?medical\s+waste\b|"
    r"\bconstruction\s+and\s+demolition\s+waste\b|"
    r"\bbattery\s+waste\b|"
    r"\bradioactive\s+waste\b|"
    r"\bother\s+hazardous\s+waste\b|"
    r"\bother\s+non[-\s]?hazardous\s+waste\b|"
    r"\bhazardous\s+waste\b|"
    r"\bnon[-\s]?hazardous\s+waste\b",
    re.I,
)

WASTE_RECOVERY_SECTION_RE = re.compile(
    r"\bwaste\s+recovered\b|"
    r"\bwaste\s+recycled\b|"
    r"\bfor\s+each\s+category\s+of\s+waste\s+generated.*recovered\b|"
    r"\(i\)\s*recycled\b|"
    r"\bi\.\s*recycled\b",
    re.I,
)


def extract_total_waste_generated_strict(text, recycled_current=None):
    """
    Strict extractor for total waste generated.

    Main fix:
    - Prefer exact row: Total (A+B+C+D+E+F+G+H)
    - Do NOT take Plastic waste / E-waste category rows.
    - If total is less than recycled waste, reject it.
    """

    lines = get_lines(text)
    candidates = []

    for i, line in enumerate(lines):
        if "pdf_page" in line.lower():
            continue

        exact_formula = TOTAL_WASTE_FORMULA_RE.search(line)
        total_text = TOTAL_WASTE_TEXT_RE.search(line)

        if not exact_formula and not total_text:
            continue

        # Case 1: exact Total (A+B+C+D+E+F+G+H) row.
        # This is the best source.
        if exact_formula:
            block = "\n".join(lines[max(0, i - 1): min(len(lines), i + 4)])
            match = TOTAL_WASTE_FORMULA_RE.search(block)

            segment = block[match.end():] if match else block
            nums = extract_numbers(segment, allow_percent=False)

            nums = [
                n for n in nums
                if n[1] not in {1, 2, 3, 4, 5, 6, 7, 8, 9}
            ]

            if not nums:
                continue

            current, previous = assign_current_previous(nums, block)

            if recycled_current is not None and current is not None:
                if current < recycled_current:
                    continue

            unit = detect_unit(block, "waste") or "tonnes"

            score = 300

            if "|" in block:
                score += 20

            if unit:
                score += 20

            candidates.append({
                "score": score,
                "current": current,
                "previous": previous,
                "unit": unit,
                "raw_values": [x[0] for x in nums[:8]],
                "evidence": compact(block),
            })

            continue

        # Case 2: same-line "Total waste generated | unit | value | value"
        # Use only same line unless the next rows are not category rows.
        if total_text:
            # Avoid headings like "Total waste generated by category..."
            # if the next rows are Plastic waste, E-waste, etc.
            next_block = "\n".join(lines[i: min(len(lines), i + 8)])

            if WASTE_CATEGORY_RE.search(next_block):
                # This is likely just the heading above category rows.
                # Do not extract Plastic waste as total.
                same_line_segment = line[total_text.end():]
                same_line_nums = extract_numbers(same_line_segment, allow_percent=False)

                same_line_nums = [
                    n for n in same_line_nums
                    if n[1] not in {1, 2, 3, 4, 5, 6, 7, 8, 9}
                ]

                if not same_line_nums:
                    continue

                nums = same_line_nums
                block = line
            else:
                block = "\n".join(lines[i: min(len(lines), i + 3)])
                match = TOTAL_WASTE_TEXT_RE.search(block)
                segment = block[match.end():] if match else block
                nums = extract_numbers(segment, allow_percent=False)

                nums = [
                    n for n in nums
                    if n[1] not in {1, 2, 3, 4, 5, 6, 7, 8, 9}
                ]

            if not nums:
                continue

            current, previous = assign_current_previous(nums, block)

            if recycled_current is not None and current is not None:
                if current < recycled_current:
                    continue

            unit = detect_unit(block, "waste") or "tonnes"

            candidates.append({
                "score": 180,
                "current": current,
                "previous": previous,
                "unit": unit,
                "raw_values": [x[0] for x in nums[:8]],
                "evidence": compact(block),
            })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["score"], reverse=True)

    best = candidates[0]

    result = {
        "current": best["current"],
        "previous": best["previous"],
        "unit": best["unit"],
        "raw_values": best["raw_values"],
        "confidence": best["score"],
    }

    if INCLUDE_EVIDENCE:
        result["evidence"] = best["evidence"]

    return result


def extract_total_waste_generated_fallback_max(text, recycled_current=None):
    """
    Fallback only.

    Looks inside the waste generated table and selects the largest plausible value.
    This is useful when the Total row exists but text layout is broken.
    """

    lines = get_lines(text)
    candidates = []

    start_indexes = []

    for i, line in enumerate(lines):
        low = line.lower()

        if "waste generated" in low or "total waste generated" in low:
            start_indexes.append(i)

    for start in start_indexes:
        block_lines = []

        for j in range(start, min(len(lines), start + 90)):
            line = lines[j]

            if j > start + 3 and WASTE_RECOVERY_SECTION_RE.search(line):
                break

            block_lines.append(line)

        block = "\n".join(block_lines)

        # Do not use fallback on unrelated sections.
        if not re.search(r"waste\s+generated|plastic\s+waste|e[-\s]?waste", block, re.I):
            continue

        line_candidates = []

        for line in block_lines:
            if "pdf_page" in line.lower():
                continue

            if WASTE_RECOVERY_SECTION_RE.search(line):
                break

            nums = extract_numbers(line, allow_percent=False)

            nums = [
                n for n in nums
                if n[1] not in {1, 2, 3, 4, 5, 6, 7, 8, 9}
            ]

            if not nums:
                continue

            current, previous = assign_current_previous(nums, line)
            unit = detect_unit(line, "waste") or detect_unit(block, "waste") or "tonnes"

            if current is None:
                continue

            if recycled_current is not None and current < recycled_current:
                continue

            # Prefer exact total formula if it appears.
            score = current

            if TOTAL_WASTE_FORMULA_RE.search(line):
                score += 10_000_000

            if TOTAL_WASTE_TEXT_RE.search(line):
                score += 5_000_000

            line_candidates.append({
                "score": score,
                "current": current,
                "previous": previous,
                "unit": unit,
                "raw_values": [x[0] for x in nums[:8]],
                "evidence": compact(line),
            })

        if line_candidates:
            line_candidates.sort(key=lambda x: x["score"], reverse=True)
            candidates.append(line_candidates[0])

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["score"], reverse=True)
    best = candidates[0]

    result = {
        "current": best["current"],
        "previous": best["previous"],
        "unit": best["unit"],
        "raw_values": best["raw_values"],
        "confidence": 90,
    }

    if INCLUDE_EVIDENCE:
        result["evidence"] = best["evidence"]

    return result


def extract_waste(text):
    """
    V3.1 waste extractor.

    Order changed:
    1. Extract recycled waste first.
    2. Use recycled value as a sanity check for total waste.
    3. Extract total waste using strict total row.
    4. If strict total fails, use max-value fallback inside waste generated table.
    """

    recycled = best_block_metric(
        text,
        label_patterns=[
            r"\(i\)\s*recycled\b",
            r"\bi\.\s*recycled\b",
            r"\brecycled\b",
            r"\bwaste\s+recycled\b",
            r"\brecycled\s+waste\b",
        ],
        kind="waste",
        exclude_primary=[
            r"water\s+recycled",
            r"input\s+material",
            r"plastic\s+packaging",
            r"percent",
            r"%",
            r"intensity",
        ],
        require_unit=False,
        exact_bonus_patterns=[
            r"\(i\)\s*recycled\b",
            r"\bi\.\s*recycled\b",
            r"\brecycled\b",
        ],
        min_value=0,
        reject_integer_serial=True,
        before=2,
        after=6,
    )

    if recycled and recycled.get("current") in {1, 2, 3, 4, 5, 6, 7, 8, 9}:
        recycled = None

    recycled_current = recycled.get("current") if recycled else None

    total_generated = extract_total_waste_generated_strict(
        text,
        recycled_current=recycled_current,
    )

    # Fallback if strict extraction failed or still produced impossible value.
    if (
        total_generated is None
        or (
            recycled_current is not None
            and total_generated.get("current") is not None
            and total_generated.get("current") < recycled_current
        )
    ):
        fallback_total = extract_total_waste_generated_fallback_max(
            text,
            recycled_current=recycled_current,
        )

        if fallback_total is not None:
            total_generated = fallback_total

    recycled_percent_direct = extract_percent_near_anchor(
        text,
        anchor_patterns=[
            r"\bwaste\s+recycled\b",
            r"\brecycled\s+waste\b",
            r"\brecycling\b",
        ],
        exclude_patterns=[
            r"water\s+recycled",
            r"input\s+material",
            r"female",
            r"women",
        ],
        max_value=100,
    )

    computed_percent = None

    if recycled and total_generated:
        r = recycled.get("current")
        g = total_generated.get("current")

        if r is not None and g not in [None, 0] and 0 <= r <= g * 1.2:
            computed_percent = r / g * 100.0

    final_percent = recycled_percent_direct["value"] if recycled_percent_direct else computed_percent

    if recycled:
        recycled["yoy_change_percent"] = pct_change(
            recycled.get("current"),
            recycled.get("previous"),
            reduction=False,
        )

    return {
        "total_waste_generated": total_generated,
        "waste_recycled": recycled,
        "waste_recycled_percent": final_percent,
        "waste_recycled_percent_direct": recycled_percent_direct,
        "waste_recycled_percent_computed": computed_percent,
    }
    
def extract_employee_diversity(text):
    female_employee_percent = extract_percent_near_anchor(
        text,
        anchor_patterns=[
            r"\bfemale\s+workforce\b",
            r"\bwomen\s+workforce\b",
            r"\bwomen\s+in\s+workforce\b",
            r"\bfemale\s+employees?\b",
            r"\bwomen\s+employees?\b",
            r"\bgender\s+diversity\b",
        ],
        exclude_patterns=[
            r"sexual\s+harassment",
            r"posh",
            r"complaints?",
            r"parental\s+leave",
            r"return\s+to\s+work",
            r"retention\s+rate",
            r"grievance",
            r"training",
        ],
        max_value=80,
        window=400,
    )

    women_on_board_percent = extract_percent_near_anchor(
        text,
        anchor_patterns=[
            r"\bwomen\s+directors?\b",
            r"\bfemale\s+directors?\b",
            r"\bwomen\s+on\s+board\b",
            r"\bgender\s+diversity\s+on\s+board\b",
        ],
        exclude_patterns=[
            r"sexual\s+harassment",
            r"posh",
            r"complaints?",
            r"training",
        ],
        max_value=80,
        window=400,
    )

    return {
        "female_employee_percent": female_employee_percent,
        "women_on_board_percent": women_on_board_percent,
    }

# =============================================================================
# ENERGY INTENSITY EXTRACTOR V5
# =============================================================================


def extract_energy_intensity(text):
    intensity = best_block_metric(
        text,
        label_patterns=[
            r"\benergy\s+intensity\s+per\s+rupee\s+of\s+turnover\b",
            r"\benergy\s+intensity\s+per\s+crore\s+of\s+turnover\b",
            r"\benergy\s+intensity\b",
            r"\benergy\s+consumption\s+per\b",
        ],
        kind=None,
        require_unit=False,
        exact_bonus_patterns=[
            r"\benergy\s+intensity\b",
        ],
        min_value=0,
        reject_integer_serial=True,
        before=1,
        after=5,
    )

    if not intensity:
        return None

    current = intensity.get("current")

    if not is_reasonable_energy_intensity(current):
        return None

    # reject absurdly large integers
    if current is not None and current > 1_000_000:
        return None

    intensity["yoy_reduction_percent"] = pct_change(
        intensity.get("current"),
        intensity.get("previous"),
        reduction=True,
    )

    return intensity

def split_sentences(text):
    flat = re.sub(r"\s+", " ", normalize_text(text))
    parts = re.split(r"(?<=[.!?])\s+|(?<=\])\s+", flat)

    return [p.strip() for p in parts if 40 <= len(p.strip()) <= 900]

# =============================================================================
# TARGET EXTRACTOR V5
# =============================================================================


def extract_targets(text, max_items=20):
    sentences = split_sentences(text)

    target_anchor = re.compile(
        r"\b(target|goal|commitment|committed|aim|plan|aspire|"
        r"net\s*zero|carbon\s+neutral|decarboni[sz]ation)\b",
        re.I,
    )

    topic_anchor = re.compile(
        r"\b(scope|emission|ghg|carbon|net\s*zero|renewable|energy|water|waste|"
        r"recycl|diversity|female|women)\b",
        re.I,
    )

    targets = []
    seen = set()

    for s in sentences:
        if not target_anchor.search(s):
            continue

        if not topic_anchor.search(s):
            continue

        years = [
            int(y)
            for y in re.findall(r"\b(20\d{2})\b", s)
        ]

        years = [
            y for y in years
            if MIN_REASONABLE_NET_ZERO_YEAR <= y <= MAX_REASONABLE_NET_ZERO_YEAR
        ]

        low = s.lower()

        topic = None

        if re.search(r"net\s*zero", low):
            topic = "net_zero"
        elif re.search(r"carbon\s+neutral", low):
            topic = "carbon_neutral"
        elif re.search(r"emission|ghg|carbon|scope", low):
            topic = "emissions"
        elif re.search(r"renewable|energy", low):
            topic = "energy"
        elif re.search(r"water", low):
            topic = "water"
        elif re.search(r"waste|recycl", low):
            topic = "waste"
        elif re.search(r"diversity|female|women", low):
            topic = "diversity"
        else:
            topic = "other"

        key = s.lower()[:180]

        if key in seen:
            continue

        seen.add(key)

        targets.append({
            "topic": topic,
            "target_year": years[0] if years else None,
            "evidence": compact(s, 800),
        })

        if len(targets) >= max_items:
            break

    net_zero_year = None

    for t in targets:
        if t["topic"] in {"net_zero", "carbon_neutral"}:
            y = t.get("target_year")

            if y and MIN_REASONABLE_NET_ZERO_YEAR <= y <= MAX_REASONABLE_NET_ZERO_YEAR:
                net_zero_year = y
                break

    return {
        "net_zero_target_year": net_zero_year,
        "targets": targets,
    }


def extract_yoy_reductions(text, max_items=15):
    sentences = split_sentences(text)

    yoy_anchor = re.compile(
        r"\b(yoy|year[-\s]?on[-\s]?year|compared\s+to\s+(?:previous\s+year|fy\s*20\d{2}|20\d{2})|"
        r"over\s+fy\s*20\d{2}|against\s+fy\s*20\d{2})\b",
        re.I,
    )

    topic_anchor = re.compile(
        r"\b(scope|emission|ghg|carbon|energy|renewable|water|waste|recycl|diesel|fuel)\b",
        re.I,
    )

    change_anchor = re.compile(
        r"\b(reduction|reduced|decrease|decreased|increase|increased|improvement|lower|higher)\b",
        re.I,
    )

    rows = []
    seen = set()

    for s in sentences:
        if not yoy_anchor.search(s):
            continue

        if not topic_anchor.search(s):
            continue

        if not change_anchor.search(s):
            continue

        percents = [float(x) for x in re.findall(r"(\d{1,3}(?:\.\d+)?)\s*%", s)]

        if not percents:
            continue

        key = s.lower()[:180]

        if key in seen:
            continue

        seen.add(key)

        direction = "reduction" if re.search(
            r"reduction|reduced|decrease|decreased|lower",
            s,
            flags=re.I,
        ) else "increase"

        rows.append({
            "change_percent": percents[0],
            "direction": direction,
            "evidence": compact(s, 800) if INCLUDE_EVIDENCE else None,
        })

        if len(rows) >= max_items:
            break

    return rows


# =============================================================================
# PROCESSING
# =============================================================================

def extract_all_kpis(text):
    return {
        "emissions": extract_emissions(text),
        "renewable_energy": extract_renewable_energy(text),
        "water": extract_water(text),
        "waste": extract_waste(text),
        "employee_diversity": extract_employee_diversity(text),
        "energy_intensity": extract_energy_intensity(text),
        "targets": extract_targets(text),
        "direct_yoy_reductions": extract_yoy_reductions(text),
    }

def get_path(obj, *keys):
    cur = obj

    for k in keys:
        if cur is None or not isinstance(cur, dict):
            return None

        cur = cur.get(k)

    return cur


def make_review_flags(flat):
    flags = []

    for scope in ["scope1", "scope2", "scope3"]:
        v = flat.get(f"{scope}_emissions_tco2e_current")
        if v in {1, 2, 3, 4, 5, 6, 7, 8, 9}:
            flags.append(f"{scope}_possible_serial_number")

    wr = flat.get("waste_recycled_current")
    wg = flat.get("total_waste_generated_current")

    if wr is not None and wg is not None and wr > wg * 1.2:
        flags.append("waste_recycled_greater_than_total_waste")

    fe = flat.get("female_employee_percent")
    if fe is not None and not (0 <= fe <= 80):
        flags.append("female_employee_percent_out_of_range")

    re_pct = flat.get("renewable_energy_percent")
    if re_pct is not None and not (0 <= re_pct <= 100):
        flags.append("renewable_energy_percent_out_of_range")

    ei = flat.get("energy_intensity_current")
    if ei in {1, 2, 3, 4, 5, 6, 7, 8, 9} and flat.get("energy_intensity_unit") is None:
        flags.append("energy_intensity_possible_serial_number")

    if flat.get("renewable_energy_review_flag"):
        flags.append(flat.get("renewable_energy_review_flag"))

    # renewable mismatch
    if flat.get("renewable_energy_review_flag"):
        flags.append(flat.get("renewable_energy_review_flag"))

# renewable > total
    r = flat.get("renewable_energy_consumption_gj")
    t = flat.get("total_energy_consumption_gj")

    if (
       r is not None
       and t not in [None, 0]
       and r > t
    ):
       flags.append("renewable_energy_greater_than_total_energy")

# absurd energy intensity
    energy_intensity = flat.get("energy_intensity_current")

    if (
       energy_intensity is not None
       and energy_intensity > MAX_REASONABLE_ENERGY_INTENSITY
    ):
       flags.append("energy_intensity_extreme_outlier")

# absurd total energy
    if (
        t is not None
        and t > MAX_REASONABLE_ENERGY_GJ
    ):
        flags.append("total_energy_extreme_outlier")

# suspicious net-zero year
    nzy = flat.get("net_zero_target_year")

    if (
        nzy is not None
        and nzy < MIN_REASONABLE_NET_ZERO_YEAR
    ):
        flags.append("net_zero_year_too_early")

    return flags


def flatten_record(record, kpis):
    emissions = kpis.get("emissions") or {}
    renewable = kpis.get("renewable_energy") or {}
    water = kpis.get("water") or {}
    waste = kpis.get("waste") or {}
    diversity = kpis.get("employee_diversity") or {}
    energy_intensity = kpis.get("energy_intensity")
    targets = kpis.get("targets") or {}
    direct_yoy = kpis.get("direct_yoy_reductions") or []

    scope1 = emissions.get("scope1")
    scope2 = emissions.get("scope2")
    scope3 = emissions.get("scope3")

    flat = {
        "file": record.get("file"),
        "filename": record.get("filename"),
        "company": record.get("company"),
        "reporting_year": record.get("reporting_year"),

        "scope1_emissions_tco2e_current": get_path(scope1, "current_tco2e"),
        "scope1_emissions_tco2e_previous": get_path(scope1, "previous_tco2e"),
        "scope1_emissions_yoy_reduction_percent": get_path(scope1, "yoy_reduction_percent"),

        "scope2_emissions_tco2e_current": get_path(scope2, "current_tco2e"),
        "scope2_emissions_tco2e_previous": get_path(scope2, "previous_tco2e"),
        "scope2_emissions_yoy_reduction_percent": get_path(scope2, "yoy_reduction_percent"),

        "scope3_emissions_tco2e_current": get_path(scope3, "current_tco2e"),
        "scope3_emissions_tco2e_previous": get_path(scope3, "previous_tco2e"),
        "scope3_emissions_yoy_reduction_percent": get_path(scope3, "yoy_reduction_percent"),

        "renewable_energy_percent": renewable.get("renewable_energy_percent"),
        "renewable_energy_percent_source": renewable.get("renewable_energy_percent_source"),
        "renewable_energy_review_flag": renewable.get("renewable_energy_review_flag"),
        "renewable_energy_consumption_gj": renewable.get("renewable_energy_consumption_gj"),
        "total_energy_consumption_gj": renewable.get("total_energy_consumption_gj"),
        
        "water_consumption_kl_current": get_path(water, "water_consumption", "current_kl"),
        "water_consumption_kl_previous": get_path(water, "water_consumption", "previous_kl"),
        "water_consumption_yoy_reduction_percent": get_path(water, "water_consumption", "yoy_reduction_percent"),

        "water_withdrawal_kl_current": get_path(water, "water_withdrawal", "current_kl"),
        "water_withdrawal_kl_previous": get_path(water, "water_withdrawal", "previous_kl"),
        "water_withdrawal_yoy_reduction_percent": get_path(water, "water_withdrawal", "yoy_reduction_percent"),

        "total_waste_generated_current": get_path(waste, "total_waste_generated", "current"),
        "total_waste_generated_previous": get_path(waste, "total_waste_generated", "previous"),

        "waste_recycled_current": get_path(waste, "waste_recycled", "current"),
        "waste_recycled_previous": get_path(waste, "waste_recycled", "previous"),
        "waste_recycled_unit": get_path(waste, "waste_recycled", "unit"),
        "waste_recycled_percent": waste.get("waste_recycled_percent"),

        "female_employee_percent": get_path(diversity, "female_employee_percent", "value"),
        "women_on_board_percent": get_path(diversity, "women_on_board_percent", "value"),

        "energy_intensity_current": get_path(energy_intensity, "current"),
        "energy_intensity_previous": get_path(energy_intensity, "previous"),
        "energy_intensity_unit": get_path(energy_intensity, "unit"),
        "energy_intensity_yoy_reduction_percent": get_path(energy_intensity, "yoy_reduction_percent"),

        "net_zero_target_year": targets.get("net_zero_target_year"),
        "targets_count": len(targets.get("targets") or []),
        "direct_yoy_reductions_count": len(direct_yoy),
    }

    s1 = flat["scope1_emissions_tco2e_current"]
    s2 = flat["scope2_emissions_tco2e_current"]

    flat["scope1_scope2_total_tco2e_current"] = s1 + s2 if s1 is not None and s2 is not None else None

    s1p = flat["scope1_emissions_tco2e_previous"]
    s2p = flat["scope2_emissions_tco2e_previous"]

    flat["scope1_scope2_total_tco2e_previous"] = s1p + s2p if s1p is not None and s2p is not None else None

    flat["scope1_scope2_yoy_reduction_percent"] = pct_change(
        flat["scope1_scope2_total_tco2e_current"],
        flat["scope1_scope2_total_tco2e_previous"],
        reduction=True,
    )

    if INCLUDE_EVIDENCE:
        flat["scope1_evidence"] = get_path(scope1, "evidence")
        flat["scope2_evidence"] = get_path(scope2, "evidence")
        flat["scope3_evidence"] = get_path(scope3, "evidence")

        flat["renewable_energy_evidence"] = (
            get_path(renewable, "renewable_energy_consumption", "evidence")
            or get_path(renewable, "renewable_energy_percent_direct", "evidence")
        )

        flat["water_consumption_evidence"] = get_path(water, "water_consumption", "evidence")
        flat["waste_recycled_evidence"] = get_path(waste, "waste_recycled", "evidence")
        flat["total_waste_generated_evidence"] = get_path(waste, "total_waste_generated", "evidence")
        flat["female_employee_evidence"] = get_path(diversity, "female_employee_percent", "evidence")
        flat["energy_intensity_evidence"] = get_path(energy_intensity, "evidence")

        flat["targets_json"] = json.dumps(targets.get("targets") or [], ensure_ascii=False)
        flat["direct_yoy_reductions_json"] = json.dumps(direct_yoy, ensure_ascii=False)

    flat["review_flags"] = ";".join(make_review_flags(flat))

    return flat


def process_record(record):
    try:
        text = normalize_text(record.get("esg_text", ""))

        kpis = extract_all_kpis(text)
        flat = flatten_record(record, kpis)

        return {
            "file": record.get("file"),
            "filename": record.get("filename"),
            "company": record.get("company"),
            "reporting_year": record.get("reporting_year"),
            "kpis": kpis,
            "flat_kpis": flat,
            "error": None,
        }

    except Exception:
        return {
            "file": record.get("file"),
            "filename": record.get("filename"),
            "company": record.get("company"),
            "reporting_year": record.get("reporting_year"),
            "kpis": {},
            "flat_kpis": {
                "file": record.get("file"),
                "filename": record.get("filename"),
                "company": record.get("company"),
                "reporting_year": record.get("reporting_year"),
            },
            "error": traceback.format_exc(),
        }

def load_jsonl(path):
    rows = []

    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()

            if not line:
                continue

            try:
                rows.append(json.loads(line))
            except Exception as e:
                print(f"Skipping invalid line {line_no}: {e}")

    return rows


# =============================================================================
# RUN
# =============================================================================

records = load_jsonl(INPUT_JSONL)

print("Records loaded:", len(records))
print("MAX_WORKERS:", MAX_WORKERS)
print("USE_PROCESS_POOL:", USE_PROCESS_POOL)

ExecutorClass = ProcessPoolExecutor if USE_PROCESS_POOL else ThreadPoolExecutor

results = []

with ExecutorClass(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(process_record, rec) for rec in records]

    for future in tqdm(as_completed(futures), total=len(futures), desc="Extracting KPI V3"):
        results.append(future.result())


# =============================================================================
# SAVE
# =============================================================================

with open(OUTPUT_JSONL, "w", encoding="utf-8") as f:
    for r in results:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")

flat_rows = [r.get("flat_kpis", {}) for r in results]
df = pd.DataFrame(flat_rows)

sort_cols = [c for c in ["company", "reporting_year", "filename"] if c in df.columns]

if sort_cols:
    df = df.sort_values(sort_cols, na_position="last").reset_index(drop=True)

df.to_csv(OUTPUT_CSV, index=False)

print("\nSaved CSV  :", OUTPUT_CSV)
print("Saved JSONL:", OUTPUT_JSONL)

display(df.head(20))


# =============================================================================
# QUALITY CHECK
# =============================================================================

required_cols = [
    "scope1_emissions_tco2e_current",
    "scope2_emissions_tco2e_current",
    "scope3_emissions_tco2e_current",
    "renewable_energy_percent",
    "water_consumption_kl_current",
    "waste_recycled_current",
    "total_waste_generated_current",
    "female_employee_percent",
    "energy_intensity_current",
    "net_zero_target_year",
]

for col in required_cols:
    if col not in df.columns:
        df[col] = None

df["kpi_values_found"] = df[required_cols].notna().sum(axis=1)

print("\nAverage KPI values found:", df["kpi_values_found"].mean())
print("Rows with review flags:", (df["review_flags"].fillna("") != "").sum())

display(
    df[
        [
            "company",
            "reporting_year",
            "kpi_values_found",
            "review_flags",
            "scope1_emissions_tco2e_current",
            "scope2_emissions_tco2e_current",
            "scope3_emissions_tco2e_current",
            "renewable_energy_percent",
            "water_consumption_kl_current",
            "waste_recycled_current",
            "total_waste_generated_current",
            "female_employee_percent",
            "energy_intensity_current",
            "net_zero_target_year",
        ]
    ].sort_values(["review_flags", "kpi_values_found"], ascending=[False, True]).head(50)
)