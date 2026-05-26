#final esg classification + intent labelling from esg dataset jsonl
# =============================================================================
# ESG SECTION CLASSIFICATION + NARRATIVE INTENT LABELLING
# Kaggle Notebook Version
# =============================================================================
# Input : ESG dataset JSONL with `esg_text`
# Output:
#   1. esg_section_classification.csv
#   2. esg_section_classification.jsonl
#   3. esg_paragraph_intents.csv
#   4. esg_paragraph_intents.jsonl
#
# This script does NOT do KPI extraction.
# It prepares PRD Phase 1B and 1D outputs.
# =============================================================================

import os
import re
import csv
import json
import traceback
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

import pandas as pd
from tqdm.auto import tqdm


# =============================================================================
# CONFIG
# =============================================================================

INPUT_JSONL = "/kaggle/input/datasets/vaibhavmeena23/esg-dataset-v3-jsonl/esg_dataset_24-25_v3.jsonl"

OUTPUT_DIR = "/kaggle/working/esg_classification_intent"

SECTION_CSV = f"{OUTPUT_DIR}/esg_section_classification_24-25.csv"
SECTION_JSONL = f"{OUTPUT_DIR}/esg_section_classification_24-25.jsonl"

PARAGRAPH_CSV = f"{OUTPUT_DIR}/esg_paragraph_intents_24-25.csv"
PARAGRAPH_JSONL = f"{OUTPUT_DIR}/esg_paragraph_intents_24-25.jsonl"

MAX_WORKERS = 8
USE_PROCESS_POOL = True

TEXT_FIELD = "esg_text"

MIN_PARAGRAPH_CHARS = 80
MAX_PARAGRAPH_CHARS = 1800

# Narrative labelling is meant for paragraphs, not pure numeric table rows.
SKIP_TABLE_LIKE_PARAGRAPHS = True

# Useful for debugging section classification.
INCLUDE_SECTION_EVIDENCE = True
MAX_EVIDENCE_PER_SECTION = 3

os.makedirs(OUTPUT_DIR, exist_ok=True)


# =============================================================================
# NORMALIZATION
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


def clean_search_text(text):
    text = normalize_text(text).lower()
    text = re.sub(r"\s+", " ", text)
    return text


def compact(text, n=700):
    return re.sub(r"\s+", " ", text or "").strip()[:n]


def safe_json(obj):
    return json.dumps(obj, ensure_ascii=False)


def sanitize_col(name):
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name


# =============================================================================
# PRD ESG SECTION PATTERNS
# =============================================================================

SECTION_PATTERNS = {
    "Environmental": [
        r"\benvironment\b",
        r"\benvironmental\b",
        r"\bpollution\b",
        r"\bemissions?\b",
        r"\benergy\b",
        r"\bwater\b",
        r"\bwaste\b",
        r"\bclimate\b",
        r"\bcarbon\b",
        r"\brenewable\b",
        r"\bbiodiversity\b",
        r"\bnatural\s+capital\b",
    ],
    "Social": [
        r"\bsocial\b",
        r"\bemployees?\b",
        r"\bworkers?\b",
        r"\bhealth\s+and\s+safety\b",
        r"\boccupational\s+health\b",
        r"\btraining\b",
        r"\bdiversity\b",
        r"\bhuman\s+rights\b",
        r"\bcommunity\b",
        r"\bstakeholders?\b",
        r"\bcustomer\s+satisfaction\b",
    ],
    "Governance": [
        r"\bgovernance\b",
        r"\bboard\b",
        r"\bcommittee\b",
        r"\bethics\b",
        r"\bcompliance\b",
        r"\banti[-\s]?corruption\b",
        r"\banti[-\s]?bribery\b",
        r"\bvigil\s+mechanism\b",
        r"\bwhistle\s*blower\b",
    ],
    "Climate Risk": [
        r"\bclimate\s+risk\b",
        r"\bclimate[-\s]?related\s+risk\b",
        r"\btransition\s+risk\b",
        r"\bphysical\s+risk\b",
        r"\bscenario\s+analysis\b",
        r"\bclimate\s+resilience\b",
        r"\bcarbon\s+pricing\b",
    ],
    "Net Zero": [
        r"\bnet\s*zero\b",
        r"\bcarbon\s+neutral\b",
        r"\bcarbon\s+neutrality\b",
        r"\bdecarboni[sz]ation\b",
        r"\bneutralisation\b",
        r"\bnet\s+zero\s+target\b",
    ],
    "Energy": [
        r"\benergy\s+consumption\b",
        r"\benergy\s+consumed\b",
        r"\benergy\s+intensity\b",
        r"\brenewable\s+energy\b",
        r"\bnon[-\s]?renewable\s+energy\b",
        r"\belectricity\b",
        r"\bfuel\s+consumption\b",
        r"\bsolar\b",
        r"\bwind\b",
        r"\bgj\b",
        r"\bkwh\b",
    ],
    "Water": [
        r"\bwater\s+withdrawal\b",
        r"\bwater\s+consumption\b",
        r"\bwater\s+discharge\b",
        r"\bwater\s+recycled\b",
        r"\bwater\s+reused\b",
        r"\bwater\s+stress\b",
        r"\bzero\s+liquid\s+discharge\b",
        r"\bzld\b",
    ],
    "Waste": [
        r"\bwaste\s+generated\b",
        r"\bwaste\s+recycled\b",
        r"\bwaste\s+disposed\b",
        r"\bwaste\s+management\b",
        r"\bhazardous\s+waste\b",
        r"\bnon[-\s]?hazardous\s+waste\b",
        r"\bplastic\s+waste\b",
        r"\be[-\s]?waste\b",
    ],
    "Scope 1": [
        r"\bscope\s*1\b",
        r"\bscope\s+i\b",
        r"\bdirect\s+ghg\s+emissions?\b",
        r"\bdirect\s+emissions?\b",
    ],
    "Scope 2": [
        r"\bscope\s*2\b",
        r"\bscope\s+ii\b",
        r"\bindirect\s+ghg\s+emissions?\b",
        r"\benergy\s+indirect\s+emissions?\b",
    ],
    "Scope 3": [
        r"\bscope\s*3\b",
        r"\bscope\s+iii\b",
        r"\bvalue\s+chain\s+emissions?\b",
        r"\bother\s+indirect\s+emissions?\b",
    ],
    "Diversity": [
        r"\bdiversity\b",
        r"\bgender\s+diversity\b",
        r"\bwomen\b",
        r"\bfemale\b",
        r"\bgender\b",
        r"\bdifferently\s+abled\b",
        r"\bpersons?\s+with\s+disabilities\b",
        r"\bpwd\b",
    ],
    "Human Rights": [
        r"\bhuman\s+rights\b",
        r"\bchild\s+labou?r\b",
        r"\bforced\s+labou?r\b",
        r"\bminimum\s+wages?\b",
        r"\bsexual\s+harassment\b",
        r"\bposh\b",
        r"\bfreedom\s+of\s+association\b",
    ],
    "CSR": [
        r"\bcsr\b",
        r"\bcorporate\s+social\s+responsibility\b",
        r"\bcommunity\s+development\b",
        r"\bcommunity\s+investment\b",
        r"\bcsr\s+committee\b",
    ],
    "Supply Chain": [
        r"\bsupply\s+chain\b",
        r"\bsuppliers?\b",
        r"\bvendors?\b",
        r"\bprocurement\b",
        r"\bsustainable\s+sourcing\b",
        r"\bresponsible\s+sourcing\b",
        r"\bvalue\s+chain\b",
    ],
    "Board Governance": [
        r"\bboard\s+of\s+directors\b",
        r"\bboard\s+composition\b",
        r"\bindependent\s+directors?\b",
        r"\bboard\s+committee\b",
        r"\baudit\s+committee\b",
        r"\brisk\s+management\s+committee\b",
    ],
    "TCFD": [
        r"\btcfd\b",
        r"\btask\s+force\s+on\s+climate[-\s]?related\s+financial\s+disclosures\b",
    ],
    "IFRS S1/S2": [
        r"\bifrs\s*s1\b",
        r"\bifrs\s*s2\b",
        r"\bissb\b",
        r"\binternational\s+sustainability\s+standards\s+board\b",
    ],
    "CDP": [
        r"\bcdp\b",
        r"\bcarbon\s+disclosure\s+project\b",
    ],
}


# =============================================================================
# NARRATIVE INTENT PATTERNS
# =============================================================================

INTENT_PATTERNS = {
    "compliance disclosure": {
        "patterns": [
            r"\bdisclosed?\b",
            r"\breported?\b",
            r"\bas\s+required\b",
            r"\bin\s+accordance\s+with\b",
            r"\bsebi\b",
            r"\bbrsr\b",
            r"\bbusiness\s+responsibility\b",
            r"\bcompanies\s+act\b",
            r"\bregulation\b",
            r"\bstatutory\b",
            r"\bcompliance\b",
            r"\bno\s+complaints?\b",
            r"\bnil\b",
            r"\bnot\s+applicable\b",
            r"\bassurance\b",
            r"\bassured\b",
        ],
        "weight": 1.0,
    },
    "marketing narrative": {
        "patterns": [
            r"\bwe\s+believe\b",
            r"\bwe\s+are\s+proud\b",
            r"\bcommitted\s+to\s+sustainability\b",
            r"\bstrong\s+commitment\b",
            r"\bcontinuous(?:ly)?\s+strive\b",
            r"\bbest[-\s]?in[-\s]?class\b",
            r"\bleading\b",
            r"\bexcellence\b",
            r"\bholistic\b",
            r"\brobust\b",
            r"\btransformative\b",
            r"\bjourney\b",
            r"\bvalue\s+creation\b",
        ],
        "weight": 0.8,
    },
    "risk disclosure": {
        "patterns": [
            r"\brisk\b",
            r"\brisks\b",
            r"\bimpact\b",
            r"\bimpacts\b",
            r"\bexposure\b",
            r"\bvulnerability\b",
            r"\bmitigation\b",
            r"\bphysical\s+risk\b",
            r"\btransition\s+risk\b",
            r"\bclimate\s+risk\b",
            r"\bmaterial\s+risk\b",
            r"\brisk\s+management\b",
            r"\brisk\s+assessment\b",
        ],
        "weight": 1.2,
    },
    "target commitment": {
        "patterns": [
            r"\btarget\b",
            r"\btargets\b",
            r"\bgoal\b",
            r"\bgoals\b",
            r"\bcommitment\b",
            r"\bcommitted\s+to\b",
            r"\bnet\s*zero\b",
            r"\bcarbon\s+neutral\b",
            r"\breduce\s+.*\bby\s+20\d{2}\b",
            r"\breduction\s+target\b",
            r"\bby\s+20\d{2}\b",
            r"\bachieve\s+.*\b20\d{2}\b",
            r"\bscience[-\s]?based\s+target\b",
        ],
        "weight": 1.4,
    },
    "operational initiative": {
        "patterns": [
            r"\bimplemented\b",
            r"\binstalled\b",
            r"\bcommissioned\b",
            r"\badopted\b",
            r"\bundertaken\b",
            r"\binitiative\b",
            r"\binitiatives\b",
            r"\bproject\b",
            r"\bprogramme\b",
            r"\bprogram\b",
            r"\bsolar\s+plant\b",
            r"\brainwater\s+harvesting\b",
            r"\brecycled\b",
            r"\breused\b",
            r"\bsaved\b",
            r"\breduced\b",
            r"\btraining\s+conducted\b",
            r"\bawareness\s+session\b",
        ],
        "weight": 1.1,
    },
    "governance explanation": {
        "patterns": [
            r"\bboard\b",
            r"\bcommittee\b",
            r"\bgovernance\b",
            r"\boversight\b",
            r"\bmanagement\s+approach\b",
            r"\bresponsibility\s+of\b",
            r"\bresponsible\s+for\b",
            r"\bpolicy\s+approved\b",
            r"\bcode\s+of\s+conduct\b",
            r"\bethics\b",
            r"\bwhistle\s*blower\b",
            r"\bvigil\s+mechanism\b",
            r"\banti[-\s]?bribery\b",
            r"\banti[-\s]?corruption\b",
        ],
        "weight": 1.2,
    },
    "policy statement": {
        "patterns": [
            r"\bpolicy\b",
            r"\bpolicies\b",
            r"\bcode\b",
            r"\bframework\b",
            r"\bguidelines?\b",
            r"\bstandard\b",
            r"\bprocedure\b",
            r"\bapplicable\s+to\b",
            r"\bapplies\s+to\b",
            r"\bzero\s+tolerance\b",
            r"\bshall\s+comply\b",
            r"\bprinciples?\b",
        ],
        "weight": 1.0,
    },
    "forward-looking statement": {
        "patterns": [
            r"\bwill\b",
            r"\bshall\b",
            r"\bplans?\s+to\b",
            r"\bintend(?:s|ed)?\s+to\b",
            r"\bexpects?\s+to\b",
            r"\baims?\s+to\b",
            r"\bgoing\s+forward\b",
            r"\bin\s+future\b",
            r"\bfuture\s+plans?\b",
            r"\broadmap\b",
            r"\bnext\s+year\b",
            r"\bupcoming\b",
        ],
        "weight": 0.9,
    },
}


INTENT_PRIORITY = [
    "target commitment",
    "risk disclosure",
    "governance explanation",
    "policy statement",
    "operational initiative",
    "compliance disclosure",
    "forward-looking statement",
    "marketing narrative",
]


# =============================================================================
# SECTION CLASSIFICATION
# =============================================================================

def count_pattern_matches(text, patterns):
    t = clean_search_text(text)
    count = 0

    for p in patterns:
        count += len(re.findall(p, t, flags=re.I))

    return count


def extract_section_evidence(text, patterns, max_items=3):
    if not INCLUDE_SECTION_EVIDENCE:
        return []

    flat = re.sub(r"\s+", " ", normalize_text(text))
    evidence = []

    for p in patterns:
        for m in re.finditer(p, flat, flags=re.I):
            start = max(0, m.start() - 180)
            end = min(len(flat), m.end() + 250)
            snippet = compact(flat[start:end], 450)

            if snippet and snippet not in evidence:
                evidence.append(snippet)

            if len(evidence) >= max_items:
                return evidence

    return evidence


def classify_sections(text):
    scores = {}
    presence = {}
    evidence = {}

    for section, patterns in SECTION_PATTERNS.items():
        count = count_pattern_matches(text, patterns)

        # Strong sections should be present even with 1 hit.
        threshold = 1

        presence[section] = count >= threshold
        scores[section] = count

        if presence[section]:
            evidence[section] = extract_section_evidence(
                text,
                patterns,
                max_items=MAX_EVIDENCE_PER_SECTION,
            )
        else:
            evidence[section] = []

    top_sections = sorted(
        [s for s, has in presence.items() if has],
        key=lambda s: scores[s],
        reverse=True,
    )

    return {
        "presence": presence,
        "scores": scores,
        "top_sections": top_sections,
        "evidence": evidence,
    }


def classify_paragraph_sections(paragraph):
    result = classify_sections(paragraph)

    labels = [
        s for s in result["top_sections"]
        if result["scores"].get(s, 0) > 0
    ]

    return labels, result["scores"]


# =============================================================================
# INTENT CLASSIFICATION
# =============================================================================

def classify_intent(paragraph):
    text = clean_search_text(paragraph)

    scores = {}

    for label, cfg in INTENT_PATTERNS.items():
        score = 0.0

        for p in cfg["patterns"]:
            matches = re.findall(p, text, flags=re.I)
            score += len(matches) * cfg["weight"]

        scores[label] = score

    # Extra contextual boosts
    if re.search(r"\bby\s+20\d{2}\b|\bnet\s*zero\b|\btarget\b", text):
        scores["target commitment"] += 2.0

    if re.search(r"\brisk\b|\bphysical\s+risk\b|\btransition\s+risk\b", text):
        scores["risk disclosure"] += 1.5

    if re.search(r"\bboard\b|\bcommittee\b|\bgovernance\b", text):
        scores["governance explanation"] += 1.0

    if re.search(r"\bpolicy\b|\bcode\s+of\s+conduct\b|\bframework\b", text):
        scores["policy statement"] += 1.0

    if re.search(r"\binstalled\b|\bimplemented\b|\binitiative\b|\bproject\b", text):
        scores["operational initiative"] += 1.0

    if re.search(r"\bsebi\b|\bbrsr\b|\bin\s+accordance\s+with\b|\bdisclosed\b", text):
        scores["compliance disclosure"] += 1.2

    if re.search(r"\bwill\b|\bplans?\s+to\b|\bgoing\s+forward\b", text):
        scores["forward-looking statement"] += 0.8

    best_score = max(scores.values()) if scores else 0

    if best_score <= 0:
        return {
            "primary_intent": "unclassified",
            "primary_intent_score": 0,
            "intent_scores": scores,
        }

    candidates = [
        label for label, score in scores.items()
        if score == best_score
    ]

    # Tie-break using PRD-friendly priority.
    primary = None

    for label in INTENT_PRIORITY:
        if label in candidates:
            primary = label
            break

    if primary is None:
        primary = candidates[0]

    return {
        "primary_intent": primary,
        "primary_intent_score": best_score,
        "intent_scores": scores,
    }


# =============================================================================
# PARAGRAPH SPLITTING
# =============================================================================

PAGE_MARKER_RE = re.compile(
    r"=+\s*PDF_PAGE\s+(\d+).*?=+",
    re.I,
)


def split_text_by_pages(text):
    text = normalize_text(text)

    matches = list(PAGE_MARKER_RE.finditer(text))

    if not matches:
        return [(None, text)]

    blocks = []

    for i, m in enumerate(matches):
        page_no = int(m.group(1))
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)

        page_text = text[start:end].strip()

        if page_text:
            blocks.append((page_no, page_text))

    return blocks


def is_table_like(paragraph):
    if not paragraph:
        return True

    p = paragraph.strip()

    if len(p) < MIN_PARAGRAPH_CHARS:
        return True

    pipe_count = p.count("|")
    digit_count = sum(ch.isdigit() for ch in p)
    alpha_count = sum(ch.isalpha() for ch in p)

    if pipe_count >= 5:
        return True

    if digit_count > alpha_count and len(p) < 800:
        return True

    if re.search(r"\[TABLE\s+\d+\s+START\]|\[TABLE\s+\d+\s+END\]", p, flags=re.I):
        return True

    return False


def split_long_paragraph(paragraph, max_chars=MAX_PARAGRAPH_CHARS):
    paragraph = paragraph.strip()

    if len(paragraph) <= max_chars:
        return [paragraph]

    sentences = re.split(r"(?<=[.!?])\s+", paragraph)

    chunks = []
    current = ""

    for sent in sentences:
        sent = sent.strip()

        if not sent:
            continue

        if len(current) + len(sent) + 1 <= max_chars:
            current = current + " " + sent if current else sent
        else:
            if current:
                chunks.append(current)

            current = sent

    if current:
        chunks.append(current)

    return chunks


def extract_paragraphs(text):
    page_blocks = split_text_by_pages(text)
    records = []

    for page_no, page_text in page_blocks:
        raw_parts = re.split(r"\n\s*\n+", page_text)

        for part in raw_parts:
            part = normalize_text(part)

            if not part:
                continue

            for para in split_long_paragraph(part):
                para = normalize_text(para)

                if len(para) < MIN_PARAGRAPH_CHARS:
                    continue

                if SKIP_TABLE_LIKE_PARAGRAPHS and is_table_like(para):
                    continue

                records.append({
                    "page": page_no,
                    "text": para,
                })

    return records


# =============================================================================
# PROCESS ONE RECORD
# =============================================================================

def process_record(record):
    try:
        text = normalize_text(record.get(TEXT_FIELD, ""))

        section_result = classify_sections(text)
        paragraph_items = extract_paragraphs(text)

        file = record.get("file")
        filename = record.get("filename")
        company = record.get("company")
        reporting_year = record.get("reporting_year") or record.get("year")

        section_flat = {
            "file": file,
            "filename": filename,
            "company": company,
            "reporting_year": reporting_year,
            "esg_text_chars": len(text),
            "paragraph_count": len(paragraph_items),
            "top_sections": ", ".join(section_result["top_sections"]),
            "section_scores_json": safe_json(section_result["scores"]),
            "section_evidence_json": safe_json(section_result["evidence"]),
            "error": None,
        }

        for section in SECTION_PATTERNS:
            col = sanitize_col(section)
            section_flat[f"has_{col}"] = bool(section_result["presence"].get(section))
            section_flat[f"score_{col}"] = int(section_result["scores"].get(section, 0))

        section_json = {
            "file": file,
            "filename": filename,
            "company": company,
            "reporting_year": reporting_year,
            "esg_text_chars": len(text),
            "paragraph_count": len(paragraph_items),
            "section_presence": section_result["presence"],
            "section_scores": section_result["scores"],
            "top_sections": section_result["top_sections"],
            "section_evidence": section_result["evidence"],
            "error": None,
        }

        paragraph_rows = []

        for idx, para_obj in enumerate(paragraph_items, start=1):
            para = para_obj["text"]

            intent = classify_intent(para)
            section_labels, para_section_scores = classify_paragraph_sections(para)

            row = {
                "file": file,
                "filename": filename,
                "company": company,
                "reporting_year": reporting_year,
                "paragraph_id": idx,
                "page": para_obj["page"],
                "primary_intent": intent["primary_intent"],
                "primary_intent_score": intent["primary_intent_score"],
                "section_labels": ", ".join(section_labels),
                "paragraph_chars": len(para),
                "paragraph_text": para,
                "intent_scores_json": safe_json(intent["intent_scores"]),
                "section_scores_json": safe_json(para_section_scores),
            }

            paragraph_rows.append(row)

        return section_flat, section_json, paragraph_rows

    except Exception:
        file = record.get("file")
        filename = record.get("filename")
        company = record.get("company")
        reporting_year = record.get("reporting_year") or record.get("year")

        section_flat = {
            "file": file,
            "filename": filename,
            "company": company,
            "reporting_year": reporting_year,
            "esg_text_chars": 0,
            "paragraph_count": 0,
            "top_sections": "",
            "section_scores_json": "{}",
            "section_evidence_json": "{}",
            "error": traceback.format_exc(),
        }

        for section in SECTION_PATTERNS:
            col = sanitize_col(section)
            section_flat[f"has_{col}"] = False
            section_flat[f"score_{col}"] = 0

        section_json = {
            "file": file,
            "filename": filename,
            "company": company,
            "reporting_year": reporting_year,
            "section_presence": {},
            "section_scores": {},
            "top_sections": [],
            "section_evidence": {},
            "error": traceback.format_exc(),
        }

        return section_flat, section_json, []


# =============================================================================
# LOAD JSONL
# =============================================================================

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


records = load_jsonl(INPUT_JSONL)

print("Records loaded:", len(records))
print("MAX_WORKERS:", MAX_WORKERS)
print("USE_PROCESS_POOL:", USE_PROCESS_POOL)


# =============================================================================
# OUTPUT FIELDNAMES
# =============================================================================

SECTION_FIELDNAMES = [
    "file",
    "filename",
    "company",
    "reporting_year",
    "esg_text_chars",
    "paragraph_count",
    "top_sections",
    "section_scores_json",
    "section_evidence_json",
    "error",
]

for section in SECTION_PATTERNS:
    col = sanitize_col(section)
    SECTION_FIELDNAMES.append(f"has_{col}")
    SECTION_FIELDNAMES.append(f"score_{col}")


PARAGRAPH_FIELDNAMES = [
    "file",
    "filename",
    "company",
    "reporting_year",
    "paragraph_id",
    "page",
    "primary_intent",
    "primary_intent_score",
    "section_labels",
    "paragraph_chars",
    "paragraph_text",
    "intent_scores_json",
    "section_scores_json",
]


# =============================================================================
# RUN PARALLEL PROCESSING
# =============================================================================

ExecutorClass = ProcessPoolExecutor if USE_PROCESS_POOL else ThreadPoolExecutor

section_rows = []
total_paragraphs = 0
errors = 0

with open(SECTION_JSONL, "w", encoding="utf-8") as section_jsonl_f, \
     open(PARAGRAPH_JSONL, "w", encoding="utf-8") as para_jsonl_f, \
     open(PARAGRAPH_CSV, "w", encoding="utf-8", newline="") as para_csv_f:

    para_writer = csv.DictWriter(
        para_csv_f,
        fieldnames=PARAGRAPH_FIELDNAMES,
        extrasaction="ignore",
    )
    para_writer.writeheader()

    with ExecutorClass(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_record, rec) for rec in records]

        for future in tqdm(as_completed(futures), total=len(futures), desc="Classifying ESG sections + intents"):
            section_flat, section_json, paragraph_rows = future.result()

            section_rows.append(section_flat)
            section_jsonl_f.write(json.dumps(section_json, ensure_ascii=False) + "\n")

            if section_flat.get("error"):
                errors += 1

            for row in paragraph_rows:
                para_writer.writerow(row)
                para_jsonl_f.write(json.dumps(row, ensure_ascii=False) + "\n")

            total_paragraphs += len(paragraph_rows)


# Save section CSV at the end
section_df = pd.DataFrame(section_rows)

sort_cols = [c for c in ["company", "reporting_year", "filename"] if c in section_df.columns]

if sort_cols:
    section_df = section_df.sort_values(sort_cols, na_position="last").reset_index(drop=True)

section_df.to_csv(SECTION_CSV, index=False)


print("\nDONE")
print("Section CSV   :", SECTION_CSV)
print("Section JSONL :", SECTION_JSONL)
print("Intent CSV    :", PARAGRAPH_CSV)
print("Intent JSONL  :", PARAGRAPH_JSONL)
print("Reports       :", len(section_rows))
print("Paragraphs    :", total_paragraphs)
print("Errors        :", errors)

display(section_df.head(20))