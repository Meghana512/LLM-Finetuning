# MD&A EXTRACTION 
!pip install pymupdf


import os
import re
import json
import pdfplumber
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

# =========================================================
# CONFIG
# =========================================================

PDF_FOLDER = "/kaggle/input/datasets/meghanakadari/indian-listed-companies-brsr-reports-fy-202324/Indian_BRSR_Reports_2025-26/brsr reports 2025-26/files"

JSON_OUTPUT = "mda_dataset_25-26.json"

CSV_OUTPUT = "mda_eda_25-26.csv"

MAX_WORKERS = 16


# =========================================================
# TEXT EXTRACTION
# =========================================================

import fitz  # PyMuPDF

def extract_text_from_pdf(pdf_path):

    text = ""

    try:

        doc = fitz.open(pdf_path)

        for page in doc:

            text += page.get_text("text")

        doc.close()

    except Exception as e:

        print(f"Error reading {pdf_path}: {e}")

    return text.lower()


# =========================================================
# CLEAN TEXT
# =========================================================

def clean_text(text):

    if not text:
        return ""

    # Remove illegal/control characters
    text = re.sub(r'[\x00-\x1F\x7F]', ' ', text)

    # Fix merged words
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)

    text = re.sub(r'([a-zA-Z])(\d)', r'\1 \2', text)

    text = re.sub(r'(\d)([a-zA-Z])', r'\1 \2', text)

    # Normalize spaces
    text = re.sub(r'\s+', ' ', text)

    return text.strip()


# =========================================================
# MD&A EXTRACTION
# =========================================================

def find_mda_section(text):

    # -------- START PATTERNS --------

    start_patterns = [

        r"management\s+discussion\s+and\s+analysis",

        r"md\s*&\s*a",

        r"management.?s\s+discussion\s+and\s+analysis",

        r"operating\s+and\s+financial\s+review",

        r"management\s+report"
    ]

    start_idx = None

    for pattern in start_patterns:

        match = re.search(pattern, text, re.IGNORECASE)

        if match:

            start_idx = match.start()

            break

    # No MD&A found
    if start_idx is None:
        return None

    # Take everything after MD&A start
    chunk = text[start_idx:]

    # -------- END PATTERNS --------

    end_patterns = [

        r"standalone financial statements",

        r"consolidated financial statements",

        r"independent auditor",

        r"auditor.?s report",

        r"balance sheet"
    ]

    end_idx = None

    for pattern in end_patterns:

        match = re.search(pattern, chunk, re.IGNORECASE)

        if match:

            end_idx = match.start()

            break

    # Final MD&A section
    if end_idx:
        mda_text = chunk[:end_idx]
    else:
        mda_text = chunk

    return mda_text.strip()


# =========================================================
# PROCESS SINGLE PDF
# =========================================================

def process_pdf(file):

    try:

        path = os.path.join(PDF_FOLDER, file)

        # Extract full text
        text = extract_text_from_pdf(path)

        # Extract MD&A
        mda_text = find_mda_section(text)

        # MD&A exists
        if mda_text:

            mda_text = clean_text(mda_text)

            word_count = len(re.findall(r'\b\w+\b', mda_text))

            has_mda = 1

        # MD&A not found
        else:

            mda_text = "NOT FOUND"

            word_count = 0

            has_mda = 0

        return {

            "file": file,

            "has_mda": has_mda,

            "word_count": word_count,

            "mda_text": mda_text
        }

    except Exception as e:

        return {

            "file": file,

            "has_mda": 0,

            "word_count": 0,

            "mda_text": f"ERROR: {str(e)}"
        }


# =========================================================
# MAIN PIPELINE
# =========================================================

def main():

    # Get all PDFs
    files = [f for f in os.listdir(PDF_FOLDER) if f.endswith(".pdf")]

    results = []

    print(f"\nFound {len(files)} PDFs\n")

    # Parallel processing
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [executor.submit(process_pdf, f) for f in files]

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Processing MD&A Reports"
        ):

            results.append(future.result())

    # =====================================================
    # SAVE FULL JSON DATASET
    # =====================================================

    with open(JSON_OUTPUT, "w", encoding="utf-8") as f:

        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\n✅ MD&A JSON saved: {JSON_OUTPUT}")

    # =====================================================
    # CREATE EDA CSV
    # =====================================================

    df = pd.DataFrame([

        {
            "file": r["file"],

            "has_mda": r["has_mda"],

            "word_count": r["word_count"]
        }

        for r in results
    ])

    # Save CSV
    df.to_csv(CSV_OUTPUT, index=False)

    print(f"✅ MD&A EDA CSV saved: {CSV_OUTPUT}")

# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":
    main()