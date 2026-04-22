#to get the ESG section (json) from annual reports

import os
import re
import json
import pdfplumber
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# -------- CONFIG --------
PDF_FOLDER = "/kaggle/input/datasets/vaibhavmeena23/demo-dataset"
OUTPUT_FILE = "esg_dataset.json"
MAX_WORKERS = 4

# -------- TEXT EXTRACTION --------
def extract_text_from_pdf(pdf_path):
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    return text.lower()

# -------- CLEAN TEXT --------
def clean_text(text):
    if not text:
        return ""

    text = re.sub(r'[\x00-\x1F\x7F]', ' ', text)

    # Fix merging issues
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    text = re.sub(r'([a-zA-Z])(\d)', r'\1 \2', text)
    text = re.sub(r'(\d)([a-zA-Z])', r'\1 \2', text)

    text = re.sub(r'\s+', ' ', text)

    return text.strip()

# -------- ESG EXTRACTION --------
def find_esg_section(text):
    # ---- START PATTERNS (very broad) ----
    start_patterns = [
        r"environmental[, ]+social[, ]+and[, ]+governance",
        r"\besg\b",
        r"sustainability",
        r"business responsibility and sustainability report",
        r"\bbrsr\b",
        r"corporate social responsibility",
        r"\bcsr\b",
        r"environmental performance",
        r"social impact",
        r"governance report"
    ]

    matches = []

    # Find ALL ESG-related occurrences
    for pattern in start_patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            matches.append(match.start())

    if not matches:
        return None

    # Start from FIRST ESG mention
    start_idx = min(matches)

    # Take LARGE chunk (high recall)
    chunk = text[start_idx:]

    # ---- END PATTERNS (only strong financial signals) ----
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

    if end_idx:
        return chunk[:end_idx].strip()
    else:
        return chunk.strip()


# -------- PROCESS SINGLE PDF --------
def process_pdf(file):
    try:
        path = os.path.join(PDF_FOLDER, file)

        text = extract_text_from_pdf(path)
        esg_text = find_esg_section(text)

        if esg_text:
            esg_text = clean_text(esg_text)
        else:
            esg_text = "NOT FOUND"

        return {
            "file": file,
            "esg_text": esg_text
        }

    except Exception as e:
        return {
            "file": file,
            "esg_text": f"ERROR: {str(e)}"
        }


# -------- MAIN PIPELINE --------
def main():
    files = [f for f in os.listdir(PDF_FOLDER) if f.endswith(".pdf")]
    results = []

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_pdf, f) for f in files]

        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing ESG"):
            results.append(future.result())

    # Save JSON
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Done! Saved to {OUTPUT_FILE}")


# -------- RUN --------
if __name__ == "__main__":
    main()