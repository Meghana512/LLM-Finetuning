import os
import re
import json
import pdfplumber
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# -------- CONFIG --------
PDF_FOLDER = "/kaggle/input/datasets/vaibhavmeena23/demo-dataset"
OUTPUT_FILE = "mda_dataset.json"
MAX_WORKERS = 4   # adjust based on CPU (4–8 is safe)

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

# -------- MD&A EXTRACTION --------
def find_mda_section(text):
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

    if start_idx is None:
        return None

    chunk = text[start_idx:]

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
        mda_text = find_mda_section(text)

        if mda_text:
            mda_text = clean_text(mda_text)
        else:
            mda_text = "NOT FOUND"

        return {
            "file": file,
            "mda_text": mda_text
        }

    except Exception as e:
        return {
            "file": file,
            "mda_text": f"ERROR: {str(e)}"
        }

# -------- MAIN PIPELINE --------
def main():
    files = [f for f in os.listdir(PDF_FOLDER) if f.endswith(".pdf")]
    results = []

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_pdf, f) for f in files]

        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing PDFs"):
            results.append(future.result())

    # Save JSON
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Done! Saved to {OUTPUT_FILE}")

# -------- RUN --------
if __name__ == "__main__":
    main()