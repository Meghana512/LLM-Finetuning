# ESG METADATA EXTRACTION PIPELINE FOR BRSR 25-26 (DIFFERENT FROM 22-23 AND 24-25)
# =========================================================
# Key fix vs v4:
#   NSE matching now uses a multi-strategy approach so it
#   works regardless of how the PDF filenames are formatted
#   in any dataset (24-25, 25-26, or future years).
#
#   Matching pipeline per filename:
#     Pass 1 — token_sort_ratio   >= threshold  (word-order insensitive)
#     Pass 2 — partial_ratio      >= threshold  (substring match)
#     Pass 3 — token_set_ratio    >= threshold  (handles extra words)
#     Pass 4 — WRatio             >= threshold  (weighted combo)
#     Pass 5 — lower threshold (55) with WRatio (last resort)
#
#   Normalisation is also more aggressive:
#     strips report-suffix noise like "BRSR", "Annual Report",
#     year tokens (2024, 25-26, FY), underscores, hyphens.
# =========================================================

!pip install pymupdf pandas tqdm yfinance rapidfuzz requests -q

import os, re, gc, json, warnings
import fitz
import pandas as pd
import yfinance as yf
import requests

from io import StringIO
from tqdm import tqdm
from rapidfuzz import process as fuzz_process, fuzz as rfuzz
from multiprocessing import Pool, cpu_count
from functools import lru_cache

warnings.filterwarnings("ignore")

# =========================================================
# CONFIG  — add all folder paths you want to process
# =========================================================

REPORTS_FOLDERS = [
    "/kaggle/input/datasets/meghanakadari/indian-companies-annual-reports-fy-202223/Annual_Report_23/annual_reports_2023",
    # Add your 25-26 folder path here:
    # "/kaggle/input/YOUR_2526_DATASET/BRSR_Reports_2025_26",
]

OUTPUT_CSV   = "metadata_dataset_combined.csv"
OUTPUT_JSONL = "metadata_dataset_combined.jsonl"

MAX_WORKERS      = 16
MAX_PAGES        = 50
MC_MAX_PAGES     = 20

THRESHOLD_HIGH   = 72    # passes 1-4 (reliable matches)
THRESHOLD_LOW    = 55    # pass 5   (last resort, may have false positives)

# =========================================================
# NSE COMPANY LIST
# =========================================================

NSE_EQUITY_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

_NAME_TO_SYM : dict = {}
_NSE_NAMES   : list = []

# Noise tokens present in report filenames but not in NSE company names
_FILENAME_NOISE = [
    r'\bbrsr\b',
    r'\bannual\b',
    r'\breport\b',
    r'\bsustainability\b',
    r'\besg\b',
    r'\bfy\b',
    r'\b20\d{2}\b',           # year: 2024, 2025, 2026
    r'\b\d{2}[-_]\d{2}\b',    # range: 24-25, 25-26
    r'\b\d{4}[-_]\d{2,4}\b',  # 2024-25, 2024-2025
]

_LEGAL_SUFFIXES = [
    r'\blimited\b', r'\bltd\.?\b', r'\bprivate\b', r'\bpvt\.?\b',
    r'\bcorporation\b', r'\bcorp\.?\b', r'\benterprises\b',
    r'\bindustries\b', r'\bgroup\b', r'\bindian\b', r'\bcompany\b',
    r'\bco\.?\b',
]


def _normalise_nse(name: str) -> str:
    name = name.lower()
    for pat in _LEGAL_SUFFIXES:
        name = re.sub(pat, '', name)
    name = re.sub(r'[^a-z0-9\s]', ' ', name)
    return re.sub(r'\s+', ' ', name).strip()


def _normalise_filename(filename: str) -> str:
    """
    More aggressive than _normalise_nse — also strips
    report-type noise, year tokens, and separators so that
    filenames like:
      "Infosys_BRSR_2025-26"
      "INFOSYS-Annual-Report-FY25-26"
      "Infosys Limited BRSR Report 25-26"
    all reduce to: "infosys"
    """
    name = filename.lower()
    name = re.sub(r'[_\-]+', ' ', name)          # separators -> spaces
    for pat in _FILENAME_NOISE:
        name = re.sub(pat, '', name)
    for pat in _LEGAL_SUFFIXES:
        name = re.sub(pat, '', name)
    name = re.sub(r'[^a-z0-9\s]', ' ', name)
    return re.sub(r'\s+', ' ', name).strip()


def _load_nse_list():
    global _NAME_TO_SYM, _NSE_NAMES
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",
    }
    try:
        print("Downloading NSE equity list ...", end=" ", flush=True)
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        resp = session.get(NSE_EQUITY_URL, headers=headers, timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text))
        df.columns = [c.strip().upper() for c in df.columns]
        name_col = next(c for c in df.columns if "NAME" in c)
        sym_col  = next(c for c in df.columns if "SYMBOL" in c)
        df = df[[sym_col, name_col]].dropna()
        df.columns = ["SYMBOL", "NAME"]
        for _, row in df.iterrows():
            key = _normalise_nse(str(row["NAME"]))
            _NAME_TO_SYM[key] = str(row["SYMBOL"]).strip() + ".NS"
        _NSE_NAMES.extend(_NAME_TO_SYM.keys())
        print(f"loaded {len(_NAME_TO_SYM)} companies.")
    except Exception as e:
        print(f"\n  WARNING: Could not load NSE list ({e}).")
        print("  Market cap will fall back to PDF extraction only.\n")


_load_nse_list()


# =========================================================
# MULTI-STRATEGY FUZZY MATCH
# =========================================================

def _find_nse_symbol(company_filename: str):
    """
    Returns (ticker_or_None, match_score).
    Tries 4 scorers at high threshold, then WRatio at low threshold.
    """
    if not _NSE_NAMES:
        return None, 0.0

    query = _normalise_filename(company_filename)
    if not query:
        return None, 0.0

    for scorer in [rfuzz.token_sort_ratio, rfuzz.partial_ratio,
                   rfuzz.token_set_ratio, rfuzz.WRatio]:
        result = fuzz_process.extractOne(
            query, _NSE_NAMES,
            scorer=scorer,
            score_cutoff=THRESHOLD_HIGH,
        )
        if result:
            name, score, _ = result
            return _NAME_TO_SYM[name], float(score)

    # Last resort: lower threshold
    result = fuzz_process.extractOne(
        query, _NSE_NAMES,
        scorer=rfuzz.WRatio,
        score_cutoff=THRESHOLD_LOW,
    )
    if result:
        name, score, _ = result
        return _NAME_TO_SYM[name], float(score)

    return None, 0.0


# =========================================================
# YFINANCE MARKET CAP
# =========================================================

@lru_cache(maxsize=4096)
def _fetch_market_cap_yf(ticker: str):
    try:
        info = yf.Ticker(ticker).fast_info
        mc   = getattr(info, "market_cap", None)
        if mc and mc > 0:
            mc_cr = mc / 1e7
            if 50 <= mc_cr <= 25_000_000:
                return f"{mc_cr:,.2f} Cr"
    except Exception:
        pass
    return None


def _market_cap_from_nse(company_filename: str):
    ticker, _ = _find_nse_symbol(company_filename)
    if not ticker:
        return None
    return _fetch_market_cap_yf(ticker)


# =========================================================
# PDF-BASED MARKET CAP  (fallback)
# =========================================================

def _to_float(s): return float(s.replace(",", "").strip())
def _plausible(v): return 50 <= v <= 25_000_000

_LABEL = (r'market\s*cap(?:itali[sz]ation)?'
          r'|market\s*value\s*(?:of\s*equity\s*)?'
          r'|mcap|m\.?\s*cap')
_NUM  = r'([\d,]+(?:\.\d+)?)'
_CR   = r'\s*(?:crore|cr\.?)'
_LKCR = r'\s*lakh\s*crore'
_BLN  = r'\s*billion'

_MC_RE = [
    re.compile(rf'(?:{_LABEL})[\s\S]{{0,200}}?(?:[\u20b9]|rs\.?|inr)\s*{_NUM}{_CR}',
               re.IGNORECASE | re.DOTALL),
    re.compile(rf'(?:{_LABEL})[\s\S]{{0,200}}?{_NUM}{_CR}',
               re.IGNORECASE | re.DOTALL),
    re.compile(rf'(?:{_LABEL})[\s\S]{{0,200}}?{_NUM}{_LKCR}',
               re.IGNORECASE | re.DOTALL),
    re.compile(rf'(?:{_LABEL})[\s\S]{{0,200}}?{_NUM}{_BLN}',
               re.IGNORECASE | re.DOTALL),
]
_IS_LAKH = [False, False, True, False]
_IS_BLN  = [False, False, False, True]
_CR_RE  = re.compile(rf'{_NUM}{_CR}', re.IGNORECASE)
_LK_RE  = re.compile(rf'{_NUM}{_LKCR}', re.IGNORECASE)
_LBL_RE = re.compile(_LABEL, re.IGNORECASE)


def _from_regex(text):
    for i, pat in enumerate(_MC_RE):
        for m in pat.finditer(text):
            try:
                v    = _to_float(m.group(1))
                v_cr = (v * 1_00_000 if _IS_LAKH[i]
                        else v * 8_300 if _IS_BLN[i] else v)
                if _plausible(v_cr):
                    return f"{v_cr:,.2f} Cr"
            except Exception:
                continue
    return None


def _pdf_blocks(pdf_path):
    try:
        doc = fitz.open(pdf_path)
        for pg in range(min(len(doc), MC_MAX_PAGES)):
            texts = [b[4].strip() for b in doc[pg].get_text("blocks") if b[4].strip()]
            for i, blk in enumerate(texts):
                if not _LBL_RE.search(blk):
                    continue
                window = " ".join(texts[i: i + 9])
                m = _LK_RE.search(window)
                if m:
                    v = _to_float(m.group(1)) * 1_00_000
                    if _plausible(v):
                        doc.close(); return f"{v:,.2f} Cr"
                m = _CR_RE.search(window)
                if m:
                    v = _to_float(m.group(1))
                    if _plausible(v):
                        doc.close(); return f"{v:,.2f} Cr"
                if i + 1 < len(texts):
                    bare = re.match(r'^[\s\u20b9Rs\.INR]*([\d,]+(?:\.\d+)?)',
                                    texts[i + 1], re.I)
                    if bare:
                        v = _to_float(bare.group(1))
                        if _plausible(v):
                            doc.close(); return f"{v:,.2f} Cr"
        doc.close()
    except Exception:
        pass
    return None


def _pdf_words(pdf_path):
    try:
        doc   = fitz.open(pdf_path)
        words = []
        for pg in range(min(len(doc), MC_MAX_PAGES)):
            words.extend(w[4] for w in doc[pg].get_text("words"))
        doc.close()
        return _from_regex(" ".join(words))
    except Exception:
        return None


def extract_market_cap(pdf_path, raw_text, company):
    return (
        _market_cap_from_nse(company)
        or _pdf_blocks(pdf_path)
        or _from_regex(raw_text[:200_000])
        or _pdf_words(pdf_path)
        or "Not Mentioned"
    )


# =========================================================
# SECTOR SCORES
# =========================================================

SECTOR_SCORES = {

    "Information Technology": [
        (3, "infosys limited"),          (3, "tata consultancy services"),
        (3, "wipro limited"),            (3, "hcl technologies"),
        (3, "tech mahindra"),            (3, "mphasis"),
        (3, "persistent systems"),       (3, "coforge"),
        (3, "ltimindtree"),              (3, "l&t technology services"),
        (3, "zensar"),                   (3, "hexaware"),
        (3, "mastek"),                   (3, "birlasoft"),
        (3, "cyient"),                   (3, "kpit technologies"),
        (3, "niit technologies"),        (3, "firstsource"),
        (3, "oracle financial services"),(3, "tata elxsi"),
        (3, "software services company"),(3, "information technology company"),
        (3, "information technology services"),(3, "it solutions provider"),
        (3, "software development company"),(3, "it enabled services"),
        (3, "ites company"),             (3, "software exports"),
        (3, "business process outsourcing"),(3, "nasscom"),
        (2, "software company"),         (2, "it company"),
        (2, "technology company"),       (2, "saas platform"),
        (2, "erp solutions"),            (2, "software products"),
        (2, "it outsourcing"),           (2, "bpo services"),
        (2, "digital solutions company"),(2, "technology services company"),
        (2, "software and services"),    (2, "it services and consulting"),
        (2, "managed services provider"),(2, "cloud services company"),
        (2, "artificial intelligence company"),(2, "data analytics company"),
        (2, "export of software"),       (2, "software export revenue"),
        (2, "application development"),  (2, "enterprise software"),
        (1, "software"),                 (1, "it services"),
    ],

    "Financial Services": [
        (3, "non-banking financial company"),(3, "non banking financial company"),
        (3, "nbfc"),                     (3, "scheduled commercial bank"),
        (3, "life insurance company"),   (3, "general insurance company"),
        (3, "asset management company"), (3, "mutual fund"),
        (3, "stock broking"),            (3, "housing finance company"),
        (3, "microfinance institution"), (3, "small finance bank"),
        (3, "payment bank"),
        (2, "banking services"),         (2, "financial institution"),
        (2, "lending business"),         (2, "deposits and advances"),
        (2, "loan portfolio"),           (2, "net interest income"),
        (2, "npa ratio"),
        (1, "bank"),                     (1, "insurance"),
    ],

    "Energy": [
        (3, "oil and gas company"),      (3, "petroleum refinery"),
        (3, "crude oil exploration"),    (3, "natural gas distribution"),
        (3, "renewable energy company"), (3, "power generation company"),
        (3, "coal mining company"),      (3, "thermal power plant"),
        (3, "solar energy company"),     (3, "wind energy company"),
        (3, "liquefied natural gas"),    (3, "hydrocarbon exploration"),
        (2, "oil refinery"),             (2, "petroleum products"),
        (2, "power utility"),            (2, "upstream oil"),
        (1, "refinery"),                 (1, "petroleum"),
    ],

    "Healthcare": [
        (3, "pharmaceutical company"),   (3, "drug manufacturer"),
        (3, "active pharmaceutical ingredient"),(3, "api manufacturer"),
        (3, "hospital chain"),           (3, "healthcare services company"),
        (3, "biotechnology company"),    (3, "medical devices company"),
        (3, "diagnostic services"),      (3, "formulations manufacturer"),
        (3, "generic drugs"),            (3, "clinical research organization"),
        (2, "pharma company"),           (2, "generics manufacturer"),
        (2, "drug discovery"),           (2, "nutraceuticals"),
        (2, "life sciences"),
        (1, "pharmaceutical"),           (1, "hospital"),  (1, "healthcare"),
    ],

    "Consumer Goods": [
        (3, "fast moving consumer goods"),(3, "fmcg company"),
        (3, "consumer products company"),(3, "food and beverage company"),
        (3, "packaged foods company"),   (3, "personal care products"),
        (3, "retail chain"),             (3, "supermarket chain"),
        (3, "e-commerce company"),       (3, "apparel company"),
        (3, "footwear company"),
        (2, "consumer brand"),           (2, "household products"),
        (2, "food processing"),          (2, "beverages company"),
        (1, "fmcg"),                     (1, "retail"),
    ],

    "Industrials": [
        (3, "capital goods manufacturer"),(3, "heavy engineering company"),
        (3, "industrial machinery"),     (3, "defence manufacturer"),
        (3, "auto components manufacturer"),(3, "automobile manufacturer"),
        (3, "commercial vehicles manufacturer"),(3, "two wheeler manufacturer"),
        (3, "passenger vehicle"),        (3, "tractor manufacturer"),
        (3, "aerospace company"),        (3, "epc contractor"),
        (3, "infrastructure developer"),
        (2, "engineering company"),      (2, "manufacturing company"),
        (2, "industrial products"),      (2, "oem supplier"),
        (1, "manufacturing"),            (1, "engineering"),
    ],

    "Materials": [
        (3, "steel manufacturer"),       (3, "iron and steel"),
        (3, "cement manufacturer"),      (3, "specialty chemicals company"),
        (3, "fertiliser company"),       (3, "aluminium smelter"),
        (3, "copper smelter"),           (3, "mining company"),
        (3, "paper and pulp"),           (3, "glass manufacturer"),
        (3, "paint manufacturer"),       (3, "agrochemicals company"),
        (3, "petrochemicals company"),
        (2, "chemical company"),         (2, "commodity chemicals"),
        (1, "cement"), (1, "steel"),     (1, "chemicals"),  (1, "mining"),
    ],

    "Real Estate": [
        (3, "real estate developer"),    (3, "property developer"),
        (3, "residential projects"),     (3, "commercial real estate"),
        (3, "township development"),     (3, "reit"),
        (3, "invit"),                    (3, "real estate investment trust"),
        (2, "housing projects"),         (2, "property development"),
        (2, "mall developer"),
        (1, "real estate"),              (1, "construction projects"),
    ],

    "Telecommunications": [
        (3, "telecom operator"),         (3, "mobile network operator"),
        (3, "telecommunications company"),(3, "internet service provider"),
        (3, "broadband services"),       (3, "spectrum allocation"),
        (3, "satellite communication"),
        (2, "telecom services"),         (2, "mobile subscribers"),
        (1, "telecommunications"),       (1, "telecom"),
    ],

    "Utilities": [
        (3, "electricity distribution company"),(3, "water utility"),
        (3, "sewage treatment"),         (3, "gas distribution company"),
        (3, "discoms"),                  (3, "state electricity board"),
        (2, "power distribution"),       (2, "electricity board"),
        (1, "utility"),
    ],

    "Media & Entertainment": [
        (3, "media company"),            (3, "broadcasting company"),
        (3, "entertainment company"),    (3, "film production"),
        (3, "ott platform"),             (3, "digital streaming"),
        (2, "television channel"),       (2, "news media"),
        (2, "publishing company"),       (2, "advertising agency"),
        (1, "media"),                    (1, "broadcasting"),
    ],
}


# =========================================================
# FIELD EXTRACTORS
# =========================================================

def detect_sector(clean_text):
    window = " ".join(clean_text.split()[:4000])
    scores = {s: 0 for s in SECTOR_SCORES}
    for sector, kws in SECTOR_SCORES.items():
        for weight, phrase in kws:
            if phrase in window:
                scores[sector] += weight
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "Unknown"


def extract_year(clean_text):
    for pat in [r'fy\s?20\d{2}[-\u2013]\d{2}',
                r'financial year\s?20\d{2}[-\u2013]\d{2}',
                r'annual report\s?20\d{2}[-\u2013]\d{2}',
                r'20\d{2}[-\u2013]\d{2}']:
        m = re.search(pat, clean_text)
        if m: return m.group(0)
    return "Unknown"


def detect_framework(clean_text):
    found = []
    for name, pat in {"BRSR": r'\bbrsr\b', "GRI": r'\bgri\b',
                       "SASB": r'\bsasb\b', "TCFD": r'\btcfd\b',
                       "CDP": r'\bcdp\b', "IFRS S1": r'ifrs\s?s1',
                       "IFRS S2": r'ifrs\s?s2'}.items():
        if re.search(pat, clean_text): found.append(name)
    return ", ".join(found) if found else "Unknown"


def detect_brsr_version(clean_text):
    if "brsr core" in clean_text: return "BRSR Core"
    if "business responsibility and sustainability report" in clean_text: return "BRSR"
    return "Unknown"


def detect_assurance(clean_text):
    for pat in [r'reasonable assurance', r'high level of assurance', r'positive assurance']:
        if re.search(pat, clean_text, re.IGNORECASE): return "Reasonable Assurance"
    for pat in [r'limited assurance', r'moderate level of assurance', r'negative assurance']:
        if re.search(pat, clean_text, re.IGNORECASE): return "Limited Assurance"
    for pat in [
        r'third[- ]party assurance',  r'third[- ]party verification',
        r'external assurance',        r'external verification',
        r'external review',           r'independent assurance',
        r'independent verification',  r'independent review',
        r'assurance statement',       r'assurance report',
        r'verification statement',    r'verification report',
        r'assured by',                r'verified by',
        r'assurance provider',        r'sustainability assurance',
        r'esg assurance',             r'brsr assurance',
        r'independently verified',    r'independently assured',
        r'bureau veritas',            r'\bdnv\b',
        r'ey.*assurance',             r'kpmg.*assurance',
        r'deloitte.*assurance',       r'pwc.*assurance',
        r'erm.*assurance',
    ]:
        if re.search(pat, clean_text, re.IGNORECASE): return "Third Party Assurance"
    for pat in [r'self[- ]declared', r'self[- ]assessed', r'self[- ]certified',
                r'internally verified', r'management assertion']:
        if re.search(pat, clean_text, re.IGNORECASE): return "Self Declared"
    return "Not Mentioned"


def detect_geography(clean_text):
    if "india" in clean_text: return "India"
    if "global" in clean_text: return "Global"
    return "Unknown"


# =========================================================
# PDF TEXT EXTRACTION
# =========================================================

def extract_text(pdf_path, max_pages=MAX_PAGES):
    raw_pages = []
    try:
        doc = fitz.open(pdf_path)
        for i in range(min(len(doc), max_pages)):
            raw_pages.append(doc[i].get_text("text"))
        doc.close()
    except Exception:
        return "", ""
    joined = "\n".join(raw_pages)
    raw    = re.sub(r'[ \t]+', ' ', joined)
    raw    = re.sub(r'[\x00-\x09\x0B\x0C\x0E-\x1F\x7F]', ' ', raw)
    clean  = re.sub(r'\s+', ' ', raw.lower()).strip()
    return clean, raw


# =========================================================
# PROCESS SINGLE PDF  — expects (folder, filename) tuple
# =========================================================

def process_pdf(args):
    folder, file = args
    try:
        company  = file.replace(".pdf", "")
        pdf_path = os.path.join(folder, file)
        clean, raw = extract_text(pdf_path)
        if not clean:
            return None

        ticker, match_score = _find_nse_symbol(company)

        return {
            "company"        : company,
            "source_folder"  : os.path.basename(folder),
            "nse_symbol"     : ticker or "Unknown",
            "match_score"    : round(match_score, 1),
            "sector"         : detect_sector(clean),
            "market_cap"     : extract_market_cap(pdf_path, raw, company),
            "reporting_year" : extract_year(clean),
            "framework_used" : detect_framework(clean),
            "brsr_version"   : detect_brsr_version(clean),
            "assurance_type" : detect_assurance(clean),
            "geography"      : detect_geography(clean),
        }
    except Exception:
        return None
    finally:
        gc.collect()


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    all_tasks = []
    for folder in REPORTS_FOLDERS:
        if not os.path.exists(folder):
            print(f"  WARNING: Folder not found, skipping: {folder}")
            continue
        files = [f for f in os.listdir(folder) if f.endswith(".pdf")]
        all_tasks.extend((folder, f) for f in files)
        print(f"  Found {len(files):>5} PDFs in {os.path.basename(folder)}")

    print(f"\nTotal Reports : {len(all_tasks)}")
    print(f"Workers       : {MAX_WORKERS}\n")

    if os.path.exists(OUTPUT_JSONL):
        os.remove(OUTPUT_JSONL)

    results = []
    with Pool(MAX_WORKERS) as pool:
        for result in tqdm(
            pool.imap(process_pdf, all_tasks),
            total=len(all_tasks),
            desc="Extracting Metadata",
        ):
            if result:
                results.append(result)
                with open(OUTPUT_JSONL, "a", encoding="utf-8") as f:
                    json.dump(result, f, ensure_ascii=False)
                    f.write("\n")

    df = pd.DataFrame(results).sort_values(
        ["source_folder", "company"]
    ).reset_index(drop=True)
    df.to_csv(OUTPUT_CSV, index=False)

    total    = len(df)
    mc_found = (df["market_cap"]  != "Not Mentioned").sum()
    uk_sec   = (df["sector"]      == "Unknown").sum()
    uk_sym   = (df["nse_symbol"]  == "Unknown").sum()

    print("\n" + "=" * 56)
    print("  METADATA EXTRACTION COMPLETE")
    print("=" * 56)
    print(f"  Rows processed    : {total}")
    print(f"  CSV               : {OUTPUT_CSV}")
    print(f"  JSONL             : {OUTPUT_JSONL}")

    for folder_name, grp in df.groupby("source_folder"):
        n      = len(grp)
        sym_ok = (grp["nse_symbol"] != "Unknown").sum()
        mc_ok  = (grp["market_cap"]  != "Not Mentioned").sum()
        scores = grp["match_score"]
        print(f"\n  [{folder_name}]  {n} reports")
        print(f"    NSE symbol found : {sym_ok}/{n}  ({sym_ok/n*100:.1f}%)")
        print(f"    Market cap found : {mc_ok}/{n}  ({mc_ok/n*100:.1f}%)")
        print(f"    Match score      : avg {scores.mean():.1f}  "
              f"(min {scores.min():.0f} / max {scores.max():.0f})")

    print(f"\n  Sector breakdown :")
    for sector, count in df["sector"].value_counts().items():
        bar = "X" * int(count / total * 35)
        print(f"    {sector:<30} {count:>5}  ({count/total*100:.1f}%)  {bar}")

    print(f"\n  Assurance breakdown :")
    for label, count in df["assurance_type"].value_counts().items():
        print(f"    {label:<30} {count:>5}  ({count/total*100:.1f}%)")

    unmatched = df[df["nse_symbol"] == "Unknown"]["company"].tolist()
    if unmatched:
        print(f"\n  Unmatched companies ({len(unmatched)}) -- first 20:")
        for c in unmatched[:20]:
            print(f"    {c}")
        if len(unmatched) > 20:
            print(f"    ... and {len(unmatched)-20} more (see nse_symbol column)")
    print("=" * 56)