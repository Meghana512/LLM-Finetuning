import os
import fitz
import pandas as pd
import nltk
import re
from multiprocessing import Pool, cpu_count
from nltk.corpus import stopwords
import warnings

warnings.filterwarnings("ignore")

nltk.download("stopwords", quiet=True)
stop_words = set(stopwords.words("english"))

reports_folder = "/kaggle/input/datasets/meghanakadari/indian-listed-companies-brsr-reports-fy-202324/Indian_BRSR_Reports_2025-26/brsr reports 2025-26/files"

sections = [
    "general disclosures",
    "management and process disclosures",
    "principle wise performance",
    "environment",
    "social",
    "governance"
]


def process_pdf(file):

    company = file.replace(".pdf", "")
    path = os.path.join(reports_folder, file)

    text = ""

    try:
        doc = fitz.open(path)

        # read only first 60 pages (BRSR usually early in report)
        for page in doc[:60]:
            text += page.get_text()

    except:
        return None

    text = text.lower()
    text = re.sub(r"[^a-z\s]", " ", text)

    words = text.split()

    total_words = len(words)

    words_no_stop = [w for w in words if w not in stop_words]

    total_words_no_stop = len(words_no_stop)

    results = []

    results.append({
        "Company": company,
        "Section": "TOTAL_REPORT",
        "Word_Count": total_words,
        "Word_Count_No_Stopwords": total_words_no_stop
    })

    for section in sections:

        if section in text:

            section_text = text.split(section, 1)[1]

            words = section_text.split()

            wc = len(words)

            words_no_stop = [w for w in words if w not in stop_words]

            wc_no_stop = len(words_no_stop)

            results.append({
                "Company": company,
                "Section": section,
                "Word_Count": wc,
                "Word_Count_No_Stopwords": wc_no_stop
            })

    return results


if __name__ == "__main__":

    files = [f for f in os.listdir(reports_folder) if f.endswith(".pdf")]

    print("Total reports:", len(files))

    with Pool(cpu_count()) as pool:
        results = pool.map(process_pdf, files)

    data = []

    for r in results:
        if r:
            data.extend(r)

    df = pd.DataFrame(data)

    section_stats = df.groupby("Section")[["Word_Count","Word_Count_No_Stopwords"]].sum()

    company_stats = df[df["Section"]=="TOTAL_REPORT"][["Company","Word_Count","Word_Count_No_Stopwords"]]

    section_presence = pd.crosstab(df["Company"], df["Section"])

    df.to_csv("detailed_word_counts_brsr_25_26.csv", index=False)
    section_stats.to_csv("section_summary_brsr_25_26.csv")
    company_stats.to_csv("company_summary_brsr_25_26.csv")
    section_presence.to_csv("section_presence_brsr_25_26.csv")

    print("EDA Completed")