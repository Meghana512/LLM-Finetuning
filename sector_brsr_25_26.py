import os
import fitz
import pandas as pd
import re
from multiprocessing import Pool, cpu_count

reports_folder = "/kaggle/input/datasets/meghanakadari/indian-listed-companies-brsr-reports-fy-202324/Indian_BRSR_Reports_2025-26/brsr reports 2025-26/files"

# Sector keyword dictionary
sector_keywords = {
    "Information Technology": ["software", "technology", "it services", "digital"],
    "Financial Services": ["bank", "finance", "financial services", "nbfc", "insurance"],
    "Energy": ["oil", "gas", "energy", "petroleum"],
    "Healthcare": ["pharma", "pharmaceutical", "healthcare", "biotech"],
    "Consumer Goods": ["consumer goods", "fmcg", "retail", "food"],
    "Industrials": ["engineering", "industrial", "manufacturing"],
    "Materials": ["chemicals", "cement", "steel", "materials"],
    "Real Estate": ["real estate", "construction", "property"],
    "Telecommunications": ["telecom", "communication", "network"]
}


def detect_sector(file):

    company = file.replace(".pdf","")
    path = os.path.join(reports_folder, file)

    text = ""

    try:
        doc = fitz.open(path)

        # read first 15 pages only (sector usually mentioned early)
        for page in doc[:15]:
            text += page.get_text()

    except:
        return {"Company": company, "Sector": "Unknown"}

    text = text.lower()

    detected_sector = "Unknown"

    for sector, keywords in sector_keywords.items():
        for keyword in keywords:
            if keyword in text:
                detected_sector = sector
                break
        if detected_sector != "Unknown":
            break

    return {"Company": company, "Sector": detected_sector}


if __name__ == "__main__":

    files = [f for f in os.listdir(reports_folder) if f.endswith(".pdf")]

    print("Total reports:", len(files))

    with Pool(cpu_count()) as pool:
        results = pool.map(detect_sector, files)

    df = pd.DataFrame(results)

    # Save company-sector mapping
    df.to_csv("company_sector_mapping_brsr_25_26.csv", index=False)

    # Create sector-wise grouping
    sector_group = df.groupby("Sector")["Company"].apply(list).reset_index()

    sector_group["Companies"] = sector_group["Company"].apply(lambda x: ", ".join(x))

    sector_group = sector_group[["Sector", "Companies"]]

    sector_group.to_csv("sector_wise_companies_brsr_25_26.csv", index=False)

    print("Sector files created successfully")