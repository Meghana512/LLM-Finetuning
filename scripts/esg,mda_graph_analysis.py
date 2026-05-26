# ESG and MD&A EDA VISUALIZATION BAR GRAPH + PIE CHART
# =========================================================

# Install if needed
!pip install pandas matplotlib -q

import pandas as pd
import matplotlib.pyplot as plt

# =========================================================
# LOAD ESG CSV
# =========================================================

df = pd.read_csv("/kaggle/input/datasets/vaibhavmeena23/esg-and-md-and-a-eda-csv-files/mda_eda/mda_eda_25-26.csv")

# =========================================================
# FILTER ONLY COMPANIES HAVING ESG
# =========================================================

df_esg = df[df["has_mda"] == 1].copy()

# Keep ORIGINAL Excel order
df_esg.reset_index(drop=True, inplace=True)

# =========================================================
# CALCULATE AVERAGE WORD COUNT
# =========================================================

avg_word_count = df_esg["word_count"].mean()

# =========================================================
# BAR GRAPH
# Word Count vs Companies
# =========================================================

plt.figure(figsize=(18, 7))

# Use numeric positions instead of company names
x_positions = range(len(df_esg))

plt.bar(
    x_positions,
    df_esg["word_count"]
)

# Average line
plt.axhline(
    y=avg_word_count,
    linestyle='--',
    linewidth=2,
    label=f'Average = {avg_word_count:,.0f} words'
)

# Labels
plt.xlabel("Companies")

plt.ylabel("MDA Word Count")

plt.title("MDA Word Count Distribution for Annual Reports 22-23 of 1394 Indian Companies")

# Remove company names from x-axis
plt.xticks([])

# Show legend
plt.legend()

plt.tight_layout()

plt.savefig(
    "mda_wordcount_bargraph_25-26.png",
    dpi=300,
    bbox_inches='tight'
)

plt.show()

# =========================================================
# PIE CHART
# ESG Presence Percentage
# =========================================================

esg_counts = df["has_mda"].value_counts()

labels = ["Has MDA Section", "No MDA Section"]

sizes = [
    esg_counts.get(1, 0),
    esg_counts.get(0, 0)
]

plt.figure(figsize=(7, 7))

plt.pie(
    sizes,
    labels=labels,
    autopct='%1.1f%%'
)

plt.title("Percentage of Companies with MDA Section for Annual Reports 22-23 of 1394 Indian Companies")
plt.savefig(
    "mda_presence_piechart_25-26.png",
    dpi=300,
    bbox_inches='tight'
)
plt.show()


print("Done")