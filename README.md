# LLM Finetuning

## Overview

This repository contains scripts used to process **corporate reports of Indian listed companies** and convert them into structured datasets that can be used for **LLM fine-tuning and financial text analysis**.

The project works with **BRSR reports and Annual Reports (PDF format)** and performs multiple extraction and preprocessing tasks.

The main functionalities include:

* **MD&A Extraction** – extracting the Management Discussion & Analysis section from annual reports
* **ESG Text Extraction** – extracting ESG related disclosures from annual and BRSR reports
* **Word Count Analysis** – computing word statistics for different sections of BRSR reports
* **Sector Classification** – identifying the sector of companies using information from annual and BRSR reports

The scripts generate **CSV datasets** that can be used for further analysis.

## Dataset

The scripts process large collections of corporate report PDFs (BRSR Reports and Annual Reports).
Due to GitHub file size limits, the datasets are not included in this repository(Kaggle links for those datasets were added) .

## Author

Meghana Kadari
