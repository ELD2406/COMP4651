# COMP4651 — H&M Retail Data Platform

Group project: streaming ingestion + PySpark processing + serving layer.

## Progress

- Spark environment is configured (local mode, driven by `config.toml`).
- Data ingestion is done: the three H&M CSVs are loaded, cleaned, and validated in
  `notebooks/01_hm_pyspark_analysis.ipynb` for batch analysis.

## Dataset

The dataset is excluded from Git because of its size. Download the three CSVs into
`h-and-m-personalized-fashion-recommendations/data/raw/` from
[HuggingFace](https://huggingface.co/datasets/einrafh/hnm-fashion-recommendations-data).

## Run the analysis notebook

Start Jupyter from the repo root and open `notebooks/01_hm_pyspark_analysis.ipynb`.
