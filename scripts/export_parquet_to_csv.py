#!/usr/bin/env python3
"""
Script d’export CSV depuis un dossier Parquet Spark
Projet P11 - OpenClassrooms
"""

import pandas as pd
import pyarrow.parquet as pq
import os

INPUT_DIR = "outputs/pca_parquet"
OUTPUT_CSV = "outputs/Nom_Prenom_2_images_072024.csv"

if not os.path.exists(INPUT_DIR):
    raise FileNotFoundError(f"❌ Dossier introuvable : {INPUT_DIR}")

# Lecture Parquet + conversion CSV
df = pq.read_table(INPUT_DIR).to_pandas()
df.to_csv(OUTPUT_CSV, index=False)

print(f"✅ CSV généré : {OUTPUT_CSV}")

# Rendre le script exécutable :
# chmod +x scripts/export_parquet_to_csv.py

# L'exécuter :
# ./scripts/export_parquet_to_csv.py

