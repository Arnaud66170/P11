#!/usr/bin/env python3
"""
Export d'un fichier CSV local depuis un dossier Parquet
Projet P11 - OpenClassrooms
"""

import os
import pandas as pd
import pyarrow.parquet as pq

# ✅ Chemin corrigé vers le dossier contenant les fichiers .parquet
input_path = "outputs/pca_parquet"
output_csv_path = "outputs/Nom_Prenom_2_images_072024.csv"

# 📂 Recherche du fichier .parquet réel (souvent un seul fichier exporté par Spark)
parquet_files = [f for f in os.listdir(input_path) if f.endswith(".parquet")]
if not parquet_files:
    raise FileNotFoundError(f"Aucun fichier .parquet trouvé dans {input_path}")

# 📥 Chargement et conversion
parquet_file_path = os.path.join(input_path, parquet_files[0])
table = pq.read_table(parquet_file_path)
df = table.to_pandas()

# 🧪 Vérification rapide
print("✅ Données chargées :")
print(df.head(3))

# 💾 Export CSV final
os.makedirs("outputs", exist_ok=True)
df.to_csv(output_csv_path, index=False)
print(f"✅ CSV exporté → {output_csv_path}")


# chmod +x scripts/parquet_to_csv_local.py
# ./scripts/parquet_to_csv_local.py