#!/usr/bin/env python3

# scripts/export_pca_csv.py

"""
Script d'export CSV depuis le DataFrame PCA Spark
Projet P11 - OpenClassrooms
"""

import os
from pyspark.sql import SparkSession

# Démarrage Spark
spark = SparkSession.builder.appName("ExportPCA").getOrCreate()

# Chargement des résultats PCA (générés lors du pipeline)
df = spark.read.parquet("outputs/pca_features.parquet")

# Transformation du vecteur PCA en colonnes individuelles
def extract_vector(row):
    return [float(x) for x in row.pca_features]

df_expanded = (
    df.select("image_id", "fruit_label", "pca_features")
      .rdd.map(lambda row: (row.image_id, row.fruit_label, *extract_vector(row)))
      .toDF()
)

# Génération des noms de colonnes
nb_dims = len(df.select("pca_features").first()["pca_features"])
columns = ["image_id", "fruit_label"] + [f"pca_{i}" for i in range(nb_dims)]
df_expanded = df_expanded.toDF(*columns)

# Export CSV local
output_path = "outputs/pca_results.csv"
df_expanded.toPandas().to_csv(output_path, index=False)

print(f"✅ Fichier CSV exporté : {output_path}")


# Exécution depuis la racine projet :
# chmod +x scripts/export_pca_csv.py
# ./scripts/export_pca_csv.py
