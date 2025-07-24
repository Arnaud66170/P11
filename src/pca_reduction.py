# src/pca_reduction.py

import os
import warnings
import numpy as np
import matplotlib.pyplot as plt

# Suppression des avertissements
warnings.filterwarnings('ignore')
plt.rcParams.update({'figure.max_open_warning': 0})

from pyspark.ml import PipelineModel
from pyspark.ml.feature import PCA, VectorAssembler
from pyspark.ml.linalg import Vectors, DenseVector, VectorUDT
# Import spécifique pour éviter les conflits de noms avec les fonctions Python
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType
import shutil

def convert_array_to_vector(df, features_col="features"):
    """
    Cette fonction convertit une colonne d'arrays Python/NumPy en vecteurs Spark ML.
    Nécessaire car MobileNetV2 produit des arrays Python, mais PCA Spark attend des VectorUDT.
    
    Paramètres :
    - df : DataFrame Spark avec une colonne 'features' au format array
    - features_col : nom de la colonne à convertir
    
    Retourne :
    - DataFrame avec la colonne convertie en format VectorUDT
    """
    print("🔄 Conversion des arrays Python vers des vecteurs Spark ML...")
    
    # UDF pour convertir un array en DenseVector Spark ML
    def array_to_vector(arr):
        if arr is None:
            return None
        # Convertit l'array en DenseVector Spark ML
        return Vectors.dense(arr)
    
    # Enregistrement de l'UDF avec le bon type de retour
    array_to_vector_udf = udf(array_to_vector, VectorUDT())
    
    # Application de la conversion
    df_converted = df.withColumn(features_col + "_vector", array_to_vector_udf(col(features_col)))
    df_converted = df_converted.drop(features_col).withColumnRenamed(features_col + "_vector", features_col)
    
    print("✅ Conversion terminée - les features sont maintenant au format VectorUDT")
    return df_converted

def get_optimal_pca_k(df, spark, max_k=200, threshold=0.95, force_retrain=False, cache_path="../outputs/pca_variance.parquet"):
    """
    Détermine le nombre optimal de composantes principales à conserver en fonction du seuil de variance cumulée souhaitée.
    
    ⚠️ CORRECTION MAJEURE : Cette fonction calcule correctement la variance cumulée
    en stockant TOUTES les variances individuelles, pas seulement la dernière valeur.

    Paramètres :
    - df : DataFrame Spark contenant les features à réduire (format VectorUDT)
    - spark : SparkSession active
    - max_k : nombre maximal de composantes à tester (augmenté à 200 pour MobileNetV2)
    - threshold : seuil de variance expliquée cumulée à atteindre (ex : 0.95)
    - force_retrain : recalcul même si un cache existe
    - cache_path : chemin de sauvegarde des variances cumulées

    Retourne :
    - k_optimal : int, nombre de composantes à retenir
    - variance_data : liste des tuples (k, variance_individuelle, variance_cumulée)
    """
    from pyspark.ml.feature import PCA
    from pyspark.ml.linalg import Vectors, VectorUDT
    import os

    # Vérifie que la colonne 'features' est bien un vecteur Spark ML
    if not isinstance(df.schema["features"].dataType, VectorUDT):
        print("❌ Erreur : La colonne 'features' doit être de type VectorUDT (Spark ML).")
        print("💡 Conseil : Utilise d'abord convert_array_to_vector() pour convertir tes données.")
        raise TypeError("La colonne 'features' doit être de type VectorUDT (Spark ML).")

    if os.path.exists(cache_path) and not force_retrain:
        print(f"✅ Cache de variance chargé depuis {cache_path}")
        df_variance = spark.read.parquet(cache_path)
        
        # Reconstruction des données pour le retour
        variance_data = []
        rows = df_variance.orderBy("k").collect()
        for row in rows:
            variance_data.append((row["k"], row["individual_variance"], row["cum_variance"]))
            
    else:
        print("🆕 Pas de cache détecté ou recalcul forcé : calcul des variances cumulées...")
        print(f"📊 Test de k=1 à k={max_k} composantes principales...")
        
        variance_data = []
        cumulative_variance = 0.0
        
        for k in range(1, max_k + 1):
            print(f"   Calcul PCA pour k={k}...", end=" ")
            
            # Création et entraînement du modèle PCA
            pca = PCA(k=k, inputCol="features", outputCol="pca_features")
            model = pca.fit(df)
            
            # Récupération des variances expliquées individuelles
            explained_variances = model.explainedVariance.toArray()
            
            # ✅ CORRECTION : Calcul correct de la variance cumulée
            # On somme TOUTES les variances individuelles jusqu'à k
            cumulative_variance = float(np.sum(explained_variances))
            
            # Variance individuelle de la k-ième composante
            individual_variance = float(explained_variances[-1])
            
            variance_data.append((k, individual_variance, cumulative_variance))
            
            print(f"Variance cumulée: {cumulative_variance:.4f} ({cumulative_variance*100:.2f}%)")
            
            # Arrêt anticipé si le seuil est atteint
            if cumulative_variance >= threshold:
                print(f"🎯 Seuil de {threshold*100:.1f}% atteint avec k={k}")
                break

        # Sauvegarde du cache
        schema = StructType([
            StructField("k", IntegerType(), True),
            StructField("individual_variance", DoubleType(), True),
            StructField("cum_variance", DoubleType(), True)
        ])
        
        df_variance = spark.createDataFrame(variance_data, schema)
        df_variance.write.mode("overwrite").parquet(cache_path)
        print(f"💾 Variance cumulée sauvegardée dans {cache_path}")

    # Affichage des résultats
    print("\n🧪 Aperçu des résultats :")
    df_variance.orderBy("k").show(min(20, max_k), truncate=False)
    
    # Recherche du k optimal
    optimal_rows = df_variance.filter(df_variance["cum_variance"] >= threshold).orderBy("k")

    if optimal_rows.count() == 0:
        print(f"⚠️  Aucune valeur de k (jusqu'à {max_k}) ne permet d'atteindre {threshold*100:.1f}% de variance.")
        k_optimal = max_k
        actual_variance = df_variance.orderBy(F.col("k").desc()).first()["cum_variance"]
        print(f"📊 Avec k={k_optimal}, variance expliquée = {actual_variance:.4f} ({actual_variance*100:.2f}%)")
        print(f"💡 Conseil : Augmente max_k ou diminue le threshold.")
    else:
        row = optimal_rows.first()
        k_optimal = int(row["k"])
        actual_variance = row["cum_variance"]
        print(f"✅ {k_optimal} composantes nécessaires pour expliquer au moins {threshold*100:.1f}% de la variance.")
        print(f"📊 Variance exacte atteinte avec k={k_optimal} : {actual_variance:.4f} ({actual_variance*100:.2f}%)")

    return k_optimal, variance_data

def plot_variance_explained(variance_data, threshold=0.95, save_path="../outputs/pca_variance_plot.png"):
    """
    Cette fonction génère un graphique de la variance expliquée cumulée
    pour visualiser l'évolution et identifier le coude optimal.
    
    Paramètres :
    - variance_data : liste des tuples (k, variance_individuelle, variance_cumulée)
    - threshold : seuil à afficher sur le graphique
    - save_path : chemin de sauvegarde du graphique
    """
    print("📈 Génération du graphique de variance expliquée...")
    
    # Extraction des données
    k_values = [item[0] for item in variance_data]
    individual_variances = [item[1] for item in variance_data]
    cumulative_variances = [item[2] for item in variance_data]
    
    # Création du graphique sans avertissements
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # Graphique 1 : Variance cumulée
        ax1.plot(k_values, cumulative_variances, 'b-', linewidth=2, marker='o', markersize=4)
        ax1.axhline(y=threshold, color='r', linestyle='--', linewidth=2, label=f'Seuil {threshold*100:.0f}%')
        ax1.set_xlabel('Nombre de composantes principales (k)')
        ax1.set_ylabel('Variance expliquée cumulée')
        ax1.set_title('Évolution de la variance expliquée cumulée en fonction de k')
        ax1.grid(True, alpha=0.3)
        ax1.legend()
        ax1.set_ylim(0, 1)
        
        # Graphique 2 : Variance individuelle par composante
        ax2.bar(k_values, individual_variances, alpha=0.7, color='green')
        ax2.set_xlabel('Numéro de la composante principale')
        ax2.set_ylabel('Variance expliquée individuelle')
        ax2.set_title('Contribution individuelle de chaque composante principale')
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # Sauvegarde
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()  # Ferme la figure pour éviter les avertissements
    
    print(f"💾 Graphique sauvegardé : {save_path}")

def apply_pca_on_features(spark, df, k=100, features_col="features", output_path="../outputs/Results_Local/features_pca.parquet", force_retrain=False):
    """
    Applique une réduction PCA sur un DataFrame Spark.
    
    ⚠️ CORRECTION : Cette fonction vérifie maintenant le format des données d'entrée

    Paramètres :
    - spark : SparkSession active à utiliser pour les lectures/écritures
    - df : DataFrame Spark contenant les vecteurs (format VectorUDT)
    - k : int, nombre de composantes principales à conserver
    - features_col : str, colonne contenant les vecteurs d'entrée
    - output_path : str, chemin du .parquet résultant
    - force_retrain : bool, si False, recharge depuis cache si existant

    Retourne :
    - DataFrame Spark avec les features réduites
    """
    import os
    from pyspark.ml.feature import PCA
    # Import local pour éviter les problèmes de chemin
    from utils import export_dataframe_if_needed

    # Vérification du format des données
    if not isinstance(df.schema[features_col].dataType, VectorUDT):
        print("❌ Erreur : La colonne features doit être de type VectorUDT")
        print("💡 Conseil : Utilise convert_array_to_vector() avant d'appeler cette fonction")
        raise TypeError(f"La colonne '{features_col}' doit être de type VectorUDT (Spark ML).")

    if os.path.exists(output_path) and not force_retrain:
        print(f"✔ Chargement des données PCA depuis le cache : {output_path}")
        return spark.read.parquet(output_path)

    print(f"⚙ Calcul PCA en cours avec k={k} composantes...")
    
    # Création et entraînement du modèle PCA
    pca = PCA(k=k, inputCol=features_col, outputCol="features_pca")
    model = pca.fit(df)
    
    # Application de la transformation
    df_pca = model.transform(df)
    
    # Ajout de métadonnées pour traçabilité
    print(f"📊 Réduction de dimensionnalité appliquée :")
    print(f"   - Dimensions originales : probablement 1280 (MobileNetV2)")
    print(f"   - Dimensions réduites : {k}")
    print(f"   - Facteur de réduction : ~{1280/k:.1f}x")
    
    # Sauvegarde
    export_dataframe_if_needed(df_pca, output_path)

    return df_pca

def plot_variance_curve(variance_data, k_optimal, threshold=0.95, save_path=None):
    """
    Affiche la courbe de variance cumulée avec annotations propres.

    :param variance_data: liste de tuples (k, var_individuelle, var_cumulée)
    :param k_optimal: entier, nombre de composantes optimales
    :param threshold: seuil de variance cumulée à atteindre (ex: 0.95)
    :param save_path: chemin d'enregistrement (facultatif)
    """
    ks = [int(row[0]) for row in variance_data]
    cum_vars = [row[2] for row in variance_data]

    plt.figure(figsize=(10, 6))
    plt.plot(ks, cum_vars, marker='o', linestyle='-', color='blue', label='Variance expliquée cumulée')
    plt.axhline(y=threshold, color='red', linestyle='--', label=f'Seuil {int(threshold*100)}%')
    plt.axvline(x=k_optimal, color='green', linestyle='--', label=f'{k_optimal} composantes')
    plt.scatter(k_optimal, cum_vars[k_optimal-1], color='black')  # point noir sur le point optimal
    plt.text(k_optimal, cum_vars[k_optimal-1]+0.01, f'{cum_vars[k_optimal-1]*100:.2f}%', ha='center', fontsize=9)

    plt.xlabel("Nombre de composantes")
    plt.ylabel("Variance expliquée cumulée")
    plt.title("Variance expliquée cumulée par la PCA (Spark)")
    plt.grid(True)
    plt.xticks(ks[::max(1, len(ks)//20)])  # espacement dynamique
    plt.yticks([round(v, 2) for v in cum_vars if round(v, 2) >= 0.5 or v == threshold] + [threshold])
    plt.legend()
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path)
    else:
        plt.show()
