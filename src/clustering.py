# src/clustering.py

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import PCA
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

# Cette fonction de visualisation effectue une PCA locale avec scikit-learn, ce qui est acceptable car on n’utilise qu’un échantillon (sample_size)
# On assume que les features_pca sont bien des DenseVector

def train_kmeans(df, features_col="features_pca", k=10, seed=42):
    """
    Cette fonction entraîne un modèle de clustering KMeans sur les données.

    Paramètres :
    - df : DataFrame Spark contenant les vecteurs réduits
    - features_col : nom de la colonne des vecteurs d’entrée
    - k : nombre de clusters à créer
    - seed : graine pour la reproductibilité

    Retour :
    - modèle entraîné (KMeansModel)
    - DataFrame avec la prédiction des clusters pour chaque ligne
    """
    kmeans = KMeans(k=k, seed=seed, featuresCol=features_col, predictionCol="cluster")
    model = kmeans.fit(df)
    df_clustered = model.transform(df)
    return model, df_clustered

def evaluate_clustering(spark, model, df_clustered, features_col="features_pca"):
    """
    Évalue le clustering par Silhouette Score, sauf si 1 seul cluster détecté (cas invalide).

    Paramètres :
    - spark : SparkSession active
    - model : modèle KMeans entraîné
    - df_clustered : DataFrame Spark avec prédiction des clusters
    - features_col : nom de la colonne des vecteurs
    """
    num_clusters = df_clustered.select("cluster").distinct().count()
    if num_clusters <= 1:
        print("❌ Silhouette non calculable : un seul cluster détecté.")
        return float('nan')

    evaluator = ClusteringEvaluator(
        featuresCol=features_col,
        predictionCol="cluster",
        metricName="silhouette"
    )
    score = evaluator.evaluate(df_clustered)
    return score

def plot_kmeans_clusters(df_clustered, features_col="features_pca", label_col="label", sample_size=1000):
    """
    Cette fonction réalise une visualisation 2D des clusters trouvés par KMeans.
    Elle applique une réduction PCA locale (hors Spark) pour les besoins du graphique.

    Paramètres :
    - df_clustered : DataFrame Spark contenant les clusters
    - features_col : colonne des vecteurs (souvent "features_pca")
    - label_col : étiquette réelle (facultatif)
    - sample_size : nombre max d’échantillons pour le tracé (par défaut 1000)
    """
    # Récupération des données en local (via toPandas) avec échantillonnage
    df_sample = df_clustered.select(features_col, "cluster", label_col).limit(sample_size).toPandas()

    # Conversion des vecteurs Spark -> vecteurs numpy
    df_sample["vector"] = df_sample[features_col].apply(lambda v: v.toArray())

    # Matrice des vecteurs
    X = np.vstack(df_sample["vector"].values)

    # PCA locale en 2D (pour la visualisation)
    from sklearn.decomposition import PCA as skPCA
    X_2D = skPCA(n_components=2).fit_transform(X)

    # Construction DataFrame pour tracé
    df_plot = pd.DataFrame({
        "PC1": X_2D[:, 0],
        "PC2": X_2D[:, 1],
        "Cluster": df_sample["cluster"].astype(str),
        "Label": df_sample[label_col]
    })

    # Tracé des clusters
    plt.figure(figsize=(10, 7))
    sns.scatterplot(data=df_plot, x="PC1", y="PC2", hue="Cluster", palette="Set2", s=50, alpha=0.7)
    plt.title("Clusters KMeans projetés en 2D (PCA locale)")
    plt.xlabel("Composante principale 1")
    plt.ylabel("Composante principale 2")
    plt.legend(title="Cluster", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()
