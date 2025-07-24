# src/classification.py

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

def prepare_data(df, label_col="label", features_col="features_pca", train_ratio=0.8):
    """
    Cette fonction sépare le DataFrame en deux sous-ensembles :
    un ensemble d'entraînement et un ensemble de test.

    Paramètres :
    - df : DataFrame Spark contenant les colonnes 'label' et 'features'
    - label_col : nom de la colonne des labels
    - features_col : nom de la colonne des vecteurs d'entrée
    - train_ratio : proportion du jeu d'entraînement (par défaut 0.8)

    Retour :
    - df_train : DataFrame Spark pour l'entraînement
    - df_test : DataFrame Spark pour l'évaluation
    """
    df = df.select("path", label_col, features_col)
    df_train, df_test = df.randomSplit([train_ratio, 1 - train_ratio], seed=42)
    return df_train, df_test

def train_classifier(df_train, label_col="label", features_col="features_pca"):
    """
    Cette fonction construit un pipeline Spark ML contenant :
    - une étape d'encodage des labels (StringIndexer)
    - un classifieur RandomForest

    Le modèle est entraîné sur le DataFrame fourni.

    Retour :
    - pipeline_model : modèle Spark entraîné
    """
    indexer = StringIndexer(inputCol=label_col, outputCol="label_indexed")
    classifier = RandomForestClassifier(
        labelCol="label_indexed",
        featuresCol=features_col,
        predictionCol="prediction",
        probabilityCol="probability",
        maxDepth=10,
        numTrees=50,
        seed=42
    )
    pipeline = Pipeline(stages=[indexer, classifier])
    pipeline_model = pipeline.fit(df_train)
    return pipeline_model

def evaluate_classifier(model, df_test, label_col="label"):
    """
    Cette fonction applique le modèle sur le jeu de test,
    puis calcule les métriques de classification :
    - Accuracy
    - F1-score
    - Précision
    - Rappel

    Retour :
    - dictionnaire contenant :
        - "metrics" : dictionnaire de scores
        - "predictions" : DataFrame Spark avec les prédictions
    """
    predictions = model.transform(df_test)

    evaluator_acc = MulticlassClassificationEvaluator(
        labelCol="label_indexed", predictionCol="prediction", metricName="accuracy"
    )
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="label_indexed", predictionCol="prediction", metricName="f1"
    )
    evaluator_precision = MulticlassClassificationEvaluator(
        labelCol="label_indexed", predictionCol="prediction", metricName="weightedPrecision"
    )
    evaluator_recall = MulticlassClassificationEvaluator(
        labelCol="label_indexed", predictionCol="prediction", metricName="weightedRecall"
    )

    metrics = {
        "accuracy": evaluator_acc.evaluate(predictions),
        "f1_score": evaluator_f1.evaluate(predictions),
        "precision": evaluator_precision.evaluate(predictions),
        "recall": evaluator_recall.evaluate(predictions)
    }

    return {
        "metrics": metrics,
        "predictions": predictions
    }

def plot_confusion_matrix(predictions, label_col="label", prediction_col="prediction"):
    """
    Cette fonction trace la matrice de confusion en comparant les labels réels
    aux prédictions du modèle.

    Elle affiche un graphique heatmap lisible avec Seaborn.

    Paramètres :
    - predictions : DataFrame Spark avec colonnes 'label' et 'prediction'
    """
    # Extraction vers Pandas pour visualisation locale
    df_pd = predictions.select(label_col, prediction_col).toPandas()

    # Calcul de la matrice de confusion
    confusion = pd.crosstab(df_pd[label_col], df_pd[prediction_col], rownames=['Réel'], colnames=['Prédit'], dropna=False)

    plt.figure(figsize=(10, 8))
    sns.heatmap(confusion, annot=True, fmt='d', cmap='Blues')
    plt.title("Matrice de confusion (labels vs. prédictions)")
    plt.xticks(rotation=45, ha="right")
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.show()
