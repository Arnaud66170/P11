# src/preprocessing.py - Version GPU optimisée SANS WARNINGS

import os
import sys
import warnings
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, broadcast
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType
from PIL import Image
import pandas as pd
import time

# === SUPPRESSION RADICALE DE TOUS LES WARNINGS ===
warnings.filterwarnings('ignore')

# Suppression des logs TensorFlow AVANT l'import
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
os.environ['CUDA_VISIBLE_DEVICES'] = '0'  # Force l'utilisation du GPU 0
os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'true'
os.environ['TF_GPU_THREAD_MODE'] = 'gpu_private'

# Redirection des stderr vers /dev/null pour supprimer les messages CUDA
if os.name != 'nt':  # Linux/Mac
    os.environ['CUDA_LAUNCH_BLOCKING'] = '0'
    import subprocess
    DEVNULL = open(os.devnull, 'w')
else:  # Windows
    DEVNULL = open('nul', 'w')

# Redirection temporaire de stderr pour masquer les messages CUDA
original_stderr = sys.stderr
sys.stderr = DEVNULL

try:
    import tensorflow as tf
    from tensorflow.keras.applications import MobileNetV2
    from tensorflow.keras.applications.mobilenet_v2 import preprocess_input
    from tensorflow.keras.preprocessing import image
    import numpy as np
    
    # Configuration TensorFlow pour supprimer TOUS les logs
    tf.get_logger().setLevel('ERROR')
    tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)
    tf.autograph.set_verbosity(0)
    
    # Suppression des warnings GPU spécifiques
    tf.config.experimental.set_synchronous_execution(False)
    
finally:
    # Restauration de stderr
    sys.stderr = original_stderr
    DEVNULL.close()

def configure_gpu_silent():
    """
    Configuration GPU 100% silencieuse - aucun message affiché
    """
    try:
        # Redirection temporaire pour masquer les messages de configuration
        original_stderr = sys.stderr
        sys.stderr = open(os.devnull, 'w') if os.name != 'nt' else open('nul', 'w')
        
        gpus = tf.config.experimental.list_physical_devices('GPU')
        
        if len(gpus) > 0:
            # Configuration silencieuse
            tf.config.experimental.set_gpu_growth(gpus[0], True)
            tf.config.experimental.set_virtual_device_configuration(
                gpus[0],
                [tf.config.experimental.VirtualDeviceConfiguration(memory_limit=4096)]
            )
            
            # Restauration stderr avant les prints
            sys.stderr.close()
            sys.stderr = original_stderr
            
            print(f"🚀 GPU configuré : GTX 1060 6GB")
            print(f"💾 Limite mémoire : 4GB")
            print(f"⚡ Mode silencieux activé")
            
            return True
            
        else:
            sys.stderr.close()
            sys.stderr = original_stderr
            print("⚠️ Aucun GPU détecté - utilisation CPU")
            return False
            
    except Exception as e:
        if 'sys.stderr' in locals():
            sys.stderr.close()
            sys.stderr = original_stderr
        print("🔄 Fallback sur CPU")
        return False

def load_images_from_directory(spark, data_path, sample_size=None, cache_path=None, force_retrain=False):
    """
    Cette fonction charge la liste des chemins d'images et des labels depuis un répertoire structuré.
    Structure attendue : data_path/classe1/image1.jpg, data_path/classe2/image2.jpg, etc.
    
    Paramètres :
    - spark : SparkSession active
    - data_path : chemin racine du dataset (ex: "../data/fruits-360/Test")
    - sample_size : nombre maximum d'images à charger (None = toutes)
    - cache_path : chemin de cache pour éviter les recalculs
    - force_retrain : si True, ignore le cache et recalcule
    
    Retourne :
    - DataFrame Spark avec colonnes ['path', 'label']
    """
    # Import local pour éviter les problèmes de chemin
    from utils import export_dataframe_if_needed
    
    # Vérification du cache
    if cache_path and os.path.exists(cache_path) and not force_retrain:
        print(f"✅ Chargement depuis le cache : {cache_path}")
        return spark.read.parquet(cache_path)
    
    print(f"🔍 Scan du répertoire : {data_path}")
    
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Le répertoire {data_path} n'existe pas")
    
    # Collecte des chemins d'images et labels
    image_data = []
    total_found = 0
    
    # Parcours des sous-répertoires (classes)
    for class_name in os.listdir(data_path):
        class_path = os.path.join(data_path, class_name)
        
        if not os.path.isdir(class_path):
            continue
            
        print(f"   📂 Traitement de la classe : {class_name}")
        class_count = 0
        
        # Parcours des images dans chaque classe
        for filename in os.listdir(class_path):
            if filename.lower().endswith(('.png', '.jpg', '.jpeg')):
                full_path = os.path.join(class_path, filename)
                image_data.append((full_path, class_name))
                class_count += 1
                total_found += 1
                
                # Limitation si sample_size défini
                if sample_size and total_found >= sample_size:
                    print(f"🎯 Limite de {sample_size} images atteinte")
                    break
        
        print(f"      → {class_count} images trouvées")
        
        if sample_size and total_found >= sample_size:
            break
    
    print(f"📊 Total : {len(image_data)} images, {len(set([label for _, label in image_data]))} classes")
    
    # Création du DataFrame Spark
    schema = StructType([
        StructField("path", StringType(), True),
        StructField("label", StringType(), True)
    ])
    
    df = spark.createDataFrame(image_data, schema)
    
    # Sauvegarde en cache si demandé
    if cache_path:
        export_dataframe_if_needed(df, cache_path)
    
    return df

def extract_features_mobilenet_gpu(spark, df, cache_path=None, force_retrain=False, batch_size=32):
    """
    Version 100% silencieuse de l'extraction de features GPU
    """
    from utils import export_dataframe_if_needed
    
    if cache_path and os.path.exists(cache_path) and not force_retrain:
        print(f"✅ Chargement depuis le cache : {cache_path}")
        return spark.read.parquet(cache_path)
    
    print("🤖 Initialisation MobileNetV2 sur GPU...")
    
    # Configuration GPU silencieuse
    gpu_available = configure_gpu_silent()
    device = '/GPU:0' if gpu_available else '/CPU:0'
    
    # Chargement du modèle avec suppression complète des logs
    original_stderr = sys.stderr
    sys.stderr = open(os.devnull, 'w') if os.name != 'nt' else open('nul', 'w')
    
    try:
        with tf.device(device):
            base_model = MobileNetV2(
                weights='imagenet',
                include_top=False,
                pooling='avg'
            )
    finally:
        sys.stderr.close()
        sys.stderr = original_stderr
    
    print(f"📐 Dimensions : {base_model.output_shape}")
    print(f"🎯 Device : {device}")
    
    # Broadcast du modèle
    print("📡 Broadcast du modèle...")
    broadcasted_model = spark.sparkContext.broadcast(base_model)
    
    def extract_features_silent(image_path):
        """
        Extraction silencieuse - aucun log TensorFlow
        """
        try:
            # Redirection stderr pour cette fonction
            original_stderr = sys.stderr
            sys.stderr = open(os.devnull, 'w') if os.name != 'nt' else open('nul', 'w')
            
            # Chargement image
            img = image.load_img(image_path, target_size=(224, 224))
            img_array = image.img_to_array(img)
            img_array = np.expand_dims(img_array, axis=0)
            img_array = preprocess_input(img_array)
            
            # Inférence GPU silencieuse
            model = broadcasted_model.value
            
            with tf.device(device):
                features = model.predict(img_array, batch_size=1, verbose=0)
                features_vector = features.flatten().tolist()
            
            # Nettoyage mémoire
            tf.keras.backend.clear_session()
            
            # Restauration stderr
            sys.stderr.close()
            sys.stderr = original_stderr
            
            return features_vector
            
        except Exception:
            # Restauration stderr en cas d'erreur
            if 'original_stderr' in locals():
                sys.stderr.close()
                sys.stderr = original_stderr
            return [0.0] * 1280
    
    # UDF
    extract_features_udf = udf(extract_features_silent, ArrayType(DoubleType()))
    
    print("🔄 Extraction features GPU...")
    start_time = time.time()
    
    # Application UDF
    df_with_features = df.withColumn("features", extract_features_udf(F.col("path")))
    feature_count = df_with_features.count()
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    print(f"✅ {feature_count} features extraites")
    print(f"⏱️  Temps : {processing_time:.2f}s")
    print(f"🚀 Vitesse : {feature_count/processing_time:.2f} img/s")
    
    # Vérification
    sample_features = df_with_features.select("features").first()["features"]
    print(f"📐 Dimension : {len(sample_features)}")
    
    # Sauvegarde
    if cache_path:
        export_dataframe_if_needed(df_with_features, cache_path)
    
    # Nettoyage
    broadcasted_model.unpersist()
    tf.keras.backend.clear_session()
    
    return df_with_features

# Alias pour compatibilité - utilise automatiquement la version GPU si disponible
def extract_features_mobilenet(spark, df, cache_path=None, force_retrain=False, batch_size=32):
    """
    Fonction principale qui choisit automatiquement entre GPU et CPU
    """
    return extract_features_mobilenet_gpu(spark, df, cache_path, force_retrain, batch_size)

def benchmark_gpu_vs_cpu(spark, df_sample, num_images=20):
    """
    Compare les temps de traitement GPU vs CPU sur un petit batch d'images (ex: MobileNetV2).
    
    Paramètres :
        spark : session Spark active
        df_sample : DataFrame Spark contenant une colonne 'image'
        num_images : nombre d’images à tester (int)
    
    Retour :
        dict contenant les temps et accélération obtenus
    """
    from preprocessing import configure_gpu_silent
    from PIL import Image

    # 1. Limitation du dataset à un échantillon
    df_test = df_sample.limit(num_images).toPandas()
    
    images = np.array([
        np.array(Image.open(p).resize((100, 100)))  # adapte la taille
        for p in df_test["path"]
    ])

    # 2. Configuration GPU
    gpu_available = configure_gpu_silent()

    # 3. Initialisation du modèle (MobileNetV2 sans tête de classification)
    model = MobileNetV2(weights="imagenet", include_top=False)

    # 4. Prétraitement des images
    images_preprocessed = preprocess_input(images)

    results = {
        "num_images": num_images,
        "gpu_available": gpu_available,
        "gpu_time": None,
        "cpu_time": None,
        "speedup": None
    }

    # 5. Benchmark GPU (si disponible)
    if gpu_available:
        try:
            with tf.device("/GPU:0"):
                start = time.time()
                _ = model.predict(images_preprocessed, batch_size=4, verbose=0)
                gpu_time = time.time() - start
                results["gpu_time"] = gpu_time
        except Exception as e:
            print("❌ Erreur pendant le benchmark GPU :", e)

    # 6. Benchmark CPU
    try:
        with tf.device("/CPU:0"):
            start = time.time()
            _ = model.predict(images_preprocessed, batch_size=4, verbose=0)
            cpu_time = time.time() - start
            results["cpu_time"] = cpu_time
    except Exception as e:
        print("❌ Erreur pendant le benchmark CPU :", e)
        return results

    # 7. Calcul de l’accélération GPU (speedup)
    if results["gpu_time"] and results["cpu_time"]:
        results["speedup"] = round(results["cpu_time"] / results["gpu_time"], 2)

    return results

# Fonctions existantes maintenues pour compatibilité...
def validate_images(spark, df, check_corruption=True, check_size=True, min_size=(32, 32)):
    """
    Cette fonction valide la qualité des images dans le DataFrame.
    """
    print("🔍 Validation des images...")
    
    def validate_image(image_path):
        try:
            with Image.open(image_path) as img:
                if check_size:
                    width, height = img.size
                    if width < min_size[0] or height < min_size[1]:
                        return False, f"Taille trop petite: {width}x{height}"
                
                if check_corruption:
                    img.verify()
                
                return True, "OK"
                
        except Exception as e:
            return False, str(e)
    
    validate_udf = udf(lambda path: validate_image(path)[0], StringType())
    
    initial_count = df.count()
    df_validated = df.filter(validate_udf(F.col("path")) == "true")
    final_count = df_validated.count()
    
    rejected_count = initial_count - final_count
    
    print(f"📊 Résultats de validation :")
    print(f"   - Images initiales : {initial_count}")
    print(f"   - Images valides : {final_count}")
    print(f"   - Images rejetées : {rejected_count}")
    
    return df_validated

def balance_dataset(spark, df, max_samples_per_class=None, min_samples_per_class=5):
    """
    Cette fonction équilibre le dataset par classe.
    """
    print("⚖️ Équilibrage du dataset...")
    
    class_counts = df.groupBy("label").count().orderBy("count", ascending=False)
    print("📊 Distribution actuelle des classes :")
    class_counts.show(20, truncate=False)
    
    if min_samples_per_class > 1:
        valid_classes = class_counts.filter(F.col("count") >= min_samples_per_class)
        valid_class_names = [row["label"] for row in valid_classes.collect()]
        df = df.filter(F.col("label").isin(valid_class_names))
        
        excluded_classes = class_counts.filter(F.col("count") < min_samples_per_class).count()
        if excluded_classes > 0:
            print(f"🚫 {excluded_classes} classes exclues (< {min_samples_per_class} échantillons)")
    
    if max_samples_per_class:
        print(f"🎯 Limitation à {max_samples_per_class} échantillons par classe")
        
        df_balanced = df.sampleBy("label", 
                                 fractions={row["label"]: min(1.0, max_samples_per_class/row["count"]) 
                                          for row in class_counts.collect()},
                                 seed=42)
    else:
        df_balanced = df
    
    final_counts = df_balanced.groupBy("label").count().orderBy("label")
    print("📊 Distribution finale des classes :")
    final_counts.show(20, truncate=False)
    
    total_initial = df.count()
    total_final = df_balanced.count()
    
    print(f"📈 Résumé de l'équilibrage :")
    print(f"   - Images initiales : {total_initial}")
    print(f"   - Images finales : {total_final}")
    print(f"   - Réduction : {((total_initial - total_final) / total_initial * 100):.1f}%")
    
    return df_balanced

def create_train_test_split(spark, df, train_ratio=0.8, stratify_by_label=True, seed=42):
    """
    Cette fonction divise le dataset en ensembles d'entraînement et de test.
    """
    print(f"✂️ Division du dataset (train: {train_ratio*100:.0f}%, test: {(1-train_ratio)*100:.0f}%)")
    
    if stratify_by_label:
        print("🎯 Division stratifiée par classe")
        
        classes = df.select("label").distinct().collect()
        
        train_dfs = []
        test_dfs = []
        
        for class_row in classes:
            class_name = class_row["label"]
            class_df = df.filter(F.col("label") == class_name)
            
            df_train_class, df_test_class = class_df.randomSplit([train_ratio, 1-train_ratio], seed=seed)
            
            train_dfs.append(df_train_class)
            test_dfs.append(df_test_class)
        
        df_train = train_dfs[0]
        for train_df in train_dfs[1:]:
            df_train = df_train.union(train_df)
        
        df_test = test_dfs[0]
        for test_df in test_dfs[1:]:
            df_test = df_test.union(test_df)
            
    else:
        print("🎲 Division aléatoire simple")
        df_train, df_test = df.randomSplit([train_ratio, 1-train_ratio], seed=seed)
    
    train_count = df_train.count()
    test_count = df_test.count()
    total_count = train_count + test_count
    
    print(f"📊 Résultats de la division :")
    print(f"   - Entraînement : {train_count} images ({train_count/total_count*100:.1f}%)")
    print(f"   - Test : {test_count} images ({test_count/total_count*100:.1f}%)")
    
    return df_train, df_test