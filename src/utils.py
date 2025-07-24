# src/utils.py

import os
import shutil
import warnings
from pyspark.sql import DataFrame
import tensorflow as tf
import gc

# Suppression des avertissements
warnings.filterwarnings('ignore')

def export_dataframe_if_needed(df, output_path, force_overwrite=True):
    """
    Cette fonction sauvegarde un DataFrame Spark au format Parquet avec gestion intelligente
    des répertoires existants et des erreurs.
    
    Paramètres :
    - df : DataFrame Spark à sauvegarder
    - output_path : chemin de sauvegarde (format .parquet)
    - force_overwrite : si True, écrase le fichier existant
    
    Retourne :
    - True si la sauvegarde a réussi, False sinon
    """
    try:
        # Création du répertoire parent si nécessaire
        parent_dir = os.path.dirname(output_path)
        if parent_dir and not os.path.exists(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)
            print(f"📁 Répertoire créé : {parent_dir}")
        
        # Suppression du répertoire existant si force_overwrite
        if os.path.exists(output_path) and force_overwrite:
            shutil.rmtree(output_path)
        
        # Sauvegarde du DataFrame
        df.write.mode("overwrite" if force_overwrite else "error").parquet(output_path)
        
        # Vérification de la taille du fichier
        if os.path.exists(output_path):
            size_mb = get_directory_size(output_path) / (1024 * 1024)
            print(f"💾 Sauvegarde réussie : {output_path} ({size_mb:.1f} MB)")
            return True
        else:
            return False
            
    except Exception as e:
        return False

def get_directory_size(directory_path):
    """
    Calcule la taille totale d'un répertoire en bytes.
    Utile pour les fichiers Parquet qui sont des répertoires.
    
    Paramètres :
    - directory_path : chemin du répertoire
    
    Retourne :
    - taille en bytes
    """
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(directory_path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if os.path.exists(filepath):
                    total_size += os.path.getsize(filepath)
    except Exception:
        pass  # Ignore silencieusement les erreurs
    
    return total_size

def clean_cache_directory(cache_path, confirm=False):
    """
    Cette fonction nettoie le répertoire de cache pour forcer les recalculs.
    Utile lors du débogage ou pour des tests propres.
    
    Paramètres :
    - cache_path : chemin du répertoire de cache à nettoyer
    - confirm : si True, demande confirmation avant suppression
    """
    if not os.path.exists(cache_path):
        return
    
    if confirm:
        response = input(f"🗑️  Supprimer le cache {cache_path} ? (o/n): ")
        if response.lower() != 'o':
            return
    
    try:
        shutil.rmtree(cache_path)
        print(f"✅ Cache supprimé : {cache_path}")
    except Exception:
        pass  # Ignore silencieusement les erreurs

def validate_spark_dataframe(df, expected_columns=None, min_rows=1):
    """
    Cette fonction valide la structure et le contenu d'un DataFrame Spark.
    Utile pour s'assurer de la cohérence des données entre les étapes.
    
    Paramètres :
    - df : DataFrame Spark à valider
    - expected_columns : liste des colonnes attendues (optionnel)
    - min_rows : nombre minimum de lignes attendu
    
    Retourne :
    - True si toutes les validations passent, False sinon
    """
    try:
        # Vérification de l'existence du DataFrame
        if df is None:
            return False
        
        # Vérification du nombre de lignes
        row_count = df.count()
        if row_count < min_rows:
            return False
        
        # Vérification des colonnes attendues
        if expected_columns:
            actual_columns = set(df.columns)
            expected_set = set(expected_columns)
            
            missing_columns = expected_set - actual_columns
            if missing_columns:
                return False
        
        print(f"✅ DataFrame valide : {row_count} lignes, {len(df.columns)} colonnes")
        return True
        
    except Exception:
        return False

def print_dataframe_summary(df, title="Résumé du DataFrame", show_sample=True, sample_size=5):
    """
    Cette fonction affiche un résumé détaillé d'un DataFrame Spark.
    Très utile pour le débogage et la validation des données.
    
    Paramètres :
    - df : DataFrame Spark à analyser
    - title : titre à afficher
    - show_sample : si True, affiche un échantillon des données
    - sample_size : nombre de lignes à afficher dans l'échantillon
    """
    print(f"\n{'='*60}")
    print(f"📊 {title.upper()}")
    print(f"{'='*60}")
    
    try:
        # Informations générales
        row_count = df.count()
        col_count = len(df.columns)
        
        print(f"📈 Nombre de lignes : {row_count:,}")
        print(f"📋 Nombre de colonnes : {col_count}")
        
        # Schéma du DataFrame
        print(f"\n🗂️  Schéma des données :")
        df.printSchema()
        
        # Échantillon des données
        if show_sample and row_count > 0:
            print(f"\n🔍 Échantillon des données (première {min(sample_size, row_count)} lignes) :")
            df.show(sample_size, truncate=False)
        
        # Statistiques sur les colonnes nulles
        print(f"\n📊 Statistiques des valeurs nulles :")
        for col_name in df.columns:
            null_count = df.filter(df[col_name].isNull()).count()
            if null_count > 0:
                null_percentage = (null_count / row_count) * 100 if row_count > 0 else 0
                print(f"   ⚠️  {col_name}: {null_count:,} nulls ({null_percentage:.1f}%)")
            else:
                print(f"   ✅ {col_name}: aucune valeur nulle")
                
    except Exception:
        pass  # Ignore silencieusement les erreurs
    
    print(f"{'='*60}\n")

def setup_project_directories(base_path="../"):
    """
    Cette fonction crée l'arborescence complète du projet si elle n'existe pas.
    Garantit que tous les répertoires nécessaires sont présents.
    
    Paramètres :
    - base_path : chemin de base du projet
    
    Retourne :
    - dictionnaire contenant tous les chemins créés
    """
    directories = {
        "data": os.path.join(base_path, "data"),
        "outputs": os.path.join(base_path, "outputs"), 
        "cache": os.path.join(base_path, "outputs", "cache"),
        "results_local": os.path.join(base_path, "outputs", "Results_Local"),
        "results_cloud": os.path.join(base_path, "outputs", "Results_Cloud"),
        "logs": os.path.join(base_path, "outputs", "logs"),
        "plots": os.path.join(base_path, "outputs", "plots")
    }
    
    created_dirs = []
    
    for name, path in directories.items():
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
            created_dirs.append(path)
    
    if created_dirs:
        print(f"📁 Répertoires créés : {len(created_dirs)}")
        for dir_path in created_dirs:
            print(f"   - {dir_path}")
    else:
        print("📁 Tous les répertoires existent déjà")
    
    return directories

def log_processing_step(step_name, details="", log_path="../outputs/logs/processing.log"):
    """
    Cette fonction enregistre les étapes de traitement dans un fichier de log.
    Utile pour tracer l'exécution et déboguer les problèmes.
    
    Paramètres :
    - step_name : nom de l'étape
    - details : détails supplémentaires (optionnel)
    - log_path : chemin du fichier de log
    """
    import datetime
    
    # Création du répertoire de log si nécessaire
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {step_name}"
    
    if details:
        log_entry += f" - {details}"
    
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(log_entry + "\n")
        
        print(f"📝 Log enregistré : {step_name}")
    except Exception:
        pass  # Ignore silencieusement les erreurs

def check_system_resources():
    """
    Cette fonction vérifie les ressources système disponibles.
    Utile pour optimiser les paramètres Spark selon l'environnement.
    
    Retourne :
    - dictionnaire contenant les informations système
    """
    try:
        import psutil
        
        resources = {
            "cpu_cores": psutil.cpu_count(logical=False),
            "cpu_threads": psutil.cpu_count(logical=True),
            "memory_gb": psutil.virtual_memory().total / (1024**3),
            "memory_available_gb": psutil.virtual_memory().available / (1024**3),
            "disk_free_gb": psutil.disk_usage('/').free / (1024**3)
        }
        
        print("🖥️  Ressources système détectées :")
        print(f"   - CPU : {resources['cpu_cores']} cœurs, {resources['cpu_threads']} threads")
        print(f"   - RAM : {resources['memory_gb']:.1f} GB total, {resources['memory_available_gb']:.1f} GB disponible")
        print(f"   - Disque : {resources['disk_free_gb']:.1f} GB libres")
        
        # Recommandations Spark
        recommended_executor_cores = min(4, resources['cpu_cores'])
        recommended_executor_memory = int(resources['memory_available_gb'] * 0.6)
        
        print(f"\n💡 Recommandations Spark :")
        print(f"   - spark.executor.cores: {recommended_executor_cores}")
        print(f"   - spark.executor.memory: {recommended_executor_memory}g")
        
        return resources
        
    except ImportError:
        print("⚠️  psutil non disponible - informations système limitées")
        return {}

def clean_gpu_cache(verbose: bool = True):
    """
    Libère la mémoire GPU et nettoie les objets inutiles de TensorFlow/Keras.
    Utile pour éviter les fuites mémoire en environnement GPU.
    """
    try:
        # Suppression explicite des graphes et objets
        tf.keras.backend.clear_session()
        gc.collect()

        # Réinitialisation des allocations mémoire GPU (si applicable)
        gpus = tf.config.list_physical_devices('GPU')
        if gpus and verbose:
            print("[INFO] Mémoire GPU nettoyée avec succès.")

    except Exception as e:
        print(f"[ERREUR] Impossible de nettoyer la mémoire GPU : {e}")