# src/emr_simulation.py
"""
Module de simulation EMR/S3 pour le projet P11 - Version optimisée
Permet de basculer entre local, simulation LocalStack et production AWS
avec gestion intelligente de la mémoire et support GPU
"""

import os
import boto3
from pyspark.sql import SparkSession
from pathlib import Path

class EMRSimulation:
    """
    Classe principale pour gérer les différents modes d'exécution :
    - local : Filesystem local + Spark local
    - simulation : LocalStack S3 + Spark local  
    - production : AWS S3 + EMR
    """
    
    def __init__(self, mode="local", enable_gpu=False):
        """
        Initialise la configuration selon le mode choisi
        
        Args:
            mode (str): "local", "simulation" ou "production"
            enable_gpu (bool): Active les optimisations GPU pour mode local
        """
        self.mode = mode
        self.enable_gpu = enable_gpu
        self.config = self._get_config()
        print(f"🔧 EMRSimulation initialisé en mode: {mode}")
        if enable_gpu and mode == "local":
            print("🚀 Optimisations GPU activées")
    
    def _get_config(self):
        """Retourne la configuration selon le mode"""
        if self.mode == "local":
            return {
                "spark_master": "local[*]",
                "storage_type": "filesystem",
                "base_path": "./outputs",
                "s3_endpoint": None
            }
        elif self.mode == "simulation":
            return {
                "spark_master": "local[*]",
                "storage_type": "s3_local",
                "base_path": "s3a://fruits-p11-simulation",
                "s3_endpoint": "http://localhost:4566"
            }
        elif self.mode == "production":
            return {
                "spark_master": None,  # Géré par EMR
                "storage_type": "s3_aws",
                "base_path": "s3a://fruits-p11-production",
                "s3_endpoint": None
            }
        else:
            raise ValueError(f"Mode non supporté: {self.mode}")
    
    def get_memory_config(self):
        """
        Retourne la configuration mémoire optimisée selon le mode et GPU
        """
        if self.mode == "local":
            if self.enable_gpu:
                # GPU local optimisé (GTX 1060 6GB)
                return {
                    "spark.executor.memory": "6g",
                    "spark.driver.memory": "4g",
                    "spark.executor.memoryFraction": "0.8"
                }
            else:
                # CPU local standard
                return {
                    "spark.executor.memory": "4g",
                    "spark.driver.memory": "2g"
                }
                
        elif self.mode == "simulation":
            # LocalStack optimisé (moins de RAM nécessaire)
            return {
                "spark.executor.memory": "3g",
                "spark.driver.memory": "2g"
            }
            
        elif self.mode == "production":
            # AWS EMR optimisé (instances m5.xlarge = 16GB RAM)
            return {
                "spark.executor.memory": "7g",
                "spark.driver.memory": "4g",
                "spark.executor.memoryFraction": "0.8"
            }
    
    def get_spark_config(self):
        """Retourne la configuration Spark complète adaptée au mode"""
        # Configuration de base commune
        base_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
        }
        
        # Ajout configuration mémoire
        memory_config = self.get_memory_config()
        base_config.update(memory_config)
        
        # Optimisations GPU et Arrow
        if self.mode == "local" and self.enable_gpu:
            base_config.update({
                "spark.sql.execution.arrow.pyspark.enabled": "true"
            })
        elif self.mode == "production":
            base_config.update({
                "spark.sql.execution.arrow.pyspark.enabled": "true"
            })
        
        # Configuration S3 selon le mode
        if self.mode == "simulation":
            # Configuration pour LocalStack S3
            base_config.update({
                "spark.hadoop.fs.s3a.endpoint": "http://localhost:4566",
                "spark.hadoop.fs.s3a.access.key": "test",
                "spark.hadoop.fs.s3a.secret.key": "test",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.sql.parquet.writeLegacyFormat": "true"
            })
        elif self.mode == "production":
            # Configuration pour AWS S3 (utilise les credentials IAM)
            base_config.update({
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider",
                "spark.sql.parquet.writeLegacyFormat": "true"
            })
        
        return base_config
    
    def get_storage_path(self, subpath=""):
        """Retourne le chemin de stockage selon le mode"""
        if self.mode == "local":
            base = Path(self.config["base_path"])
            base.mkdir(parents=True, exist_ok=True)
            if subpath:
                full_path = base / subpath
                full_path.parent.mkdir(parents=True, exist_ok=True)
                return str(full_path)
            return str(base)
        else:
            # Modes S3 (simulation ou production)
            if subpath:
                return f"{self.config['base_path']}/{subpath}"
            return self.config["base_path"]
    
    def get_s3_config(self):
        """Retourne la configuration S3 selon le mode"""
        if self.mode == "local":
            return None
        elif self.mode == "simulation":
            return {
                "endpoint_url": "http://localhost:4566",
                "aws_access_key_id": "test",
                "aws_secret_access_key": "test",
                "region_name": "us-east-1"
            }
        else:  # production
            return {
                "region_name": "eu-west-1"  # Conformité RGPD
            }
    
    def test_s3_connection(self):
        """Test rapide de connexion S3"""
        if self.mode == "local":
            return True
        
        try:
            s3_client = get_s3_client(mode=self.mode)
            if s3_client:
                # Test simple : lister les buckets
                s3_client.list_buckets()
                return True
        except Exception as e:
            print(f"❌ Test S3 échoué: {e}")
            return False

def get_spark_session(mode="local", app_name="FruitsPipeline", enable_gpu=False):
    """
    Crée une session Spark adaptée au mode d'exécution SANS conflit de configuration
    
    Args:
        mode (str): "local", "simulation" ou "production"
        app_name (str): Nom de l'application Spark
        enable_gpu (bool): Active les optimisations GPU locales
    
    Returns:
        SparkSession: Session Spark configurée
    """
    # Détection automatique GPU si pas spécifié explicitement
    if mode == "local" and not enable_gpu:
        try:
            import tensorflow as tf
            gpus = tf.config.list_physical_devices('GPU')
            enable_gpu = len(gpus) > 0
            if enable_gpu:
                print("🚀 GPU détecté automatiquement")
        except:
            pass
    
    emr_sim = EMRSimulation(mode=mode, enable_gpu=enable_gpu)
    
    # ✅ CONFIGURATION UNIFIÉE (pas de conflit)
    final_config = emr_sim.get_spark_config()
    
    # Construction de la session Spark
    builder = SparkSession.builder.appName(app_name)
    
    # Ajout des packages Hadoop nécessaires pour S3A
    builder = builder.config("spark.jars.packages", 
                         "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.696")

    # Configuration S3A spécifique pour mode simulation ou production
    if mode in ["simulation", "production"]:
        builder = builder \
            .config("spark.hadoop.fs.s3a.access.key", "dummy") \
            .config("spark.hadoop.fs.s3a.secret.key", "dummy") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Configuration du master (seulement pour local et simulation)
    if emr_sim.config["spark_master"]:
        builder = builder.master(emr_sim.config["spark_master"])
    
    # 🔧 APPLICATION CONFIG UNIFIÉE (une seule fois, pas de conflit)
    for key, value in final_config.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    
    # Configuration post-création
    spark.sparkContext.setLogLevel("WARN")
    
    # Affichage config appliquée
    memory_config = emr_sim.get_memory_config()
    print(f"✅ Session Spark créée - Mode: {mode} - Version: {spark.version}")
    print(f"📊 Config mémoire: {memory_config['spark.executor.memory']} executor / {memory_config['spark.driver.memory']} driver")
    print(f"🎯 Cores disponibles: {spark.sparkContext.defaultParallelism}")
    
    return spark

def get_s3_client(mode="local"):
    """
    Crée un client S3 adapté au mode d'exécution
    
    Args:
        mode (str): "local", "simulation" ou "production"
    
    Returns:
        boto3.client: Client S3 configuré (None pour mode local)
    """
    if mode == "local":
        print("💡 Mode local : pas de client S3 nécessaire")
        return None
    
    emr_sim = EMRSimulation(mode=mode)
    s3_config = emr_sim.get_s3_config()
    
    if s3_config:
        s3_client = boto3.client('s3', **s3_config)
        print(f"✅ Client S3 créé - Mode: {mode}")
        return s3_client
    else:
        raise ValueError(f"Configuration S3 impossible pour le mode: {mode}")

def create_bucket_if_not_exists(mode="simulation", bucket_name="fruits-p11-simulation"):
    """
    Crée un bucket S3 s'il n'existe pas déjà
    
    Args:
        mode (str): Mode d'exécution
        bucket_name (str): Nom du bucket à créer
    
    Returns:
        bool: True si bucket créé ou existe déjà
    """
    if mode == "local":
        print("💡 Mode local : pas de bucket S3 nécessaire")
        return True
    
    try:
        s3_client = get_s3_client(mode=mode)
        
        # Vérification si le bucket existe
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"✅ Bucket {bucket_name} existe déjà")
            return True
        except:
            # Le bucket n'existe pas, on le crée
            if mode == "simulation":
                s3_client.create_bucket(Bucket=bucket_name)
            else:  # production
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'}
                )
            print(f"✅ Bucket {bucket_name} créé avec succès")
            return True
            
    except Exception as e:
        print(f"❌ Erreur création bucket {bucket_name}: {e}")
        return False

def get_optimal_config_for_hardware():
    """
    Détecte automatiquement la config optimale selon le hardware disponible
    """
    config_info = {
        "gpu_available": False,
        "recommended_mode": "local",
        "memory_recommendation": "4g/2g"
    }
    
    # Détection GPU
    try:
        import tensorflow as tf
        gpus = tf.config.list_physical_devices('GPU')
        if len(gpus) > 0:
            config_info["gpu_available"] = True
            config_info["memory_recommendation"] = "6g/4g"
            print(f"🚀 GPU détecté: {gpus[0].name}")
    except:
        print("💻 Mode CPU détecté")
    
    # Détection RAM disponible
    try:
        import psutil
        ram_gb = psutil.virtual_memory().total / (1024**3)
        if ram_gb >= 16:
            config_info["memory_recommendation"] = "6g/4g" if config_info["gpu_available"] else "4g/2g"
        elif ram_gb >= 8:
            config_info["memory_recommendation"] = "4g/2g"
        else:
            config_info["memory_recommendation"] = "2g/1g"
        print(f"💾 RAM détectée: {ram_gb:.1f} GB")
    except:
        print("💾 RAM non détectable")
    
    return config_info

# Tests rapides si le module est exécuté directement
if __name__ == "__main__":
    print("🧪 Test du module emr_simulation.py - Version optimisée")
    
    # Test détection hardware
    print("\n--- Détection Hardware ---")
    hw_config = get_optimal_config_for_hardware()
    print(f"Config recommandée: {hw_config}")
    
    # Test des différents modes
    for mode in ["local", "simulation", "production"]:
        try:
            print(f"\n--- Test mode {mode} ---")
            enable_gpu = hw_config["gpu_available"] and mode == "local"
            emr = EMRSimulation(mode=mode, enable_gpu=enable_gpu)
            print(f"Storage path: {emr.get_storage_path('test')}")
            print(f"Memory config: {emr.get_memory_config()}")
            print(f"S3 config: {emr.get_s3_config()}")
            
            # Test connexion S3 si applicable
            if mode != "local":
                s3_ok = emr.test_s3_connection()
                print(f"Test S3: {'✅' if s3_ok else '❌'}")
                
        except Exception as e:
            print(f"❌ Erreur mode {mode}: {e}")
    
    print("\n✅ Tests terminés")