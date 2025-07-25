# src/emr_simulation.py
"""
Module de configuration pour simulation EMR locale
Permet de switcher entre local/simulation/production sans changer le code
"""

import os
import sys
from pyspark.sql import SparkSession

class EMRConfig:
    """
    Configuration centralisée pour les 3 modes:
    - local: Spark local + filesystem local
    - simulation: Spark local + LocalStack S3  
    - production: EMR + AWS S3
    """
    
    def __init__(self, mode="local"):
        self.mode = mode
        self._setup_paths()
        self._setup_spark_config()
        
        print(f"🔧 Configuration EMR: Mode {mode.upper()}")
    
    def _setup_paths(self):
        """Configure les chemins selon le mode"""
        if self.mode == "local":
            self.data_path = "data/fruits-360/"
            self.output_path = "outputs/"
            self.cache_path = "outputs/cache/"
            
        elif self.mode == "simulation":
            # LocalStack S3 (localhost:4566)
            self.data_path = "s3a://fruits-p11-local/raw-data/"
            self.output_path = "s3a://fruits-p11-local/results/"
            self.cache_path = "s3a://fruits-p11-local/cache/"
            
        elif self.mode == "production":
            # AWS S3 réel
            bucket_name = "fruits-p11-arnaud"  # À personnaliser
            self.data_path = f"s3a://{bucket_name}/raw-data/"
            self.output_path = f"s3a://{bucket_name}/results/"
            self.cache_path = f"s3a://{bucket_name}/cache/"
        
        else:
            raise ValueError(f"Mode non supporté: {self.mode}")
    
    def _setup_spark_config(self):
        """Configure Spark selon le mode"""
        self.spark_config = {
            # Configuration commune à tous les modes
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true", 
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
            "spark.sql.parquet.writeLegacyFormat": "true"
        }
        
        if self.mode == "local":
            self.spark_config.update({
                "spark.master": "local[4]",
                "spark.executor.memory": "2g",
                "spark.driver.memory": "2g"
            })
            
        elif self.mode == "simulation":
            # Configuration pour LocalStack S3
            self.spark_config.update({
                "spark.master": "local[4]",
                "spark.executor.memory": "2g", 
                "spark.driver.memory": "2g",
                
                # Configuration S3 pour LocalStack
                "spark.hadoop.fs.s3a.endpoint": "http://localhost:4566",
                "spark.hadoop.fs.s3a.access.key": "test",
                "spark.hadoop.fs.s3a.secret.key": "test",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            })
            
        elif self.mode == "production":
            # EMR gère automatiquement la plupart des configs
            self.spark_config.update({
                # Ces configs seront appliquées sur EMR
                "spark.executor.memory": "7g",
                "spark.driver.memory": "4g"
            })
    
    def get_spark_session(self, app_name="FruitsProcessing"):
        """Crée une session Spark configurée pour le mode choisi"""
        
        if self.mode == "production" and os.path.exists('/emr'):
            # Sur EMR, utilise la session existante si disponible
            spark = SparkSession.builder.appName(app_name).getOrCreate()
            
            # Applique juste les configs additionnelles
            for key, value in self.spark_config.items():
                if not key.startswith("spark.master"):  # EMR gère le master
                    spark.conf.set(key, value)
                    
        else:
            # Mode local ou simulation
            builder = SparkSession.builder.appName(app_name)
            
            # Applique toutes les configurations
            for key, value in self.spark_config.items():
                builder = builder.config(key, value)
            
            spark = builder.getOrCreate()
        
        # Log level pour réduire le bruit
        spark.sparkContext.setLogLevel("WARN")
        
        self._print_session_info(spark)
        return spark
    
    def _print_session_info(self, spark):
        """Affiche les infos de la session créée"""
        print(f"   📊 Spark Version: {spark.version}")
        print(f"   🖥️  Master: {spark.sparkContext.master}")
        print(f"   🔢 Cores: {spark.sparkContext.defaultParallelism}")
        print(f"   📁 Data Path: {self.data_path}")
        print(f"   💾 Output Path: {self.output_path}")
    
    def test_configuration(self):
        """Test rapide de la configuration"""
        print(f"\n🧪 Test configuration mode {self.mode}...")
        
        try:
            spark = self.get_spark_session("ConfigTest")
            
            # Test basique de fonctionnement
            test_data = [(1, "test"), (2, "config")]
            df = spark.createDataFrame(test_data, ["id", "value"])
            count = df.count()
            
            print(f"✅ Spark fonctionne: {count} lignes test")
            
            if self.mode in ["simulation", "production"]:
                # Test d'accès S3 (uniquement si S3 configuré)
                try:
                    # Tentative de listing du bucket
                    spark.read.text(self.data_path).limit(1).collect()
                    print("✅ Accès S3 fonctionnel")
                except:
                    print("⚠️  S3 non accessible (normal si pas de données uploadées)")
            
            spark.stop()
            return True
            
        except Exception as e:
            print(f"❌ Erreur configuration: {e}")
            return False

def detect_environment():
    """Détecte automatiquement l'environnement d'exécution"""
    if os.path.exists('/emr'):
        return "production"
    elif os.environ.get('LOCALSTACK_ENDPOINT'):
        return "simulation" 
    else:
        return "local"

# Fonction utilitaire pour migration facile
def get_default_config():
    """Retourne une config par défaut selon l'environnement détecté"""
    env = detect_environment()
    return EMRConfig(env)

if __name__ == "__main__":
    # Test des 3 modes
    for mode in ["local", "simulation", "production"]:
        print(f"\n{'='*50}")
        print(f"TEST MODE: {mode.upper()}")
        print(f"{'='*50}")
        
        config = EMRConfig(mode)
        success = config.test_configuration()
        
        if success:
            print(f"✅ Mode {mode} OK")
        else:
            print(f"❌ Mode {mode} KO")