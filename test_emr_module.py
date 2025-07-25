#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test du module emr_simulation.py WSL2
À exécuter dans WSL2 : python test_emr_module.py
LocalStack doit être démarré : cd docker && docker-compose up -d
"""

import sys
import os

# Ajout du chemin src pour les imports
sys.path.append('./src')

def test_imports():
    """Test des imports du module"""
    print("=== Test Import Module ===")
    try:
        from emr_simulation import EMRSimulation, get_spark_session, get_s3_client
        print("✅ Import emr_simulation.py OK")
        return True
    except ImportError as e:
        print(f"❌ Erreur import : {e}")
        print("Vérifiez que le fichier src/emr_simulation.py existe")
        return False

def test_local_mode():
    """Test du mode local"""
    print("\n=== Test Configuration Mode Local ===")
    try:
        from emr_simulation import EMRSimulation
        config = EMRSimulation(mode="local")
        storage_path = config.get_storage_path()
        print(f"✅ Config local OK : {storage_path}")
        return True
    except Exception as e:
        print(f"❌ Erreur config local : {e}")
        return False

def test_spark_local():
    """Test Spark en mode local"""
    print("\n=== Test Spark Session Local ===")
    try:
        from emr_simulation import get_spark_session
        spark = get_spark_session(mode="local")
        print(f"✅ Spark local OK : version {spark.version}")
        
        # Test rapide DataFrame
        test_data = [(1, "pomme"), (2, "orange"), (3, "banane")]
        df = spark.createDataFrame(test_data, ["id", "fruit"])
        count = df.count()
        print(f"✅ Test DataFrame OK : {count} lignes")
        
        spark.stop()
        return True
    except Exception as e:
        print(f"❌ Erreur Spark : {e}")
        return False

def test_simulation_mode():
    """Test du mode simulation (LocalStack)"""
    print("\n=== Test Configuration Simulation ===")
    try:
        from emr_simulation import EMRSimulation, get_s3_client
        
        config_sim = EMRSimulation(mode="simulation")
        storage_path = config_sim.get_storage_path()
        print(f"✅ Config simulation OK : {storage_path}")
        
        # Test S3 local
        s3_client = get_s3_client(mode="simulation")
        buckets = s3_client.list_buckets()
        bucket_count = len(buckets['Buckets'])
        print(f"✅ S3 Local accessible : {bucket_count} buckets")
        
        # Affichage des buckets
        for bucket in buckets['Buckets']:
            print(f"   - {bucket['Name']}")
            
        return True
        
    except Exception as e:
        print(f"⚠️ Simulation non accessible : {e}")
        print("Vérifiez que LocalStack est démarré : docker ps")
        return False

def main():
    """Test principal"""
    print("🧪 TEST MODULE EMR_SIMULATION")
    print("=" * 50)
    
    # Tests séquentiels
    tests = [
        ("Import", test_imports),
        ("Local Mode", test_local_mode), 
        ("Spark Local", test_spark_local),
        ("Simulation Mode", test_simulation_mode)
    ]
    
    results = {}
    for test_name, test_func in tests:
        results[test_name] = test_func()
        if not results[test_name] and test_name in ["Import", "Local Mode"]:
            print(f"❌ Test {test_name} échoué - Arrêt des tests")
            break
    
    # Résumé
    print("\n" + "=" * 50)
    print("📊 RÉSUMÉ DES TESTS")
    for test_name, success in results.items():
        status = "✅ OK" if success else "❌ ÉCHEC"
        print(f"{test_name:15} : {status}")
    
    if all(results.values()):
        print("\n🎉 Tous les tests passent ! Prêt pour l'intégration notebook.")
    else:
        print("\n⚠️ Certains tests échouent. Debug nécessaire.")

if __name__ == "__main__":
    main()