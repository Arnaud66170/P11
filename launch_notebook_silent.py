#!/usr/bin/env python3
# launch_notebook_silent.py - Lanceur Jupyter sans warnings TensorFlow/CUDA

import os
import sys
import subprocess
import warnings

def setup_silent_environment():
    """
    Configure l'environnement pour supprimer TOUS les warnings
    """
    # Variables d'environnement pour supprimer les logs TensorFlow/CUDA
    silent_env = {
        'TF_CPP_MIN_LOG_LEVEL': '3',
        'PYTHONWARNINGS': 'ignore',
        'TF_ENABLE_ONEDNN_OPTS': '0',
        'CUDA_VISIBLE_DEVICES': '0',
        'TF_FORCE_GPU_ALLOW_GROWTH': 'true',
        'TF_GPU_THREAD_MODE': 'gpu_private',
        'CUDA_LAUNCH_BLOCKING': '0',
        'TF_DETERMINISTIC_OPS': '1',
        'TF_DISABLE_SEGMENT_REDUCTION_OP_DETERMINISM_EXCEPTIONS': '1'
    }
    
    # Ajout à l'environnement actuel
    for key, value in silent_env.items():
        os.environ[key] = value
    
    print("🔇 Environnement silencieux configuré")
    print("🚀 Variables GPU configurées pour GTX 1060")

def launch_jupyter():
    """
    Lance Jupyter avec suppression complète des warnings
    """
    print("📝 Lancement de Jupyter Notebook...")
    
    # Commande Jupyter avec redirection des erreurs
    cmd = [
        sys.executable, '-m', 'jupyter', 'notebook',
        '--notebook-dir=notebooks',
        '--no-browser',
        '--port=8888'
    ]
    
    # Lancement avec redirection stderr vers null
    if os.name != 'nt':  # Linux/Mac
        with open('/dev/null', 'w') as devnull:
            subprocess.run(cmd, stderr=devnull)
    else:  # Windows
        with open('nul', 'w') as devnull:
            subprocess.run(cmd, stderr=devnull)

if __name__ == "__main__":
    print("🎯 Lancement du notebook P11 en mode silencieux")
    print("=" * 50)
    
    setup_silent_environment()
    
    print("\n💡 Instructions :")
    print("1. Le notebook va s'ouvrir sans warnings TensorFlow/CUDA")
    print("2. Ton GPU GTX 1060 sera automatiquement détecté")
    print("3. Aucun message d'erreur CUDA ne s'affichera")
    
    try:
        launch_jupyter()
    except KeyboardInterrupt:
        print("\n👋 Arrêt du notebook")
    except Exception as e:
        print(f"❌ Erreur : {e}")
        print("🔄 Lance manuellement : jupyter notebook notebooks/01_fruits_pipeline_cloud.ipynb")