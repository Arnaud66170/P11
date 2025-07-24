#!/bin/bash
# install_cuda_gpu.sh - Installation complète CUDA pour P11

echo "🎯 INSTALLATION CUDA POUR GTX 1060 - UBUNTU"
echo "================================================"

# Vérification que le script est lancé depuis l'environnement virtuel
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "❌ Environnement virtuel non activé"
    echo "💡 Active d'abord : source venv_p11/bin/activate"
    exit 1
fi

echo "✅ Environnement virtuel détecté : $VIRTUAL_ENV"

# Étape 1 : Mise à jour système
echo -e "\n🔄 Mise à jour du système..."
sudo apt update

# Étape 2 : Installation CUDA Toolkit
echo -e "\n🔧 Installation CUDA Toolkit..."
sudo apt install -y nvidia-cuda-toolkit

# Étape 3 : Vérification installation
echo -e "\n🔍 Vérification CUDA..."
if command -v nvcc &> /dev/null; then
    echo "✅ CUDA Toolkit installé :"
    nvcc --version
else
    echo "❌ Erreur installation CUDA"
    exit 1
fi

# Étape 4 : Configuration variables d'environnement
echo -e "\n⚙️  Configuration variables d'environnement..."

# Vérification si déjà configuré
if grep -q "cuda" ~/.bashrc; then
    echo "✅ Variables CUDA déjà configurées dans ~/.bashrc"
else
    echo "🔧 Ajout variables CUDA à ~/.bashrc..."
    echo '' >> ~/.bashrc
    echo '# CUDA Configuration for P11' >> ~/.bashrc
    echo 'export PATH=/usr/local/cuda/bin:$PATH' >> ~/.bashrc
    echo 'export LD_LIBRARY_PATH=/usr/local/cuda/lib64:$LD_LIBRARY_PATH' >> ~/.bashrc
    echo '✅ Variables ajoutées à ~/.bashrc'
fi

# Chargement des variables pour cette session
export PATH=/usr/local/cuda/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/cuda/lib64:$LD_LIBRARY_PATH

# Étape 5 : Vérification librairies CUDA
echo -e "\n🔍 Vérification librairies CUDA..."
cuda_libs=$(ldconfig -p | grep cuda | wc -l)
if [ $cuda_libs -gt 0 ]; then
    echo "✅ $cuda_libs librairies CUDA trouvées"
    echo "🎯 Principales librairies :"
    ldconfig -p | grep -E "(libcudart|libcublas|libcurand)" | head -5
else
    echo "⚠️ Peu de librairies CUDA détectées"
    echo "💡 Un redémarrage peut être nécessaire"
fi

# Étape 6 : Réinstallation TensorFlow
echo -e "\n🤖 Réinstallation TensorFlow avec support GPU..."
pip uninstall -y tensorflow tensorflow-gpu
pip install tensorflow[and-cuda]==2.13.0

# Étape 7 : Test final
echo -e "\n🚀 Test final GPU..."
python3 << EOF
import tensorflow as tf
import os

print("="*50)
print("TEST TENSORFLOW GPU")
print("="*50)

print(f"TensorFlow version : {tf.__version__}")
print(f"CUDA built : {tf.test.is_built_with_cuda()}")

gpus = tf.config.list_physical_devices('GPU')
print(f"GPUs détectés : {len(gpus)}")

if gpus:
    print("✅ GPU DÉTECTÉ !")
    for i, gpu in enumerate(gpus):
        print(f"   GPU {i}: {gpu.name}")
    
    # Test opération simple
    try:
        with tf.device('/GPU:0'):
            a = tf.constant([1.0, 2.0])
            b = tf.constant([3.0, 4.0])
            c = tf.add(a, b)
            print(f"✅ Test calcul GPU réussi : {c.numpy()}")
    except Exception as e:
        print(f"❌ Erreur calcul GPU : {e}")
else:
    print("❌ AUCUN GPU DÉTECTÉ")
    print("💡 Un redémarrage peut être nécessaire")

print("="*50)
EOF

# Étape 8 : Résumé final
echo -e "\n🎯 INSTALLATION TERMINÉE"
echo "=========================="

if python3 -c "import tensorflow as tf; exit(0 if tf.config.list_physical_devices('GPU') else 1)" 2>/dev/null; then
    echo "✅ SUCCÈS - GPU prêt pour P11 !"
    echo "🚀 Tu peux maintenant utiliser l'accélération GPU"
    echo ""
    echo "💡 Pour tester dans ton projet :"
    echo "   cd ~/P11/2-python"
    echo "   python3 gpu_diagnostic.py"
else
    echo "⚠️ GPU non encore détecté"
    echo "🔄 Solutions :"
    echo "   1. Redémarre Ubuntu : sudo reboot"
    echo "   2. Puis relance le test"
    echo "   3. Vérifie les variables : source ~/.bashrc"
fi

echo ""
echo "🔥 Prêt à faire fonctionner ton GTX 1060 à fond !"