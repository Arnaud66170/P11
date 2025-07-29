#!/bin/bash
# Bootstrap optimisé pour P11 - Installation minimale mais efficace

set -e

# Mise à jour système (rapide)
sudo yum update -y

# Installation packages Python nécessaires uniquement
sudo pip3 install --upgrade pip setuptools wheel

# Packages strictement nécessaires (pas de bloat)
sudo pip3 install \
    pillow==10.0.1 \
    tensorflow==2.13.0 \
    keras==2.13.1 \
    numpy==1.24.3 \
    pandas==2.0.3 \
    pyarrow==12.0.1

# Configuration TensorFlow optimisée
export TF_CPP_MIN_LOG_LEVEL=2

# Vérification installation
python3 -c "import tensorflow as tf; import PIL; print('✅ Packages OK')"

echo "🎉 Bootstrap terminé avec succès"
