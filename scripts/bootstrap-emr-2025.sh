#!/bin/bash
set -e

echo "🔧 Bootstrap EMR P11 - Janvier 2025"
echo "=================================="

# Mise à jour système
sudo yum update -y

# Python et pip récents
sudo yum install -y python3-pip python3-devel

# Upgrade pip pour éviter les warnings
sudo pip3 install --upgrade pip setuptools wheel

# Librairies ML essentielles (versions compatibles)
sudo pip3 install --no-cache-dir \
    tensorflow==2.13.0 \
    pillow==10.0.0 \
    numpy==1.24.3 \
    pandas==2.0.3 \
    pyarrow==12.0.1 \
    matplotlib==3.7.2 \
    seaborn==0.12.2

# Configuration Spark pour ML
echo "spark.serializer org.apache.spark.serializer.KryoSerializer" >> /etc/spark/conf/spark-defaults.conf
echo "spark.sql.adaptive.enabled true" >> /etc/spark/conf/spark-defaults.conf
echo "spark.sql.adaptive.coalescePartitions.enabled true" >> /etc/spark/conf/spark-defaults.conf

echo "✅ Bootstrap terminé avec succès"
