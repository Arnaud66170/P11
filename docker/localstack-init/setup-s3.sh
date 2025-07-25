#!/bin/bash
# Fichier: docker/localstack-init/setup-s3.sh
# Ce script s'exécute automatiquement quand LocalStack démarre

echo "🚀 Initialisation S3 LocalStack pour P11..."

# Attendre que S3 soit prêt
awslocal s3 ls > /dev/null 2>&1
while [ $? -ne 0 ]; do
  echo "⏳ Attente S3 service..."
  sleep 2
  awslocal s3 ls > /dev/null 2>&1
done

# Création du bucket principal
echo "📁 Création bucket fruits-p11-local..."
awslocal s3 mb s3://fruits-p11-local

# Création de la structure de dossiers
echo "📂 Création structure dossiers..."
awslocal s3api put-object --bucket fruits-p11-local --key raw-data/
awslocal s3api put-object --bucket fruits-p11-local --key processed/
awslocal s3api put-object --bucket fruits-p11-local --key results/
awslocal s3api put-object --bucket fruits-p11-local --key logs/

# Vérification
echo "✅ Buckets créés:"
awslocal s3 ls

echo "✅ Structure bucket fruits-p11-local:"
awslocal s3 ls s3://fruits-p11-local/

echo "🎉 LocalStack S3 prêt pour P11 !"