#!/bin/bash

# SCRIPT: deploy-emr-p11.sh
# EMPLACEMENT: ~/P11/2-python/scripts/deploy-emr-p11.sh
# 
# Script EMR P11 - Version corrigée avec instances compatibles
# Auteur: Nono66 - Projet P11 Big Data
# Date: 2025

set -e

echo "🚀 Déploiement EMR P11 - Version Corrigée"
echo "=========================================="

# Configuration
CLUSTER_NAME="p11-fruits-demo-2025"
BUCKET_NAME="fruits-p11-production"
REGION="eu-west-1"
KEY_NAME="p11-keypair"

# Types d'instances compatibles 2025
MASTER_INSTANCE="m5.xlarge"     # Plus petit que m5.large mais compatible
WORKER_INSTANCE="m5.xlarge"     # Même type pour simplicité
WORKER_COUNT=1                   # Juste 1 worker pour économiser

# Répertoires
AWS_CONFIG_DIR="../aws-config"
mkdir -p "$AWS_CONFIG_DIR"

echo "📦 Création du script bootstrap optimisé..."

# Bootstrap script optimisé pour 2025
cat > bootstrap-emr-2025.sh << 'EOF'
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

# Installation Zeppelin (si pas déjà fait)
if [ ! -d "/opt/zeppelin" ]; then
    echo "📓 Installation Zeppelin..."
    cd /opt
    sudo wget -q https://archive.apache.org/dist/zeppelin/zeppelin-0.10.1/zeppelin-0.10.1-bin-all.tgz
    sudo tar -xzf zeppelin-0.10.1-bin-all.tgz
    sudo mv zeppelin-0.10.1-bin-all zeppelin
    sudo chown -R hadoop:hadoop zeppelin
    rm -f zeppelin-0.10.1-bin-all.tgz
    
    # Configuration Zeppelin pour écouter sur toutes les interfaces
    sudo tee /opt/zeppelin/conf/zeppelin-site.xml > /dev/null << 'ZEPPELIN_CONF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>zeppelin.server.addr</name>
    <value>0.0.0.0</value>
  </property>
  <property>
    <name>zeppelin.server.port</name>
    <value>8080</value>
  </property>
</configuration>
ZEPPELIN_CONF
fi

echo "✅ Bootstrap terminé avec succès"
EOF

echo "🪣 Upload du bootstrap vers S3..."
aws s3 cp bootstrap-emr-2025.sh s3://$BUCKET_NAME/bootstrap/bootstrap-emr-2025.sh --region $REGION
echo "✅ Bootstrap uploadé"

echo "🚀 Création cluster EMR avec instances compatibles..."

# Configuration EMR avec instances 2025
CLUSTER_ID=$(aws emr create-cluster \
    --applications Name=Hadoop Name=Spark Name=Zeppelin \
    --name "$CLUSTER_NAME" \
    --release-label emr-6.15.0 \
    --instance-groups \
        InstanceGroupType=MASTER,InstanceCount=1,InstanceType=$MASTER_INSTANCE,BidPrice=0.08 \
        InstanceGroupType=CORE,InstanceCount=$WORKER_COUNT,InstanceType=$WORKER_INSTANCE,BidPrice=0.08 \
    --bootstrap-actions Path=s3://$BUCKET_NAME/bootstrap/bootstrap-emr-2025.sh \
    --ec2-attributes KeyName=$KEY_NAME \
    --service-role EMR_DefaultRole \
    --job-flow-role EMR_EC2_DefaultRole \
    --region $REGION \
    --auto-scaling-role EMR_AutoScaling_DefaultRole \
    --log-uri s3://$BUCKET_NAME/logs/ \
    --query 'ClusterId' \
    --output text)

if [ $? -eq 0 ] && [ ! -z "$CLUSTER_ID" ]; then
    echo "✅ Cluster créé avec succès !"
    echo "$CLUSTER_ID" > "$AWS_CONFIG_DIR/cluster-id.txt"
    echo "🆔 Cluster ID: $CLUSTER_ID"
else
    echo "❌ Erreur lors de la création du cluster"
    exit 1
fi

echo ""
echo "🎉 DÉPLOIEMENT RÉUSSI !"
echo "======================"
echo "🏷️  Nom: $CLUSTER_NAME"
echo "🆔 ID: $CLUSTER_ID"
echo "💰 Coût: ~0.16€/heure (2 instances m5.xlarge SPOT)"
echo "⏱️  Initialisation: 15-20 minutes"
echo ""
echo "📋 COMMANDES UTILES:"
echo "# Vérifier l'état:"
echo "aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Status.State' --region $REGION"
echo ""
echo "# Récupérer l'IP du master:"
echo "aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.MasterPublicDnsName' --region $REGION --output text"
echo ""
echo "# Arrêter le cluster:"
echo "aws emr terminate-clusters --cluster-ids $CLUSTER_ID --region $REGION"

# Cleanup
rm -f bootstrap-emr-2025.sh

echo ""
echo "⚠️  IMPORTANT: N'oublie pas d'arrêter le cluster après la demo !"
echo "aws emr terminate-clusters --cluster-ids $CLUSTER_ID --region $REGION"