#!/bin/bash

# SCRIPT: deploy-emr-legacy.sh
# Compatible avec AWS CLI v1.41.x (syntaxe 2019)
# Auteur: Nono66 - Projet P11

set -e

echo "🚀 Déploiement EMR P11 - AWS CLI v1.41.x Legacy"
echo "==============================================="

# Configuration
CLUSTER_NAME="p11-fruits-demo-$(date +%Y%m%d-%H%M)"
BUCKET_NAME="fruits-p11-production"
REGION="eu-west-1"
KEY_NAME="p11-keypair"

# Instances pour distribution réelle
MASTER_INSTANCE="m5.xlarge"
WORKER_INSTANCE="m5.xlarge"
WORKER_COUNT=3

# Répertoires
AWS_CONFIG_DIR="../aws-config"
mkdir -p "$AWS_CONFIG_DIR"

echo "📋 Configuration:"
echo "   - AWS CLI: $(aws --version | cut -d' ' -f1)"
echo "   - Cluster: $CLUSTER_NAME"
echo "   - Workers: $WORKER_COUNT x $WORKER_INSTANCE"

# Vérification des prérequis
echo "🔍 Vérification des prérequis..."

if ! aws iam get-role --role-name EMR_DefaultRole >/dev/null 2>&1; then
    echo "❌ Rôle EMR_DefaultRole manquant !"
    echo "💡 Créer avec: aws emr create-default-roles"
    exit 1
fi

if ! aws ec2 describe-key-pairs --key-names "$KEY_NAME" --region "$REGION" >/dev/null 2>&1; then
    echo "❌ Keypair '$KEY_NAME' manquant !"
    echo "💡 Créer avec: aws ec2 create-key-pair --key-name $KEY_NAME --region $REGION"
    exit 1
fi

echo "✅ Prérequis OK"

# Upload du bootstrap si nécessaire
echo "📦 Préparation du bootstrap..."

cat > bootstrap-emr-legacy.sh << 'EOF'
#!/bin/bash
set -e

echo "🔧 Bootstrap EMR P11 - Legacy Compatible"
echo "======================================="

# Mise à jour système
sudo yum update -y

# Installation Python et pip
sudo yum install -y python3-pip python3-devel gcc

# Upgrade pip
sudo pip3 install --upgrade pip setuptools wheel

# Installation des packages ML (versions stables)
sudo pip3 install --no-cache-dir \
    tensorflow==2.12.0 \
    pillow==9.5.0 \
    numpy==1.24.3 \
    pandas==2.0.3 \
    scikit-learn==1.3.0 \
    matplotlib==3.7.2

# Configuration Spark
sudo tee -a /etc/spark/conf/spark-defaults.conf << 'SPARK_CONF'
spark.serializer org.apache.spark.serializer.KryoSerializer
spark.sql.adaptive.enabled true
spark.sql.adaptive.coalescePartitions.enabled true
spark.executor.memory 3g
spark.driver.memory 2g
SPARK_CONF

echo "✅ Bootstrap terminé avec succès"
EOF

# Upload du bootstrap
aws s3 cp bootstrap-emr-legacy.sh s3://$BUCKET_NAME/bootstrap/bootstrap-emr-legacy.sh --region $REGION
echo "✅ Bootstrap uploadé sur S3"

echo "🚀 Création cluster EMR avec syntaxe legacy..."

# SYNTAXE AWS CLI v1.41.x - Très basique, sans options avancées
CLUSTER_ID=$(aws emr create-cluster \
    --name "$CLUSTER_NAME" \
    --release-label emr-6.15.0 \
    --applications Name=Hadoop Name=Spark Name=Zeppelin \
    --instance-type $MASTER_INSTANCE \
    --instance-count $((WORKER_COUNT + 1)) \
    --ec2-attributes KeyName=$KEY_NAME \
    --service-role EMR_DefaultRole \
    --bootstrap-actions Path=s3://$BUCKET_NAME/bootstrap/bootstrap-emr-legacy.sh \
    --log-uri s3://$BUCKET_NAME/logs/ \
    --region $REGION \
    --output text \
    --query 'ClusterId')

if [ $? -eq 0 ] && [ ! -z "$CLUSTER_ID" ]; then
    echo "✅ Cluster créé avec succès !"
    echo "$CLUSTER_ID" > "$AWS_CONFIG_DIR/cluster-id.txt"
    echo ""
    echo "🎉 DÉPLOIEMENT RÉUSSI !"
    echo "======================"
    echo "🆔 Cluster ID: $CLUSTER_ID"
    echo "🏷️  Nom: $CLUSTER_NAME"
    echo "⚙️  Config: $((WORKER_COUNT + 1)) instances $MASTER_INSTANCE"
    echo "💰 Coût: ~$((((WORKER_COUNT + 1)) * 12))c/heure"
    echo "⏱️  Initialisation: 15-20 minutes"
    
    # Sauvegarde des infos
    cat > "$AWS_CONFIG_DIR/cluster-info.txt" << EOF
CLUSTER_ID=$CLUSTER_ID
CLUSTER_NAME=$CLUSTER_NAME
INSTANCE_COUNT=$((WORKER_COUNT + 1))
INSTANCE_TYPE=$MASTER_INSTANCE
CREATED=$(date)
REGION=$REGION
EOF
    
    echo ""
    echo "📋 COMMANDES DE SUIVI:"
    echo ""
    echo "# Vérifier l'état du cluster:"
    echo "aws emr describe-cluster --cluster-id $CLUSTER_ID --region $REGION"
    echo ""
    echo "# Attendre que le cluster soit prêt:"
    echo "aws emr wait cluster-running --cluster-id $CLUSTER_ID --region $REGION"
    echo ""
    echo "# Récupérer l'adresse du master:"
    echo "aws emr describe-cluster --cluster-id $CLUSTER_ID --region $REGION --query 'Cluster.MasterPublicDnsName'"
    echo ""
    echo "# Arrêter le cluster (IMPORTANT!):"
    echo "aws emr terminate-clusters --cluster-ids $CLUSTER_ID --region $REGION"
    echo ""
    
    # Surveillance automatique
    echo "⏳ Surveillance du démarrage..."
    echo "   État actuel: $(aws emr describe-cluster --cluster-id $CLUSTER_ID --region $REGION --query 'Cluster.Status.State' --output text)"
    echo "   Attendre 'RUNNING' pour utiliser le cluster"
    echo ""
    echo "🔍 Pour suivre en temps réel:"
    echo "watch 'aws emr describe-cluster --cluster-id $CLUSTER_ID --region $REGION --query \"Cluster.Status.State\" --output text'"
    
else
    echo "❌ Erreur lors de la création du cluster"
    echo "Vérifiez les logs et permissions"
    exit 1
fi

# Nettoyage
rm -f bootstrap-emr-legacy.sh

echo ""
echo "⚠️  RAPPEL IMPORTANT:"
echo "   - Le cluster coûte ~$((((WORKER_COUNT + 1)) * 12))c/heure"
echo "   - Ne pas oublier de l'arrêter après la démo !"
echo "   - Commande d'arrêt: aws emr terminate-clusters --cluster-ids $CLUSTER_ID --region $REGION"

# Lancement du script, depuis scripts/ :

# ./deploy-emr-p11.sh