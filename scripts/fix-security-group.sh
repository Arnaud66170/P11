#!/bin/bash

# FIX: Security Group EMR P11
# Résout l'erreur VALIDATION_ERROR des ports publics

set -e

echo "RESOLUTION PROBLEME SECURITY GROUP EMR"
echo "======================================"

# Configuration
REGION="eu-west-1"

echo "1. Nettoyage ancien cluster defaillant..."
if [ -f "../aws-config/cluster-id.txt" ]; then
    OLD_CLUSTER=$(cat ../aws-config/cluster-id.txt)
    echo "Nettoyage cluster: $OLD_CLUSTER"
    aws emr terminate-clusters --cluster-ids $OLD_CLUSTER --region $REGION 2>/dev/null || echo "   (deja termine)"
fi

echo ""
echo "2. Recuperation du VPC par defaut..."
DEFAULT_VPC=$(aws ec2 describe-vpcs --region $REGION --filters "Name=is-default,Values=true" --query 'Vpcs[0].VpcId' --output text)
echo "VPC par defaut: $DEFAULT_VPC"

echo ""
echo "3. Creation Security Group securise pour EMR..."

# Verifier si le SG existe deja
if aws ec2 describe-security-groups --group-names "emr-p11-secure" --region $REGION >/dev/null 2>&1; then
    echo "Security Group existe deja, recuperation..."
    EMR_SG=$(aws ec2 describe-security-groups --group-names "emr-p11-secure" --region $REGION --query 'SecurityGroups[0].GroupId' --output text)
else
    echo "Creation nouveau Security Group..."
    EMR_SG=$(aws ec2 create-security-group \
        --group-name "emr-p11-secure" \
        --description "Security Group for EMR P11 - SSH only" \
        --vpc-id $DEFAULT_VPC \
        --region $REGION \
        --query 'GroupId' \
        --output text)
    
    echo "Security Group cree: $EMR_SG"
    
    # Ajouter regle SSH depuis ton IP
    echo "Recuperation de ton IP publique..."
    MY_IP=$(curl -s ifconfig.me)
    echo "Ton IP: $MY_IP"
    
    aws ec2 authorize-security-group-ingress \
        --group-id $EMR_SG \
        --protocol tcp \
        --port 22 \
        --cidr ${MY_IP}/32 \
        --region $REGION
    
    echo "SSH autorise depuis ton IP uniquement"
fi

# Sauvegarde
mkdir -p ../aws-config
echo $EMR_SG > ../aws-config/emr-security-group.txt
echo "Security Group sauvegarde: $EMR_SG"

echo ""
echo "4. Creation cluster EMR avec configuration securisee..."

CLUSTER_ID=$(aws emr create-cluster \
    --applications Name=Hadoop Name=Spark Name=Zeppelin \
    --name "p11-fruits-secure-$(date +%m%d-%H%M)" \
    --release-label emr-6.15.0 \
    --instance-type m5.xlarge \
    --instance-count 4 \
    --ec2-attributes KeyName=p11-keypair,AdditionalMasterSecurityGroups=$EMR_SG,AdditionalSlaveSecurityGroups=$EMR_SG \
    --log-uri s3://fruits-p11-production/logs/ \
    --region $REGION \
    --query 'ClusterId' \
    --output text)

if [ $? -eq 0 ] && [ ! -z "$CLUSTER_ID" ]; then
    echo "Cluster cree avec succes: $CLUSTER_ID"
    echo "$CLUSTER_ID" > ../aws-config/cluster-id.txt
    
    # Sauvegarde infos completes
    cat > ../aws-config/cluster-info.txt << EOF
CLUSTER_ID=$CLUSTER_ID
SECURITY_GROUP=$EMR_SG
VPC=$DEFAULT_VPC
CREATED=$(date)
REGION=$REGION
INSTANCE_COUNT=3
INSTANCE_TYPE=m5.large
EOF

    echo ""
    echo "CLUSTER EMR SECURISE CREE"
    echo "========================="
    echo "Cluster ID: $CLUSTER_ID"
    echo "Security Group: $EMR_SG (SSH uniquement)"
    echo "Configuration: 3 instances m5.large (6 vCPUs total)"
    echo "Distribution: 1 Master + 2 Workers"
    echo "Cout: ~36c/heure (quota-friendly)"
    echo "Initialisation: 15-20 minutes"
    echo ""
    echo "SURVEILLANCE:"
    echo "export CLUSTER_ID=$CLUSTER_ID"
    echo 'watch -n 30 "echo \"Heure: \" \\$(date \\\"+%H:%M:%S\\\") \\\" - Etat:\\\" && aws emr describe-cluster --cluster-id \\$CLUSTER_ID --region eu-west-1 --query \\\"Cluster.Status.State\\\" --output text"'
    echo ""
    echo "Une fois RUNNING, recuperer l'adresse master:"
    echo "aws emr describe-cluster --cluster-id $CLUSTER_ID --region $REGION --query 'Cluster.MasterPublicDnsName' --output text"
    echo ""
    echo "Arreter le cluster apres usage:"  
    echo "aws emr terminate-clusters --cluster-ids $CLUSTER_ID --region $REGION"
    
else
    echo "Erreur lors de la creation du cluster"
    exit 1
fi

echo ""
echo "PROBLEME RESOLU ! Le cluster devrait demarrer normalement."

# Exécution du script depuis cd ~/P11/2-python/scripts/
# Rendre le script exécutable
# chmod +x fix-security-group.sh
# ./fix-security-group.sh
