#!/bin/bash

# Script de collecte des preuves EMR pour soutenance
# À exécuter APRÈS ton pipeline Zeppelin

set -e

echo "COLLECTE DES PREUVES EMR POUR SOUTENANCE"
echo "========================================"

# Configuration
export CLUSTER_ID=$(cat ../aws-config/cluster-id.txt)
REGION="eu-west-1"
PROOF_DIR="../outputs/preuves-soutenance"
mkdir -p "$PROOF_DIR"

echo "Cluster analysé: $CLUSTER_ID"

# 1. Informations du cluster
echo "1. Récupération infos cluster..."
aws emr describe-cluster --cluster-id $CLUSTER_ID --region $REGION > "$PROOF_DIR/cluster-details.json"

MASTER_DNS=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --region $REGION --query 'Cluster.MasterPublicDnsName' --output text)
echo "Master DNS: $MASTER_DNS" > "$PROOF_DIR/cluster-urls.txt"

# 2. URLs d'accès
echo "2. Génération URLs d'accès..."
cat >> "$PROOF_DIR/cluster-urls.txt" << EOF
SPARK UI: http://$MASTER_DNS:4040
ZEPPELIN: http://$MASTER_DNS:8890
YARN UI: http://$MASTER_DNS:8088
HDFS UI: http://$MASTER_DNS:9870
EMR Console: https://eu-west-1.console.aws.amazon.com/emr/home?region=eu-west-1#cluster-details:$CLUSTER_ID
EOF

# 3. Instances du cluster
echo "3. Liste des instances..."
aws emr list-instances --cluster-id $CLUSTER_ID --region $REGION > "$PROOF_DIR/cluster-instances.json"

# Résumé lisible
aws emr list-instances --cluster-id $CLUSTER_ID --region $REGION --query 'Instances[].{Type:InstanceGroupType,State:State,PrivateIP:PrivateDnsName,PublicIP:PublicDnsName}' --output table > "$PROOF_DIR/instances-summary.txt"

# 4. Métriques CloudWatch
echo "4. Récupération métriques CloudWatch..."
START_TIME=$(date -u -d '2 hours ago' +%Y-%m-%dT%H:%M:%S)
END_TIME=$(date -u +%Y-%m-%dT%H:%M:%S)

# Nombre de workers running
aws cloudwatch get-metric-statistics \
    --namespace AWS/ElasticMapReduce \
    --metric-name CoreNodesRunning \
    --dimensions Name=JobFlowId,Value=$CLUSTER_ID \
    --start-time $START_TIME \
    --end-time $END_TIME \
    --period 300 \
    --statistics Average,Maximum \
    --region $REGION > "$PROOF_DIR/metrics-workers.json" 2>/dev/null || echo "Métriques workers non disponibles"

# 5. Logs S3
echo "5. Vérification logs S3..."
aws s3 ls s3://fruits-p11-production/logs/$CLUSTER_ID/ --recursive --region $REGION > "$PROOF_DIR/s3-logs-list.txt" 2>/dev/null || echo "Pas de logs S3"

# 6. Résultats de traitement
echo "6. Vérification résultats S3..."
aws s3 ls s3://fruits-p11-production/outputs/ --recursive --human-readable --region $REGION > "$PROOF_DIR/s3-results-list.txt" 2>/dev/null || echo "Pas de résultats S3"

# 7. Steps exécutés
echo "7. Historique des steps..."
aws emr list-steps --cluster-id $CLUSTER_ID --region $REGION > "$PROOF_DIR/cluster-steps.json" 2>/dev/null || echo "Pas de steps custom"

# 8. Configuration réseau
echo "8. Configuration réseau..."
aws emr describe-cluster --cluster-id $CLUSTER_ID --region $REGION --query 'Cluster.Ec2InstanceAttributes' > "$PROOF_DIR/network-config.json"

# 9. Génération rapport HTML
echo "9. Génération rapport de preuves..."
cat > "$PROOF_DIR/rapport-preuves.html" << EOF
<!DOCTYPE html>
<html>
<head>
    <title>Preuves EMR P11 - $(date)</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .section { margin: 20px 0; padding: 15px; border: 1px solid #ddd; }
        .highlight { background-color: #f0f0f0; padding: 10px; }
        pre { background-color: #f8f8f8; padding: 10px; overflow-x: auto; }
        .success { color: green; font-weight: bold; }
        .info { color: blue; }
    </style>
</head>
<body>
    <h1>🎯 Preuves de Calcul Distribué EMR - Projet P11</h1>
    
    <div class="section">
        <h2>📋 Informations Cluster</h2>
        <div class="highlight">
            <strong>Cluster ID:</strong> $CLUSTER_ID<br>
            <strong>Master DNS:</strong> $MASTER_DNS<br>
            <strong>Date de génération:</strong> $(date)<br>
            <strong>Région:</strong> $REGION (RGPD compliant)
        </div>
    </div>
    
    <div class="section">
        <h2>🔗 URLs de Vérification</h2>
        <p class="info">À utiliser pendant la soutenance :</p>
        <ul>
            <li><strong>Spark UI:</strong> <a href="http://$MASTER_DNS:4040">http://$MASTER_DNS:4040</a></li>
            <li><strong>Zeppelin:</strong> <a href="http://$MASTER_DNS:8890">http://$MASTER_DNS:8890</a></li>
            <li><strong>YARN UI:</strong> <a href="http://$MASTER_DNS:8088">http://$MASTER_DNS:8088</a></li>
            <li><strong>EMR Console:</strong> <a href="https://eu-west-1.console.aws.amazon.com/emr/home?region=eu-west-1#cluster-details:$CLUSTER_ID">AWS Console</a></li>
        </ul>
    </div>
    
    <div class="section">
        <h2>📊 Preuves de Distribution</h2>
        <p class="success">✅ Cluster multi-nœuds configuré</p>
        <p class="info">Vérifications à montrer :</p>
        <ul>
            <li>Spark UI → Onglet "Executors" (multiple machines)</li>
            <li>YARN UI → Resource Manager (workers actifs)</li>
            <li>EMR Console → Hardware (instances multiples)</li>
            <li>CloudWatch → Métriques de charge distribuée</li>
        </ul>
    </div>
    
    <div class="section">
        <h2>📁 Fichiers Générés</h2>
        <ul>
            <li><code>cluster-details.json</code> - Configuration complète</li>
            <li><code>cluster-instances.json</code> - Liste des nœuds</li>
            <li><code>instances-summary.txt</code> - Résumé lisible</li>
            <li><code>metrics-workers.json</code> - Métriques CloudWatch</li>
            <li><code>s3-logs-list.txt</code> - Logs d'exécution</li>
            <li><code>s3-results-list.txt</code> - Résultats de traitement</li>
        </ul>
    </div>
    
    <div class="section">
        <h2>🎯 Points Clés Soutenance</h2>
        <div class="highlight">
            <p><strong>Argument 1:</strong> Calcul distribué sur plusieurs nœuds AWS</p>
            <p><strong>Argument 2:</strong> Données et résultats stockés sur S3</p>
            <p><strong>Argument 3:</strong> Conformité RGPD (région EU)</p>
            <p><strong>Argument 4:</strong> Scalabilité prouvée (ajout/suppression nœuds)</p>
        </div>
    </div>
</body>
</html>
EOF

echo ""
echo "COLLECTE TERMINEE"
echo "================="
echo "📁 Dossier preuves: $PROOF_DIR"
echo "🌐 Rapport HTML: $PROOF_DIR/rapport-preuves.html"
echo "📋 URLs d'accès: $PROOF_DIR/cluster-urls.txt"
echo ""
echo "🎯 Pour la soutenance, ouvrir:"
echo "   1. Le rapport HTML"
echo "   2. Spark UI: http://$MASTER_DNS:4040"
echo "   3. EMR Console AWS"
echo ""
echo "⚠️  Ne pas oublier d'arrêter le cluster après:"
echo "   aws emr terminate-clusters --cluster-ids $CLUSTER_ID --region $REGION"


# Rendre le script actif :
# chmod +x collect_emr_proofs.sh

# Exécution du script depuis cd ~/P11/2-python/scripts/
# ./collect_emr_proofs.sh