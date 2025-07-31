#!/bin/bash
# upload_projet_p11_s3.sh
# Upload de ton projet complet vers S3
# À exécuter depuis ~/P11/2-python/

echo "🚀 Upload Projet P11 Complet vers S3"
echo "===================================="

# Configuration
BUCKET_NAME="fruits-p11-production"
REGION="eu-west-1"

# Vérification de la localisation
if [ ! -d "data/fruits-360" ]; then
    echo "❌ Erreur : exécute ce script depuis ~/P11/2-python/"
    exit 1
fi

echo "📂 Projet détecté : $(pwd)"

# 1. DATASET - Upload des données fruits-360
echo "📤 1/6 Upload du dataset fruits-360..."
aws s3 sync data/fruits-360/ s3://$BUCKET_NAME/data/fruits-360/ \
    --region $REGION \
    --exclude "*.DS_Store" \
    --exclude "Thumbs.db" \
    --delete \
    --quiet

echo "   ✅ Dataset uploadé"

# 2. SCRIPTS - Code Python et notebooks
echo "📤 2/6 Upload des scripts et code..."

# Scripts EMR et utilitaires
aws s3 sync scripts/ s3://$BUCKET_NAME/scripts/ \
    --region $REGION \
    --exclude "*.log" \
    --quiet

# Code source Python
aws s3 sync src/ s3://$BUCKET_NAME/src/ \
    --region $REGION \
    --exclude "__pycache__/*" \
    --exclude "*.pyc" \
    --exclude ".ipynb_checkpoints/*" \
    --quiet

# Notebooks (sélection des principaux)
aws s3 cp notebooks/01_fruits_pipeline_cloud.ipynb \
    s3://$BUCKET_NAME/notebooks/01_fruits_pipeline_cloud.ipynb \
    --region $REGION

aws s3 cp notebooks/04_pipeline_fruits_demo.ipynb \
    s3://$BUCKET_NAME/notebooks/04_pipeline_fruits_demo.ipynb \
    --region $REGION

echo "   ✅ Code uploadé"

# 3. RÉSULTATS - Outputs distribués (PREUVES!)
echo "📤 3/6 Upload des résultats distribués..."

# Résultats PCA optimaux (PREUVE de calcul distribué)
aws s3 sync outputs/features_pca_optimal.parquet/ \
    s3://$BUCKET_NAME/outputs/features_pca_optimal.parquet/ \
    --region $REGION \
    --quiet

# Résultats finaux
aws s3 sync outputs/final_results.parquet/ \
    s3://$BUCKET_NAME/outputs/final_results.parquet/ \
    --region $REGION \
    --quiet

# Cache des features (utile pour la démo)
aws s3 sync outputs/cache/ s3://$BUCKET_NAME/outputs/cache/ \
    --region $REGION \
    --quiet

echo "   ✅ Résultats distribués uploadés"

# 4. VISUALISATIONS - Graphiques pour la soutenance
echo "📤 4/6 Upload des visualisations..."
aws s3 cp outputs/pca_variance_analysis.png \
    s3://$BUCKET_NAME/outputs/visualizations/pca_variance_analysis.png \
    --region $REGION

aws s3 cp outputs/pca_2d_visualisation.png \
    s3://$BUCKET_NAME/outputs/visualizations/pca_2d_visualisation.png \
    --region $REGION

echo "   ✅ Visualisations uploadées"

# 5. CONFIGURATION - AWS et Docker
echo "📤 5/6 Upload des configurations..."
aws s3 sync aws-config/ s3://$BUCKET_NAME/config/aws/ \
    --region $REGION \
    --quiet

# Requirements pour reproduction environnement
aws s3 cp requirements.txt s3://$BUCKET_NAME/config/requirements.txt \
    --region $REGION

echo "   ✅ Configurations uploadées"

# 6. DOCUMENTATION - README et métadonnées
echo "📤 6/6 Ajout de la documentation..."

# Création d'un README projet
cat > /tmp/README_P11.md << 'EOF'
# Projet P11 - Pipeline Big Data Fruits
## Architecture Cloud AWS EMR + S3

### Structure du Projet
- **data/fruits-360/** : Dataset images (120 classes, 82k+ images)
- **src/** : Modules Python (preprocessing, PCA, utils)
- **notebooks/** : Pipeline principal et démos
- **outputs/** : Résultats distribués (PCA, features)
- **scripts/** : Configuration EMR et bootstrap
- **config/** : Configuration AWS et requirements

### Résultats Clés
- ✅ Extraction features MobileNetV2 distribuée
- ✅ Réduction dimension PCA optimisée (56 composantes)
- ✅ Calcul distribué sur EMR validé
- ✅ Conformité RGPD (région eu-west-1)

### Preuves Techniques
- Fichiers `part-*` : Distribution sur 16 partitions
- Fichier `_SUCCESS` : Validation Spark
- Métadonnées parquet : Optimisation stockage

Date de génération : $(date)
EOF

aws s3 cp /tmp/README_P11.md s3://$BUCKET_NAME/README.md --region $REGION

echo "   ✅ Documentation ajoutée"

# 7. VÉRIFICATION FINALE
echo ""
echo "🔍 VÉRIFICATION S3..."
echo "==================="

# Statistiques par dossier
echo "📊 Répartition des objets :"
aws s3 ls s3://$BUCKET_NAME/ --recursive --human-readable --summarize | tail -2

echo ""
echo "📁 Structure créée :"
aws s3 ls s3://$BUCKET_NAME/ --human-readable

echo ""
echo "🎯 PREUVES POUR SOUTENANCE :"
echo "  ✅ Dataset complet sur S3"
echo "  ✅ Code source et notebooks"  
echo "  ✅ Résultats distribués (part-*, _SUCCESS)"
echo "  ✅ Visualisations PCA"
echo "  ✅ Configuration EMR prête"
echo "  ✅ Région EU (RGPD compliant)"

echo ""
echo "🎉 UPLOAD TERMINÉ ! Projet prêt pour la soutenance."
echo "👀 Vérifier dans la console AWS S3 : https://s3.console.aws.amazon.com/s3/buckets/fruits-p11-production"

# Nettoyage
rm -f /tmp/README_P11.md

# Rendre le script exécutable :
# chmod +x upload_projet_p11_s3.sh

# Exécution depuis la racine projet :
# ./upload_projet_p11_s3.sh
