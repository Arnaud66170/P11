#!/bin/bash
# Arrêt automatique EMR après délai
sleep 14400  # Attendre X heures
aws emr terminate-clusters --cluster-ids $(cat aws-config/cluster-id.txt) --region eu-west-1
echo "🛑 Cluster arrêté automatiquement après 4h"
