#!/usr/bin/env bash
# Ré-exporte le dashboard (après l'avoir modifié dans Kibana) vers kibana/dashboard.ndjson
set -euo pipefail
KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"
curl -sS -X POST "${KIBANA_URL}/api/saved_objects/_export" \
  -H "kbn-xsrf: true" -H "Content-Type: application/json" \
  -d '{"type": ["dashboard", "visualization", "index-pattern"], "includeReferencesDeep": true}' \
  -o "$(dirname "$0")/dashboard.ndjson"
echo "Export terminé -> kibana/dashboard.ndjson"
