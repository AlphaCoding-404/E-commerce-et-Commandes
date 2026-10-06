#!/usr/bin/env bash
# Importe data view + visualisations + dashboard dans Kibana.
set -euo pipefail
KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"
curl -sS -X POST "${KIBANA_URL}/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  --form file=@"$(dirname "$0")/dashboard.ndjson"
echo
