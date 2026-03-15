#!/bin/sh
set -e

echo "==> Initializing Superset database..."
superset db upgrade

echo "==> Creating admin user..."
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@demo.local \
  --password admin123 || echo "(admin may already exist)"

echo "==> Initializing Superset..."
superset init

echo "==> Creating Arrow Flight SQL datasource..."
superset import-datasources -p /dashboards/datasources.yaml || echo "(datasource import failed, will create via API)"

SUPERSET_URL="http://superset:8088"

echo "==> Waiting for Superset API..."
until curl -sf "${SUPERSET_URL}/health" > /dev/null 2>&1; do
  sleep 3
done

# Get CSRF token and session cookie
echo "==> Authenticating with Superset API..."
CSRF_TOKEN=$(curl -sf -c /tmp/superset-cookies.txt \
  "${SUPERSET_URL}/api/v1/security/csrf_token/" | sed 's/.*"result":"\([^"]*\)".*/\1/')

LOGIN_RESP=$(curl -sf -b /tmp/superset-cookies.txt -c /tmp/superset-cookies.txt \
  -X POST "${SUPERSET_URL}/api/v1/security/login" \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin123","provider":"db","refresh":true}')

ACCESS_TOKEN=$(echo "${LOGIN_RESP}" | sed 's/.*"access_token":"\([^"]*\)".*/\1/')

if [ -z "${ACCESS_TOKEN}" ]; then
  echo "==> WARNING: Could not get access token. Manual datasource setup required."
else
  FLIGHT_HOST="${FLIGHT_SQL_HOST:-flight-sql-superset}"
  FLIGHT_PORT="${FLIGHT_SQL_PORT:-32010}"

  echo "==> Creating Flight SQL database connection..."
  curl -sf -X POST "${SUPERSET_URL}/api/v1/database/" \
    -H "Authorization: Bearer ${ACCESS_TOKEN}" \
    -H 'Content-Type: application/json' \
    -d "{
      \"database_name\": \"Elasticsearch (Flight SQL)\",
      \"engine\": \"elastiq\",
      \"sqlalchemy_uri\": \"elastiq://${FLIGHT_HOST}:${FLIGHT_PORT}?insecure=true\",
      \"extra\": \"{\\\"allows_virtual_table_explore\\\": true}\"
    }" || echo "(database may already exist)"

  echo ""
  echo "==> Creating sample dataset for 'ecommerce' table..."
  # Get database ID
  DB_ID=$(curl -sf "${SUPERSET_URL}/api/v1/database/" \
    -H "Authorization: Bearer ${ACCESS_TOKEN}" | \
    sed 's/.*"id":\([0-9]*\).*/\1/' | head -1)

  if [ -n "${DB_ID}" ]; then
    # Create dataset
    curl -sf -X POST "${SUPERSET_URL}/api/v1/dataset/" \
      -H "Authorization: Bearer ${ACCESS_TOKEN}" \
      -H 'Content-Type: application/json' \
      -d "{
        \"database\": ${DB_ID},
        \"table_name\": \"ecommerce\",
        \"schema\": \"\"
      }" || echo "(dataset may already exist)"

    echo ""
    echo "==> Creating demo charts and dashboard..."
    /app/.venv/bin/python /dashboards/create_dashboards.py || echo "(dashboard creation skipped)"
  fi
fi

echo ""
echo "============================================"
echo "  Superset is ready!"
echo "  Web UI: http://localhost:8088"
echo "  Login:  admin / admin123"
echo "  Source: Elasticsearch (Flight SQL)"
echo "  Dashboard: E-Commerce Analytics"
echo "============================================"
