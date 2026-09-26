#!/usr/bin/env bash

# Telemetrix - Grafana one-shot launcher
#
# Spins up a single Grafana container with:
#   - SimpleJSON datasource plugin
#   - Telemetrix datasource pointing at the local provider
#   - Telemetrix dashboard auto-provisioned from ./grafana/dashboards
#
# Stop with Ctrl-C or:
#   docker stop telemetrix-grafana
#
# Examples:
#   ./run_grafana.sh
#   GF_PORT=4000 PROVIDER_PORT=7777 ./run_grafana.sh

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

GF_PORT="${GF_PORT:-3000}"
PROVIDER_HOST="${PROVIDER_HOST:-host.docker.internal}"
PROVIDER_PORT="${PROVIDER_PORT:-7777}"

GF_IMAGE="${GF_IMAGE:-grafana/grafana-oss:11.3.0}"
GF_CONTAINER="${GF_CONTAINER:-telemetrix-grafana}"

GRAFANA_DATA_DIR="$HERE/grafana_data"
GRAFANA_LOG_DIR="$HERE/grafana_logs"
GRAFANA_PROVISIONING_DIR="$HERE/grafana/provisioning"
GRAFANA_DASHBOARD_DIR="$HERE/grafana/dashboards"

# ---------------------------------------------------------------------------
# Prepare directories
# ---------------------------------------------------------------------------

mkdir -p \
  "$GRAFANA_DATA_DIR" \
  "$GRAFANA_LOG_DIR" \
  "$GRAFANA_PROVISIONING_DIR/datasources" \
  "$GRAFANA_PROVISIONING_DIR/dashboards" \
  "$GRAFANA_DASHBOARD_DIR"

# Grafana runs as UID 472 in the official image.
# This keeps the local development setup simple and avoids permission issues.
chmod -R 777 \
  "$GRAFANA_DATA_DIR" \
  "$GRAFANA_LOG_DIR" \
  2>/dev/null || true

# ---------------------------------------------------------------------------
# Datasource provisioning
# ---------------------------------------------------------------------------

cat > "$GRAFANA_PROVISIONING_DIR/datasources/telemetrix.yml" <<YAML
apiVersion: 1

datasources:
  - name: Telemetrix
    uid: telemetrix
    type: simpod-json-datasource
    access: proxy
    url: http://${PROVIDER_HOST}:${PROVIDER_PORT}
    isDefault: true
    editable: true
YAML

# ---------------------------------------------------------------------------
# Dashboard provisioning
# ---------------------------------------------------------------------------

cat > "$GRAFANA_PROVISIONING_DIR/dashboards/telemetrix.yml" <<YAML
apiVersion: 1

providers:
  - name: Telemetrix dashboards
    orgId: 1
    folder: ""
    type: file
    disableDeletion: false
    editable: true
    updateIntervalSeconds: 5

    options:
      path: /etc/grafana/dashboards
YAML

# ---------------------------------------------------------------------------
# Docker networking
# ---------------------------------------------------------------------------

EXTRA_FLAGS=()

# On Linux, host.docker.internal is not normally defined automatically.
if [[ "$(uname -s)" == "Linux" ]]; then
  EXTRA_FLAGS+=(
    --add-host=host.docker.internal:host-gateway
  )
fi

# ---------------------------------------------------------------------------
# Startup information
# ---------------------------------------------------------------------------

echo
echo "▶ Grafana      : http://localhost:${GF_PORT}"
echo "▶ Login        : admin / admin"
echo "▶ Data source  : http://${PROVIDER_HOST}:${PROVIDER_PORT}"
echo "▶ Dashboard    : Telemetrix - Connected Vehicle Fleet"
echo "▶ Container    : ${GF_CONTAINER}"
echo "▶ Stop         : Ctrl-C or docker stop ${GF_CONTAINER}"
echo

# ---------------------------------------------------------------------------
# Remove previous container
# ---------------------------------------------------------------------------

docker rm -f "${GF_CONTAINER}" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# Start Grafana
# ---------------------------------------------------------------------------

docker run --rm \
  --name "${GF_CONTAINER}" \
  -p "${GF_PORT}:3000" \
  "${EXTRA_FLAGS[@]}" \
  \
  -v "$GRAFANA_PROVISIONING_DIR:/etc/grafana/provisioning:ro" \
  -v "$GRAFANA_DASHBOARD_DIR:/etc/grafana/dashboards:ro" \
  -v "$GRAFANA_DATA_DIR:/var/lib/grafana" \
  -v "$GRAFANA_LOG_DIR:/var/log/grafana" \
  \
  -e GF_SECURITY_ADMIN_USER=admin \
  -e GF_SECURITY_ADMIN_PASSWORD=admin \
  -e GF_USERS_ALLOW_SIGN_UP=false \
  \
  -e GF_INSTALL_PLUGINS=simpod-json-datasource \
  -e GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=simpod-json-datasource \
  \
  -e GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/etc/grafana/dashboards/telemetrix.json \
  \
  -e GF_AUTH_ANONYMOUS_ENABLED=true \
  -e GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer \
  \
  "${GF_IMAGE}"
