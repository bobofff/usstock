#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${USSTOCK_PROJECT_DIR:-$(cd -- "${SCRIPT_DIR}/.." && pwd)}"
SERVICE_NAME="${USSTOCK_SERVICE_NAME:-usstock-admin}"
SERVICE_USER="${USSTOCK_SERVICE_USER:-${USER}}"
HOST="${USSTOCK_ADMIN_HOST:-127.0.0.1}"
PORT="${USSTOCK_ADMIN_PORT:-7878}"
UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
SUDO=""

if [[ "${EUID}" -ne 0 ]]; then
  SUDO="sudo"
fi

if [[ ! -x "${PROJECT_DIR}/.venv/bin/usstock" ]]; then
  echo "Missing executable: ${PROJECT_DIR}/.venv/bin/usstock" >&2
  echo "Run python3.13 -m venv .venv && source .venv/bin/activate && pip install -e . first." >&2
  exit 1
fi

if [[ ! -f "${PROJECT_DIR}/.env" ]]; then
  echo "Missing env file: ${PROJECT_DIR}/.env" >&2
  echo "Copy .env.example to .env and configure DATABASE_URL first." >&2
  exit 1
fi

tmp_unit="$(mktemp)"
trap 'rm -f "${tmp_unit}"' EXIT

cat > "${tmp_unit}" <<UNIT
[Unit]
Description=USStock Admin Panel
After=network.target postgresql.service

[Service]
Type=simple
WorkingDirectory=${PROJECT_DIR}
EnvironmentFile=${PROJECT_DIR}/.env
ExecStart=${PROJECT_DIR}/.venv/bin/usstock admin --host ${HOST} --port ${PORT}
Restart=always
RestartSec=5
User=${SERVICE_USER}

[Install]
WantedBy=multi-user.target
UNIT

${SUDO} install -m 0644 "${tmp_unit}" "${UNIT_PATH}"
${SUDO} systemctl daemon-reload
${SUDO} systemctl enable "${SERVICE_NAME}"
${SUDO} systemctl restart "${SERVICE_NAME}"

echo "Installed and started ${SERVICE_NAME}."
echo "Status: sudo systemctl status ${SERVICE_NAME}"
echo "Logs:   sudo journalctl -u ${SERVICE_NAME} -f"
