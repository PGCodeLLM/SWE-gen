#!/usr/bin/env bash
# Install node-local build-slot files and the docker PATH wrapper on one host.
# Usage: sudo SWEGEN_BUILD_SLOTS=32 ./install-build-slots.sh
set -euo pipefail

SLOTS="${SWEGEN_BUILD_SLOTS:-32}"
SLOT_DIR="${SWEGEN_BUILD_SLOT_DIR:-/data/swegen-k3s/build-slots}"
BIN_DIR="${SWEGEN_BUILD_BIN_DIR:-/data/swegen-k3s/bin}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WRAPPER_SRC="${SWEGEN_WRAPPER_SRC:-${SCRIPT_DIR}/docker-build-slot-wrapper.py}"

mkdir -p "${SLOT_DIR}" "${BIN_DIR}"
printf '%s\n' "${SLOTS}" >"${SLOT_DIR}/count"
for i in $(seq 0 $((SLOTS - 1))); do
  : >"${SLOT_DIR}/${i}"
done
install -m 0755 "${WRAPPER_SRC}" "${BIN_DIR}/docker"
# Optional direct name for debugging
install -m 0755 "${WRAPPER_SRC}" "${BIN_DIR}/swegen-docker"

echo "installed slots=${SLOTS} dir=${SLOT_DIR}"
echo "wrapper=${BIN_DIR}/docker"
ls -la "${SLOT_DIR}" | head -20
ls -la "${BIN_DIR}/docker"
# smoke: non-build should not need slots (just resolve binary)
"${BIN_DIR}/docker" version >/dev/null
echo "smoke docker version via wrapper: ok"
