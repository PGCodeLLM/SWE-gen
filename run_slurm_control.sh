#!/bin/bash
set -euo pipefail

workspace="${SWEGEN_SLURM_WORKSPACE:-/data/work/slurm-swegen}"
config="${SWEGEN_CONTROL_CONFIG:-/data/work/alex/SWE-gen/swegen-config.yaml}"

if [[ "${EUID:-$(id -u)}" != "0" ]]; then
  echo "run_slurm_control.sh must run as root so pause/resume can use scontrol" >&2
  exit 1
fi

cd "$workspace"
exec .venv/bin/python src/slurm_control.py --config "$config" --workspace "$workspace" --initialize
