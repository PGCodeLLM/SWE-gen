#!/usr/bin/env bash
set -euo pipefail

INPUT_DIR="data_cache/successful_20260612_200042_bz/"
OUTPUT_DIR="data_cache/successful_20260612_200042_bz_codex_hacking_out"
# CONFIG="reward_hacking_detector/hacking_checker_QWEN_ONLY.toml"
CONFIG="src/reward_hacking_detector/hacking_checker_CODEX_ONLY.toml"

start=$(date +%s)
echo "Started at: $(date)"

python src/reward_hacking_detector/hacking.py \
    --input_dir "$INPUT_DIR" \
    --output_dir "$OUTPUT_DIR" \
    --config "$CONFIG" \
    --max-concurrency 8

end=$(date +%s)
elapsed=$((end - start))
echo "Finished at: $(date)"
printf 'Elapsed time: %02dh:%02dm:%02ds (%d seconds)\n' \
    $((elapsed / 3600)) $(((elapsed % 3600) / 60)) $((elapsed % 60)) "$elapsed"
