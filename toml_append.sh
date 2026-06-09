#!/usr/bin/env bash
#
# toml_append.sh — append a [cwm_task_metadata] block to every task.toml.
#
# For each tasks/<subfolder>/task.toml, append the following block (unless the
# task.toml already contains a [cwm_task_metadata] section):
#
#   [cwm_task_metadata]
#   repo_full_name = "<owner>/<repo>"
#   pr_id = "<pr_number>"
#   issue_number = "<issue_number>"      # only when present in the JSONL
#   training_domain = "feature"
#   source_commit = "<base_commit>"
#
# Metadata is looked up from a JSONL file (default 1601_repo_pr_pairs.jsonl) by
# matching each task folder name against "<repo with / -> __>-<pull_number>".
#
# Usage:
#   ./toml_append.sh [JSONL_FILE] [TASKS_DIR]
#
# Defaults: JSONL_FILE=1601_repo_pr_pairs.jsonl  TASKS_DIR=tasks

set -euo pipefail

JSONL="${1:-1601_repo_pr_pairs.jsonl}"
TASKS_DIR="${2:-tasks}"

command -v jq >/dev/null 2>&1 || { echo "toml_append.sh: jq is required" >&2; exit 1; }
[[ -f "$JSONL" ]] || { echo "toml_append.sh: JSONL not found: $JSONL" >&2; exit 1; }
[[ -d "$TASKS_DIR" ]] || { echo "toml_append.sh: tasks dir not found: $TASKS_DIR" >&2; exit 1; }

# Build lookup tables keyed by the task folder name derived from each JSONL row:
#   "<repo with '/' replaced by '__'>-<pull_number>", lowercased so it matches
#   task folders even when the orchestrator lowercased mixed-case repo names
#   (e.g. PaddlePaddle/PaddleFormers -> paddlepaddle__paddleformers).
declare -A REPO PULL BASE ISSUE
while IFS=$'\t' read -r key repo pull base issue; do
    [[ -n "$key" ]] || continue
    REPO["$key"]="$repo"
    PULL["$key"]="$pull"
    BASE["$key"]="$base"
    ISSUE["$key"]="$issue"
done < <(jq -r '
    [
        ((.repo | gsub("/"; "__")) + "-" + (.pull_number | tostring) | ascii_downcase),
        .repo,
        (.pull_number | tostring),
        (.base_commit // ""),
        (.issue_number // "" | tostring)
    ] | @tsv
' "$JSONL")

# TOML basic-string escaping for backslash and double-quote.
esc() { printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'; }

appended=0 skipped=0 missing=0
for toml in "$TASKS_DIR"/*/task.toml; do
    [[ -f "$toml" ]] || continue
    folder="$(basename "$(dirname "$toml")")"
    key="${folder,,}"  # lowercase to match the lowercased JSONL keys

    if grep -q '^\[cwm_task_metadata\]' "$toml"; then
        skipped=$((skipped + 1))
        continue
    fi

    if [[ -z "${REPO[$key]+x}" ]]; then
        echo "toml_append.sh: no JSONL entry for '$folder'; skipping" >&2
        missing=$((missing + 1))
        continue
    fi

    repo="${REPO[$key]}"
    pr="${PULL[$key]}"
    base="${BASE[$key]}"
    issue="${ISSUE[$key]}"

    # base_commit is populated in the JSONL "soon"; until then, skip rather than
    # write an empty source_commit that the presence check would never refill.
    if [[ -z "$base" ]]; then
        echo "toml_append.sh: no base_commit yet for '$folder'; skipping" >&2
        missing=$((missing + 1))
        continue
    fi

    {
        printf '\n[cwm_task_metadata]\n'
        printf 'repo_full_name = "%s"\n' "$(esc "$repo")"
        printf 'pr_id = "%s"\n' "$(esc "$pr")"
        if [[ -n "$issue" ]]; then
            printf 'issue_number = "%s"\n' "$(esc "$issue")"
        fi
        printf 'training_domain = "feature"\n'
        printf 'source_commit = "%s"\n' "$(esc "$base")"
    } >> "$toml"

    appended=$((appended + 1))
done

echo "toml_append.sh: appended=$appended skipped(existing)=$skipped skipped(no match/no base_commit)=$missing"
