#!/usr/bin/env python3
"""One-shot: merge Stage-3 reward-backfill verdicts into the shared postcheck ledger.

The reward-backfill worker (Stage 3) writes pass/fail reward-hack verdicts to
its own private ledger. A non-baseline Stage-2 validation worker is supposed to
overlay those verdicts onto the shared postcheck ledger and mark each
baseline-valid instance ``accepted`` (reward-clean) or ``rejected`` (hacking).
That merge worker has not been running, so ~9.7k already-verdicted instances
are stuck as ``baseline_valid`` in the shared ledger with ``reward_hack=pending``.

This script performs that merge directly, without re-running Harbor or fetching
test bundles:

  1. Load the latest per-instance snapshot from the shared postcheck ledger.
  2. Load the latest per-instance snapshot from the reward-backfill ledger.
  3. For every instance that is baseline-valid (nop=pass/reward 0,
     oracle=pass/reward 1) AND has a terminal backfill verdict (pass/fail)
     AND is not already terminal (accepted/rejected/blacklisted) in the shared
     ledger: overlay the backfill reward_hack fields onto the shared record and
     append an ``accepted``/``rejected`` record (with ``--no-push`` semantics;
     image pushing is left to retroactive_push/push_all_verified).

Reuses the validation worker's own overlay/finalize helpers so the accepted
determination is identical to the live pipeline.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

# Import the live merge logic so we never drift from the pipeline's definition.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from slurm_validation_worker import (  # noqa: E402
    BACKFILL_TERMINAL_STATES,
    TERMINAL_STATUSES,
    append_private_jsonl,
    load_latest_postchecks,
    overlay_backfill_reward,
    reward_matches,
)
from slurm_reward_backfill_worker import baseline_is_valid  # noqa: E402


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--postcheck-ledger",
        required=True,
        type=Path,
        help="Shared postcheck-status.jsonl (read + appended to).",
    )
    parser.add_argument(
        "--reward-backfill-ledger",
        required=True,
        type=Path,
        help="Stage-3 reward-backfill private ledger (read-only).",
    )
    parser.add_argument(
        "--worker-id",
        default="backfill-merger-0",
        help="worker_id stamped onto appended records.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report counts only; do not append to the shared ledger.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Stop after merging this many instances (0 = no limit).",
    )
    args = parser.parse_args(argv)

    print(f"loading shared postcheck ledger: {args.postcheck_ledger}", flush=True)
    shared = load_latest_postchecks(args.postcheck_ledger)
    print(f"  {len(shared)} distinct instances in shared ledger", flush=True)

    print(f"loading reward-backfill ledger: {args.reward_backfill_ledger}", flush=True)
    backfill = load_latest_postchecks(args.reward_backfill_ledger)
    print(f"  {len(backfill)} distinct instances in backfill ledger", flush=True)

    merged = 0
    skipped_already_terminal = 0
    skipped_not_baseline_valid = 0
    skipped_no_terminal_backfill = 0
    accepted = 0
    rejected = 0

    for instance, record in shared.items():
        if record.get("status") in TERMINAL_STATUSES:
            skipped_already_terminal += 1
            continue
        if not baseline_is_valid(record):
            skipped_not_baseline_valid += 1
            continue
        backfill_record = backfill.get(instance)
        state = overlay_backfill_reward(record, backfill_record)
        if state not in BACKFILL_TERMINAL_STATES:
            skipped_no_terminal_backfill += 1
            continue

        reward_hack = record.get("reward_hack") if isinstance(record.get("reward_hack"), dict) else {}
        is_accepted = (
            state == "pass"
            and reward_hack.get("is_hacking") is False
        )
        record.update(
            {
                "status": "accepted" if is_accepted else "rejected",
                "stage": "complete",
                "finished_at": _now_iso(),
                "next_retry_at": None,
                "error": None,
                "worker_id": args.worker_id,
                "checker_node": os.uname().nodename,
                "merged_from_backfill": True,
            }
        )
        if is_accepted:
            accepted += 1
        else:
            rejected += 1
        merged += 1

        if not args.dry_run:
            append_private_jsonl(args.postcheck_ledger, record)

        if args.limit and merged >= args.limit:
            break

    print("", flush=True)
    print(f"merged (appended to shared ledger): {merged}", flush=True)
    print(f"  accepted: {accepted}", flush=True)
    print(f"  rejected: {rejected}", flush=True)
    print(f"skipped already terminal (accepted/rejected/blacklisted): {skipped_already_terminal}", flush=True)
    print(f"skipped not baseline-valid: {skipped_not_baseline_valid}", flush=True)
    print(f"skipped no terminal backfill verdict: {skipped_no_terminal_backfill}", flush=True)
    if args.dry_run:
        print("(dry-run: no records appended)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
