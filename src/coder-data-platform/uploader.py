#!/usr/bin/env python3
"""Upload Harbor instances to the CWM platform as Oracle runs.

The uploader submits task directories in fixed-size batches and waits for each
submitted run to finish before submitting the next batch.
"""

from __future__ import annotations

import argparse
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile


PLATFORM_API_BASE_URL = "http://159.138.1.45:6066"
PLATFORM_USERNAME = "Alex Yang"
PLATFORM_PASSWORD = "Alex Yang"
PRODUCTION_LINE_ID = "feature-implement"
DEFAULT_BATCH_SIZE = 300
DEFAULT_WAIT_TIMEOUT_SECONDS = 24 * 60 * 60
DEFAULT_POLL_INTERVAL_SECONDS = 60.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload Harbor instance directories to CWM Oracle runs."
    )
    parser.add_argument(
        "input_dir",
        type=Path,
        help="Folder containing Harbor instance directories.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of instances per upload batch. Defaults to {DEFAULT_BATCH_SIZE}.",
    )
    parser.add_argument(
        "--batch-name-prefix",
        default="swegen-oracle",
        help="Prefix for generated CWM batch names.",
    )
    parser.add_argument(
        "--wait-timeout-seconds",
        type=int,
        default=DEFAULT_WAIT_TIMEOUT_SECONDS,
        help="Maximum time to wait for each submitted batch run.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        help="Seconds between run status polls.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional path to write the upload report JSON.",
    )
    return parser.parse_args()


def iter_instances(input_dir: Path) -> list[Path]:
    if not input_dir.is_dir():
        raise ValueError(f"input directory does not exist: {input_dir}")
    return sorted(
        child
        for child in input_dir.iterdir()
        if child.is_dir()
        and (child / "task.toml").is_file()
        and (child / "environment").is_dir()
        and (child / "tests").is_dir()
        and (child / "solution").is_dir()
    )


def batched(items: list[Path], batch_size: int) -> list[list[Path]]:
    if batch_size <= 0:
        raise ValueError("batch size must be positive")
    return [
        items[index : index + batch_size]
        for index in range(0, len(items), batch_size)
    ]


def make_archive(instance_dir: Path, output_dir: Path) -> Path:
    archive_path = output_dir / f"{instance_dir.name}.zip"
    archive_root = PurePosixPath(instance_dir.name)
    with ZipFile(archive_path, mode="w", compression=ZIP_DEFLATED) as archive:
        for path in sorted(instance_dir.rglob("*")):
            archive_name = archive_root / PurePosixPath(
                path.relative_to(instance_dir).as_posix()
            )
            if path.is_dir():
                archive.writestr(f"{archive_name}/", b"")
            else:
                archive.write(path, str(archive_name))
    return archive_path


def make_client() -> Any:
    try:
        from cwm import CwmClient
    except ImportError as exc:
        raise RuntimeError("cannot import cwm.CwmClient") from exc

    return CwmClient(
        platform_api_base_url=PLATFORM_API_BASE_URL,
        platform_username=PLATFORM_USERNAME,
        platform_password=PLATFORM_PASSWORD,
    )


def public_run_id(run: dict[str, Any]) -> str:
    for key in ("public_id", "id", "run_id"):
        value = run.get(key)
        if value is not None and str(value).strip():
            return str(value)
    raise ValueError(f"submitted run did not include a run id: {run}")


def submit_batch(
    client: Any,
    *,
    batch_name: str,
    archives: list[Path],
    wait_timeout_seconds: int,
    poll_interval_seconds: float,
) -> dict[str, Any]:
    run = client.tasks.submit_task_dir_run(
        batch_name=batch_name,
        archives=archives,
        production_line_id=PRODUCTION_LINE_ID,  # type: ignore[arg-type]
        execution_backend="cpu",
        verification_mode="enabled",
        runtime_agent_injection_enabled=False,
        agent_model_combinations=[
            {"agent_type": "oracle", "traj_count": 1},
        ],
        confirm=False,
    )
    run_id = public_run_id(run)
    final = client.tasks.wait(
        run_id,
        timeout_seconds=wait_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )
    return {
        "batch_name": batch_name,
        "run_id": run_id,
        "submitted": run,
        "final": final,
    }


def process_instances(
    input_dir: Path,
    *,
    batch_size: int,
    batch_name_prefix: str,
    wait_timeout_seconds: int,
    poll_interval_seconds: float,
) -> dict[str, Any]:
    instances = iter_instances(input_dir)
    if not instances:
        raise ValueError(f"no Harbor instances found under: {input_dir}")

    batches = batched(instances, batch_size)
    report: dict[str, Any] = {
        "input_dir": str(input_dir),
        "production_line_id": PRODUCTION_LINE_ID,
        "batch_size": batch_size,
        "instance_count": len(instances),
        "batch_count": len(batches),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "batches": [],
    }

    with make_client() as client:
        for batch_number, batch in enumerate(batches, start=1):
            batch_name = f"{batch_name_prefix}-{batch_number:04d}"
            print(
                f"submitting batch {batch_number}/{len(batches)} "
                f"({len(batch)} instances): {batch_name}",
                flush=True,
            )
            with tempfile.TemporaryDirectory(prefix="swegen-upload-") as tmp:
                tmp_dir = Path(tmp)
                archives = [make_archive(instance, tmp_dir) for instance in batch]
                result = submit_batch(
                    client,
                    batch_name=batch_name,
                    archives=archives,
                    wait_timeout_seconds=wait_timeout_seconds,
                    poll_interval_seconds=poll_interval_seconds,
                )
            result["instances"] = [instance.name for instance in batch]
            report["batches"].append(result)
            final_status = result["final"].get("status") or result["final"].get(
                "result_status"
            )
            print(
                f"completed batch {batch_number}/{len(batches)}: "
                f"run_id={result['run_id']} status={final_status}",
                flush=True,
            )

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    return report


def main() -> int:
    args = parse_args()
    report = process_instances(
        args.input_dir.resolve(),
        batch_size=args.batch_size,
        batch_name_prefix=args.batch_name_prefix,
        wait_timeout_seconds=args.wait_timeout_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
    )
    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"output_json={args.output_json}")
    print(f"uploaded_instances={report['instance_count']}")
    print(f"submitted_batches={report['batch_count']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
