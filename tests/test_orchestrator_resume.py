import json

from orchestrator import Entry, build_packages, task_dir_name


def test_build_packages_retries_failed_partial_tasks_and_skips_successes(tmp_path) -> None:
    output_dir = tmp_path / "tasks"
    postprocessed_dir = tmp_path / "tasks_voyager_postprocessed"
    output_dir.mkdir()
    postprocessed_dir.mkdir()

    successful = Entry(repo="owner/repo", pull_number="2")
    failed = Entry(repo="owner/repo", pull_number="1")

    (output_dir / task_dir_name(failed.repo, failed.pull_number)).mkdir()
    (postprocessed_dir / task_dir_name(successful.repo, successful.pull_number)).mkdir()

    progress_path = tmp_path / "orchestrator-progress.jsonl"
    progress_path.write_text("")
    instance_status_path = tmp_path / "orchestrator-instance-status.jsonl"
    instance_status_path.write_text(
        json.dumps(
            {
                "event": "instance_status",
                "status": "success",
                "instance": task_dir_name(successful.repo, successful.pull_number),
            }
        )
        + "\n"
    )

    packages, skipped = build_packages(
        [successful, failed],
        output_dir,
        False,
        state_dir=tmp_path,
        progress_path=progress_path,
        instance_status_path=instance_status_path,
        postprocessed_dir=postprocessed_dir,
    )

    assert skipped == 1
    assert packages == [[failed]]
