import json
from concurrent.futures import ProcessPoolExecutor

from swegen.create.task_reference import TaskReferenceStore


def save_reference(path, index: int) -> bool:
    return TaskReferenceStore(path).save(
        repo=f"owner/repo-{index}",
        task_id=f"owner__repo-{index}-{index}",
        pr_number=index,
    )


def test_save_uses_atomic_json_and_preserves_existing_references(tmp_path) -> None:
    path = tmp_path / "task_references.json"
    store = TaskReferenceStore(path)

    assert store.save("owner/repo", "owner__repo-1", 1)
    assert store.save("owner/repo", "owner__repo-2", 2)

    data = json.loads(path.read_text())
    assert [reference["pr_number"] for reference in data["owner/repo"]] == [1, 2]
    assert store.lock_file.exists()
    assert not list(tmp_path.glob(".task_references.json.tmp.*"))


def test_concurrent_saves_do_not_lose_repositories(tmp_path) -> None:
    path = tmp_path / "task_references.json"

    with ProcessPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(save_reference, [path] * 16, range(16)))

    assert all(results)
    data = json.loads(path.read_text())
    assert set(data) == {f"owner/repo-{index}" for index in range(16)}
