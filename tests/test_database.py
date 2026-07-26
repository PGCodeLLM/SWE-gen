from __future__ import annotations

from swegen.database import DatabasePRTask, PRTaskDatabase
from swegen.model_settings import DatabaseSettings


class FakeCursor:
    def __init__(self):
        self.calls = []
        self._stage = ""
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=None):
        self.calls.append((str(query), params))
        rendered = str(query)
        if "COUNT" in rendered:
            self._stage = "candidates"
        elif "pg_try_advisory" in rendered:
            self._stage = "lock"
        elif "GREATEST" in rendered:
            self._stage = "release"
            self.rowcount = 1
        elif "UPDATE" in rendered:
            self._stage = "update"

    def fetchall(self):
        if self._stage == "candidates":
            return [("owner/repo", 0, 1.5, 3)]
        if self._stage == "update":
            return [
                ("owner/repo", 12, "base-12", "owner__repo-12", 3),
                ("owner/repo", 10, "base-10", "owner__repo-10", 1),
                ("owner/repo", 11, "base-11", "owner__repo-11", 1),
            ]
        return []

    def fetchone(self):
        return (True,)


class FakeConnection:
    def __init__(self):
        self.cursor_instance = FakeCursor()
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_args):
        self.committed = exc_type is None
        return False

    def cursor(self):
        return self.cursor_instance


def _database(
    *,
    exclude_languages=("Python", "rust", "python"),
    pr_categories=("Feature", "bugfix"),
) -> PRTaskDatabase:
    return PRTaskDatabase(
        DatabaseSettings(
            host="localhost",
            port=5432,
            database="mindforge",
            user="postgres",
            password="secret",
            table="swegen.pr_tasks",
            max_retries=3,
            exclude_languages=exclude_languages,
            pr_categories=pr_categories,
        )
    )


def test_claim_repo_package_updates_lease_and_retry_in_same_transaction(monkeypatch):
    database = _database()
    connection = FakeConnection()
    monkeypatch.setattr(database, "_connect", lambda: connection)

    tasks = database.claim_repo_package(
        force_rebuild=False,
        include_obs_missing=False,
        lease_seconds_per_task=100,
    )

    assert [task.instance_id for task in tasks] == [
        "owner__repo-11",
        "owner__repo-10",
        "owner__repo-12",
    ]
    assert [task.swegen_retries for task in tasks] == [1, 1, 3]
    assert connection.committed
    update_call = next(call for call in connection.cursor_instance.calls if "UPDATE" in call[0])
    assert update_call[1] == (
        100,
        "owner/repo",
        3,
        ["python", "rust"],
        ["feature", "bugfix"],
    )
    assert "swegen_retries" in update_call[0]
    assert "COALESCE(swegen_retries, 0) < %s" in update_call[0]
    assert "primary_language" in update_call[0]
    assert "pr_category" in update_call[0]
    assert "unlock_time" in update_call[0]
    candidate_query, candidate_params = connection.cursor_instance.calls[0]
    assert candidate_params == (3, ["python", "rust"], ["feature", "bugfix"])
    assert "swegen_bz_passed" in candidate_query
    assert "obs_exists" in candidate_query
    assert "MIN(COALESCE(swegen_retries, 0))" in candidate_query
    assert "AVG(COALESCE(swegen_retries, 0))" in candidate_query
    assert "ORDER BY min_retries, avg_retries, task_count DESC, repo" in candidate_query


def test_force_and_obs_flags_remove_only_the_requested_filters():
    eligible = str(
        PRTaskDatabase._eligibility_sql(
            force_rebuild=True,
            include_obs_missing=True,
        )
    )
    assert "unlock_time" in eligible
    assert "swegen_retries" in eligible
    assert "pr_category" in eligible
    assert "primary_language" not in eligible
    assert "swegen_bz_passed" not in eligible
    assert "obs_exists" not in eligible


def test_excluded_languages_add_primary_language_filter():
    eligible = str(
        PRTaskDatabase._eligibility_sql(
            force_rebuild=False,
            include_obs_missing=False,
            exclude_languages=("python",),
        )
    )

    assert "primary_language" in eligible
    assert "pr_category" in eligible


def test_claim_parameters_omit_language_array_when_exclusion_list_is_empty(monkeypatch):
    database = _database(exclude_languages=(), pr_categories=("feature",))
    connection = FakeConnection()
    monkeypatch.setattr(database, "_connect", lambda: connection)

    database.claim_repo_package(
        force_rebuild=False,
        include_obs_missing=False,
        lease_seconds_per_task=100,
    )

    candidate_params = connection.cursor_instance.calls[0][1]
    update_params = next(
        params for query, params in connection.cursor_instance.calls if "UPDATE" in query
    )
    assert candidate_params == (3, ["feature"])
    assert update_params == (100, "owner/repo", 3, ["feature"])


def test_release_claims_unlocks_and_restores_retry_count(monkeypatch):
    database = _database()
    connection = FakeConnection()
    monkeypatch.setattr(database, "_connect", lambda: connection)
    task = DatabasePRTask(
        repo="owner/repo",
        pull_number=10,
        base_commit="base",
        instance_id="owner__repo-10",
        swegen_retries=3,
    )

    released = database.release_claims([task])

    assert released == 1
    query, params = connection.cursor_instance.calls[0]
    assert "unlock_time = CURRENT_TIMESTAMP" in query
    assert "swegen_retries = GREATEST" in query
    assert params == ("owner__repo-10", 3)
