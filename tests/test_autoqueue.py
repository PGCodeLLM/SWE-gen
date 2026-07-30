from __future__ import annotations

from contextlib import contextmanager

from autoqueue import fill_generate_queue
from swegen.model_settings import AutoqueueSettings


def normalize_sql(query: str) -> str:
    return " ".join(query.split())


class FakeCursor:
    def __init__(self, rows=(), *, rowcount: int = 0) -> None:
        self.rows = list(rows)
        self.rowcount = rowcount

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows.copy()


class AutoqueueConnection:
    def __init__(
        self,
        *,
        queued: int,
        candidates: list[tuple[str, int]],
        duplicate_repos: set[str] | None = None,
    ) -> None:
        self.queued = queued
        self.candidates = candidates
        self.duplicate_repos = duplicate_repos or set()
        self.calls: list[tuple[str, tuple[object, ...], int]] = []
        self.events: list[str] = []
        self.transaction_depth = 0
        self.next_message_id = 70

    @contextmanager
    def transaction(self):
        self.transaction_depth += 1
        self.events.append(f"transaction-enter-{self.transaction_depth}")
        try:
            yield
        except BaseException:
            self.events.append(f"transaction-rollback-{self.transaction_depth}")
            raise
        else:
            self.events.append(f"transaction-exit-{self.transaction_depth}")
        finally:
            self.transaction_depth -= 1

    def execute(self, query: str, params=None):
        sql = normalize_sql(query)
        parameters = tuple(params or ())
        assert self.transaction_depth > 0, f"SQL escaped transaction: {sql}"
        self.calls.append((sql, parameters, self.transaction_depth))

        if sql.startswith("SELECT pg_advisory_xact_lock"):
            return FakeCursor()
        if sql.startswith("SELECT count(*) AS queued"):
            return FakeCursor(({"queued": self.queued},))
        if sql.startswith("SELECT source.repo, source.pull_number"):
            limit = int(parameters[-1])
            return FakeCursor(
                [
                    {"repo": repo, "pull_number": pull_number}
                    for repo, pull_number in self.candidates[:limit]
                ]
            )
        if sql.startswith("INSERT INTO pipeline_tasks"):
            repo = str(parameters[2])
            if repo in self.duplicate_repos:
                return FakeCursor()
            return FakeCursor(({"task_id": parameters[0], "task_version": parameters[1]},))
        if "pgmq.send" in sql:
            self.next_message_id += 1
            return FakeCursor(({"send": self.next_message_id},))
        if sql.startswith('UPDATE "public"."pr_tasks"'):
            return FakeCursor(rowcount=1)
        raise AssertionError(f"Unexpected SQL: {sql}")


def test_fill_generate_queue_counts_and_enqueues_under_one_outer_transaction() -> None:
    connection = AutoqueueConnection(
        queued=1,
        candidates=[("Owner/Repo", 9), ("Other/Repo", 8)],
    )
    settings = AutoqueueSettings(max_queued=3, require_obs=False)

    result = fill_generate_queue(connection, settings)

    assert result.queue_limit == 3
    assert result.queued_before == 1
    assert result.enqueued == ("owner__repo-9", "other__repo-8")
    assert connection.events[0] == "transaction-enter-1"
    assert connection.events[-1] == "transaction-exit-1"
    assert all(depth >= 1 for _sql, _params, depth in connection.calls)
    sql_calls = [sql for sql, _params, _depth in connection.calls]
    assert "pg_advisory_xact_lock" in sql_calls[0]
    assert "state = 'queued' AND current_stage = 'generate'" in sql_calls[1]
    candidate_call = next(call for call in connection.calls if "FOR UPDATE" in call[0])
    assert "FOR UPDATE OF source SKIP LOCKED LIMIT %s" in candidate_call[0]
    assert candidate_call[1][-1] == 2
    assert (
        sql_calls.count(
            'UPDATE "public"."pr_tasks" SET instance_id = COALESCE(NULLIF(instance_id, \'\'), %s), swegen_retries = COALESCE(swegen_retries, 0) + 1 WHERE repo = %s AND pull_number = %s'
        )
        == 2
    )


def test_fill_generate_queue_respects_remaining_capacity() -> None:
    connection = AutoqueueConnection(
        queued=4,
        candidates=[("Owner/One", 1), ("Owner/Two", 2)],
    )
    settings = AutoqueueSettings(max_queued=5, require_obs=False)

    result = fill_generate_queue(connection, settings)

    assert result.enqueued == ("owner__one-1",)
    candidate_call = next(call for call in connection.calls if "FOR UPDATE" in call[0])
    assert candidate_call[1][-1] == 1


def test_fill_generate_queue_does_nothing_when_backlog_is_full() -> None:
    connection = AutoqueueConnection(queued=5, candidates=[("Owner/Repo", 1)])

    result = fill_generate_queue(
        connection,
        AutoqueueSettings(max_queued=5, require_obs=False),
    )

    assert result.enqueued == ()
    assert not any("FOR UPDATE" in sql for sql, _params, _depth in connection.calls)
    assert not any("INSERT INTO pipeline_tasks" in sql for sql, _params, _depth in connection.calls)


def test_manual_enqueue_race_is_skipped_without_consuming_an_extra_slot() -> None:
    connection = AutoqueueConnection(
        queued=0,
        candidates=[("Owner/Duplicate", 1), ("Owner/Fresh", 2)],
        duplicate_repos={"owner/duplicate"},
    )

    result = fill_generate_queue(
        connection,
        AutoqueueSettings(max_queued=2, require_obs=False),
    )

    assert result.enqueued == ("owner__fresh-2",)
    updates = [sql for sql, _params, _depth in connection.calls if sql.startswith("UPDATE")]
    assert len(updates) == 1
    assert "transaction-rollback-2" in connection.events
