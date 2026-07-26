from __future__ import annotations

from dataclasses import dataclass

from psycopg import connect, sql

from swegen.model_settings import DatabaseSettings


@dataclass(frozen=True)
class DatabasePRTask:
    repo: str
    pull_number: int
    base_commit: str
    instance_id: str
    swegen_retries: int


class PRTaskDatabase:
    """Atomic repository-package allocator for ``swegen.pr_tasks``."""

    def __init__(self, settings: DatabaseSettings):
        self.settings = settings
        schema, table = settings.table.split(".", 1)
        self._relation = sql.Identifier(schema, table)

    def _connect(self):
        return connect(
            host=self.settings.host,
            port=self.settings.port,
            dbname=self.settings.database,
            user=self.settings.user,
            password=self.settings.password,
            connect_timeout=self.settings.connect_timeout,
        )

    @staticmethod
    def _eligibility_sql(*, force_rebuild: bool, include_obs_missing: bool) -> sql.SQL:
        clauses = [
            sql.SQL("(unlock_time IS NULL OR unlock_time <= CURRENT_TIMESTAMP)"),
            sql.SQL("COALESCE(swegen_retries, 0) < %s"),
        ]
        if not force_rebuild:
            clauses.append(sql.SQL("COALESCE(swegen_bz_passed, FALSE) = FALSE"))
        if not include_obs_missing:
            clauses.append(sql.SQL("obs_exists = TRUE"))
        return sql.SQL(" AND ").join(clauses)

    def claim_repo_package(
        self,
        *,
        force_rebuild: bool,
        include_obs_missing: bool,
        lease_seconds_per_task: int,
    ) -> list[DatabasePRTask]:
        """Claim one eligible repository group in a single transaction.

        A transaction-scoped advisory lock serializes allocation by repository.
        The row update that sets ``unlock_time`` and increments
        ``swegen_retries`` is committed with the selection, so another SWE-gen
        process can never observe a partially claimed package.
        """
        eligible = self._eligibility_sql(
            force_rebuild=force_rebuild,
            include_obs_missing=include_obs_missing,
        )
        candidates_query = sql.SQL(
            "SELECT repo, "
            "MIN(COALESCE(swegen_retries, 0)) AS min_retries, "
            "AVG(COALESCE(swegen_retries, 0)) AS avg_retries, "
            "COUNT(*) AS task_count "
            "FROM {table} WHERE {eligible} "
            "GROUP BY repo ORDER BY min_retries, avg_retries, task_count DESC, repo"
        ).format(table=self._relation, eligible=eligible)

        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(candidates_query, (self.settings.max_retries,))
                candidates = cursor.fetchall()
                for repo, _min_retries, _avg_retries, _task_count in candidates:
                    cursor.execute(
                        "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))",
                        (repo,),
                    )
                    if not cursor.fetchone()[0]:
                        continue

                    # Re-check eligibility after obtaining the repo lock. A
                    # competing allocator may have claimed it since the
                    # candidate list was read.
                    lease_seconds = max(1, lease_seconds_per_task)
                    update_query = sql.SQL(
                        "UPDATE {table} SET "
                        "unlock_time = CURRENT_TIMESTAMP + (%s * INTERVAL '1 second'), "
                        "swegen_retries = COALESCE(swegen_retries, 0) + 1, "
                        "instance_id = COALESCE(NULLIF(instance_id, ''), "
                        "LOWER(REPLACE(repo, '/', '__')) || '-' || pull_number::text) "
                        "WHERE repo = %s AND {eligible} "
                        "RETURNING repo, pull_number, COALESCE(base_commit, ''), "
                        "instance_id, swegen_retries"
                    ).format(table=self._relation, eligible=eligible)
                    cursor.execute(
                        update_query,
                        (lease_seconds, repo, self.settings.max_retries),
                    )
                    rows = cursor.fetchall()
                    if not rows:
                        continue
                    rows.sort(key=lambda row: (int(row[4]), -int(row[1])))
                    return [
                        DatabasePRTask(
                            repo=str(row[0]),
                            pull_number=int(row[1]),
                            base_commit=str(row[2]),
                            instance_id=str(row[3]),
                            swegen_retries=int(row[4]),
                        )
                        for row in rows
                    ]
        return []

    def mark_swegen_passed(self, instance_id: str) -> None:
        query = sql.SQL("UPDATE {table} SET swegen_bz_passed = TRUE WHERE instance_id = %s").format(
            table=self._relation
        )
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (instance_id,))
                if cursor.rowcount != 1:
                    raise RuntimeError(
                        f"Expected one database row for instance_id={instance_id!r}; "
                        f"updated {cursor.rowcount}"
                    )
