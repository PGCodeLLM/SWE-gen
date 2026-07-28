"""Typed contracts shared by all distributed pipeline stages."""

from datetime import datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PipelineStage(StrEnum):
    """A durable processing stage in the relay pipeline."""

    GENERATE = "generate"
    VALIDATE = "validate"
    REWARD = "reward"
    PUSH = "push"

    @property
    def next_stage(self) -> "PipelineStage | None":
        """Return the only valid successor, or ``None`` for the final stage."""

        return _NEXT_STAGE[self]


class QueueName(StrEnum):
    """Fixed PGMQ queue names (PGMQ limits names to 47 characters)."""

    GENERATE = "swegen_generate"
    VALIDATE = "swegen_validate"
    REWARD = "swegen_reward"
    PUSH = "swegen_push"
    DEAD = "swegen_dead"


class RetryDisposition(StrEnum):
    """Action taken after a worker reports a failed delivery."""

    RETRY = "retry"
    DEAD_LETTER = "dead_letter"


_NEXT_STAGE: dict[PipelineStage, PipelineStage | None] = {
    PipelineStage.GENERATE: PipelineStage.VALIDATE,
    PipelineStage.VALIDATE: PipelineStage.REWARD,
    PipelineStage.REWARD: PipelineStage.PUSH,
    PipelineStage.PUSH: None,
}

_QUEUE_BY_STAGE: dict[PipelineStage, QueueName] = {
    PipelineStage.GENERATE: QueueName.GENERATE,
    PipelineStage.VALIDATE: QueueName.VALIDATE,
    PipelineStage.REWARD: QueueName.REWARD,
    PipelineStage.PUSH: QueueName.PUSH,
}


def queue_for_stage(stage: PipelineStage) -> QueueName:
    """Return the processing queue dedicated to ``stage``."""

    return _QUEUE_BY_STAGE[stage]


class QueueMessage(BaseModel):
    """Identifier-only handoff payload stored in PGMQ."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    event_id: UUID
    task_id: str = Field(min_length=1)
    task_version: int = Field(gt=0)
    stage: PipelineStage
    attempt: int = Field(gt=0)
    trace_id: UUID
    enqueued_at: datetime

    @field_validator("task_id")
    @classmethod
    def validate_task_id(cls, value: str) -> str:
        """Reject whitespace-only identifiers and store the canonical value."""

        value = value.strip()
        if not value:
            raise ValueError("task_id must not be blank")
        return value

    @field_validator("enqueued_at")
    @classmethod
    def validate_enqueued_at(cls, value: datetime) -> datetime:
        """Require an unambiguous timestamp for cross-node handoffs."""

        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("enqueued_at must be timezone-aware")
        return value


class ClaimedMessage(BaseModel):
    """A validated PGMQ delivery and its visibility metadata."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    queue: QueueName
    msg_id: int = Field(gt=0)
    read_count: int = Field(gt=0)
    enqueued_at: datetime
    visible_at: datetime
    message: QueueMessage

    @field_validator("enqueued_at", "visible_at")
    @classmethod
    def validate_pgmq_timestamp(cls, value: datetime) -> datetime:
        """Reject ambiguous timestamps returned by a misconfigured driver."""

        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("PGMQ timestamps must be timezone-aware")
        return value


class QueueMetrics(BaseModel):
    """A point-in-time snapshot returned by ``pgmq.metrics``."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    queue: QueueName
    queue_length: int = Field(ge=0)
    visible_length: int = Field(ge=0)
    newest_message_age_seconds: int | None = Field(default=None, ge=0)
    oldest_message_age_seconds: int | None = Field(default=None, ge=0)
    total_messages: int = Field(ge=0)
    scraped_at: datetime

    @field_validator("scraped_at")
    @classmethod
    def validate_scraped_at(cls, value: datetime) -> datetime:
        """Require an unambiguous metrics timestamp."""

        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("scraped_at must be timezone-aware")
        return value
