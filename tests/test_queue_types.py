"""Tests for queue types, errors, and option dataclasses."""

from __future__ import annotations

import pytest

from celerity.resources.queue.errors import QueueError
from celerity.resources.queue.types import (
    BatchSendEntry,
    BatchSendFailure,
    BatchSendResult,
    BatchSendSuccess,
    SendMessageOptions,
)


class TestQueueError:
    def test_message(self) -> None:
        err = QueueError("something went wrong")
        assert str(err) == "something went wrong"
        assert err.resource is None
        assert err.__cause__ is None

    def test_resource(self) -> None:
        err = QueueError("fail", resource="my-queue")
        assert err.resource == "my-queue"

    def test_cause(self) -> None:
        cause = RuntimeError("original")
        err = QueueError("wrapped", cause=cause)
        assert err.__cause__ is cause


class TestSendMessageOptions:
    def test_defaults(self) -> None:
        opts = SendMessageOptions()
        assert opts.group_id is None
        assert opts.deduplication_id is None
        assert opts.delay_seconds is None
        assert opts.attributes is None

    def test_all_fields(self) -> None:
        opts = SendMessageOptions(
            group_id="grp-1",
            deduplication_id="dedup-1",
            delay_seconds=30,
            attributes={"key": "value"},
        )
        assert opts.group_id == "grp-1"
        assert opts.deduplication_id == "dedup-1"
        assert opts.delay_seconds == 30
        assert opts.attributes == {"key": "value"}

    def test_frozen(self) -> None:
        opts = SendMessageOptions()
        with pytest.raises(AttributeError):
            opts.group_id = "changed"  # type: ignore[misc]


class TestBatchSendEntry:
    def test_minimal(self) -> None:
        entry = BatchSendEntry(id="e1", body="hello")
        assert entry.id == "e1"
        assert entry.body == "hello"
        assert entry.group_id is None
        assert entry.deduplication_id is None
        assert entry.delay_seconds is None
        assert entry.attributes is None

    def test_all_fields(self) -> None:
        entry = BatchSendEntry(
            id="e1",
            body="hello",
            group_id="grp",
            deduplication_id="dedup",
            delay_seconds=5,
            attributes={"tag": "val"},
        )
        assert entry.group_id == "grp"
        assert entry.deduplication_id == "dedup"
        assert entry.delay_seconds == 5
        assert entry.attributes == {"tag": "val"}


class TestBatchSendSuccess:
    def test_construction(self) -> None:
        s = BatchSendSuccess(id="e1", message_id="msg-123")
        assert s.id == "e1"
        assert s.message_id == "msg-123"


class TestBatchSendFailure:
    def test_construction(self) -> None:
        f = BatchSendFailure(id="e2", code="InternalError", message="boom", sender_fault=False)
        assert f.id == "e2"
        assert f.code == "InternalError"
        assert f.message == "boom"
        assert f.sender_fault is False

    def test_sender_fault_true(self) -> None:
        f = BatchSendFailure(id="e3", code="InvalidInput", message="bad", sender_fault=True)
        assert f.sender_fault is True


class TestBatchSendResult:
    def test_mixed(self) -> None:
        result = BatchSendResult(
            successful=[BatchSendSuccess(id="e1", message_id="m1")],
            failed=[BatchSendFailure(id="e2", code="Err", message="fail", sender_fault=False)],
        )
        assert len(result.successful) == 1
        assert len(result.failed) == 1
        assert result.successful[0].id == "e1"
        assert result.failed[0].id == "e2"

    def test_empty_defaults(self) -> None:
        result = BatchSendResult()
        assert result.successful == []
        assert result.failed == []
