"""Tests for topic types, errors, and option dataclasses."""

from __future__ import annotations

import pytest

from celerity.resources.topic.errors import TopicError
from celerity.resources.topic.types import (
    BatchPublishEntry,
    BatchPublishFailure,
    BatchPublishResult,
    BatchPublishSuccess,
    PublishOptions,
)


class TestTopicError:
    def test_message(self) -> None:
        err = TopicError("something went wrong")
        assert str(err) == "something went wrong"
        assert err.resource is None
        assert err.__cause__ is None

    def test_resource(self) -> None:
        err = TopicError("fail", resource="my-topic")
        assert err.resource == "my-topic"

    def test_cause(self) -> None:
        cause = RuntimeError("original")
        err = TopicError("wrapped", cause=cause)
        assert err.__cause__ is cause


class TestPublishOptions:
    def test_defaults(self) -> None:
        opts = PublishOptions()
        assert opts.group_id is None
        assert opts.deduplication_id is None
        assert opts.subject is None
        assert opts.attributes is None

    def test_all_fields(self) -> None:
        opts = PublishOptions(
            group_id="grp-1",
            deduplication_id="dedup-1",
            subject="Hello",
            attributes={"key": "value"},
        )
        assert opts.group_id == "grp-1"
        assert opts.deduplication_id == "dedup-1"
        assert opts.subject == "Hello"
        assert opts.attributes == {"key": "value"}

    def test_frozen(self) -> None:
        opts = PublishOptions()
        with pytest.raises(AttributeError):
            opts.group_id = "changed"  # type: ignore[misc]


class TestBatchPublishEntry:
    def test_minimal(self) -> None:
        entry = BatchPublishEntry(id="e1", body="hello")
        assert entry.id == "e1"
        assert entry.body == "hello"
        assert entry.group_id is None
        assert entry.subject is None
        assert entry.attributes is None

    def test_all_fields(self) -> None:
        entry = BatchPublishEntry(
            id="e1",
            body="hello",
            group_id="grp",
            deduplication_id="dedup",
            subject="Subject",
            attributes={"tag": "val"},
        )
        assert entry.group_id == "grp"
        assert entry.deduplication_id == "dedup"
        assert entry.subject == "Subject"
        assert entry.attributes == {"tag": "val"}


class TestBatchPublishSuccess:
    def test_construction(self) -> None:
        s = BatchPublishSuccess(id="e1", message_id="msg-123")
        assert s.id == "e1"
        assert s.message_id == "msg-123"


class TestBatchPublishFailure:
    def test_construction(self) -> None:
        f = BatchPublishFailure(id="e2", code="InternalError", message="boom", sender_fault=False)
        assert f.id == "e2"
        assert f.code == "InternalError"
        assert f.message == "boom"
        assert f.sender_fault is False

    def test_sender_fault_true(self) -> None:
        f = BatchPublishFailure(id="e3", code="InvalidInput", message="bad", sender_fault=True)
        assert f.sender_fault is True


class TestBatchPublishResult:
    def test_mixed(self) -> None:
        result = BatchPublishResult(
            successful=[BatchPublishSuccess(id="e1", message_id="m1")],
            failed=[BatchPublishFailure(id="e2", code="Err", message="fail", sender_fault=False)],
        )
        assert len(result.successful) == 1
        assert len(result.failed) == 1

    def test_empty_defaults(self) -> None:
        result = BatchPublishResult()
        assert result.successful == []
        assert result.failed == []
