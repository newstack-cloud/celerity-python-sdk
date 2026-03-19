"""Tests for cache types and error classes."""

from __future__ import annotations

from celerity.resources.cache.errors import CacheError
from celerity.resources.cache.types import SetOptions, SortedSetMember, TransactionResult


class TestCacheError:
    def test_message(self) -> None:
        err = CacheError("something went wrong")
        assert str(err) == "something went wrong"

    def test_with_cause(self) -> None:
        cause = RuntimeError("redis down")
        err = CacheError("connection failed", cause=cause)
        assert err.__cause__ is cause

    def test_without_cause(self) -> None:
        err = CacheError("oops")
        assert err.__cause__ is None


class TestSetOptions:
    def test_defaults(self) -> None:
        opts = SetOptions()
        assert opts.ttl_seconds is None
        assert opts.if_not_exists is False
        assert opts.if_exists is False

    def test_with_ttl(self) -> None:
        opts = SetOptions(ttl_seconds=60)
        assert opts.ttl_seconds == 60

    def test_if_not_exists(self) -> None:
        opts = SetOptions(if_not_exists=True)
        assert opts.if_not_exists is True

    def test_if_exists(self) -> None:
        opts = SetOptions(if_exists=True)
        assert opts.if_exists is True


class TestSortedSetMember:
    def test_construction(self) -> None:
        m = SortedSetMember(member="alice", score=1.5)
        assert m.member == "alice"
        assert m.score == 1.5

    def test_equality(self) -> None:
        a = SortedSetMember(member="bob", score=2.0)
        b = SortedSetMember(member="bob", score=2.0)
        assert a == b


class TestTransactionResult:
    def test_construction(self) -> None:
        r = TransactionResult(results=["OK", 1, True])
        assert r.results == ["OK", 1, True]

    def test_empty(self) -> None:
        r = TransactionResult(results=[])
        assert r.results == []
