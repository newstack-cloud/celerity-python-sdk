"""Tests for Redis Cluster hash slot helpers."""

from __future__ import annotations

import pytest

from celerity.resources.cache.errors import CacheError
from celerity.resources.cache.providers.redis.cluster import (
    assert_same_slot,
    group_by_slot,
    hash_slot,
)


class TestHashSlot:
    def test_known_value(self) -> None:
        # "foo" hashes to slot 12182 via CRC16-CCITT mod 16384
        assert hash_slot("foo") == 12182

    def test_hash_tag_same_slot(self) -> None:
        assert hash_slot("{user}:profile") == hash_slot("{user}:session")

    def test_different_keys_different_slots(self) -> None:
        # Not guaranteed but statistically very likely
        assert hash_slot("alpha") != hash_slot("beta")

    def test_empty_hash_tag_uses_full_key(self) -> None:
        # Empty braces {} should use the full key
        assert hash_slot("key{}value") == hash_slot("key{}value")
        # And it should differ from using "key" alone
        assert hash_slot("key{}value") != hash_slot("key")

    def test_no_closing_brace_uses_full_key(self) -> None:
        assert hash_slot("key{tag") == hash_slot("key{tag")

    def test_slot_range(self) -> None:
        slot = hash_slot("test-key")
        assert 0 <= slot < 16384


class TestGroupBySlot:
    def test_groups_correctly(self) -> None:
        keys = ["{x}:1", "{x}:2", "{y}:1"]
        groups = group_by_slot(keys)

        x_slot = hash_slot("{x}:1")
        y_slot = hash_slot("{y}:1")

        assert x_slot in groups
        assert y_slot in groups
        assert len(groups[x_slot]) == 2
        assert len(groups[y_slot]) == 1

    def test_preserves_indices(self) -> None:
        keys = ["a", "b", "c"]
        groups = group_by_slot(keys)
        all_entries = [entry for slot_keys in groups.values() for entry in slot_keys]
        indices = sorted(idx for idx, _ in all_entries)
        assert indices == [0, 1, 2]

    def test_empty_keys(self) -> None:
        assert group_by_slot([]) == {}


class TestAssertSameSlot:
    def test_same_slot_passes(self) -> None:
        assert_same_slot(["{x}:1", "{x}:2"])

    def test_single_key_passes(self) -> None:
        assert_same_slot(["only-one"])

    def test_empty_passes(self) -> None:
        assert_same_slot([])

    def test_different_slots_raises(self) -> None:
        with pytest.raises(CacheError, match="same slot"):
            assert_same_slot(["alpha", "beta"])
