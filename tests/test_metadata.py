"""Tests for celerity.metadata."""

from celerity.metadata.keys import get_metadata, set_metadata
from celerity.metadata.store import HandlerMetadataStore


class TestGetSetMetadata:
    def test_set_and_get_class_level(self) -> None:
        class Target:
            pass

        set_metadata(Target, "key1", "value1")
        assert get_metadata(Target, "key1") == "value1"

    def test_get_unset_key_returns_none(self) -> None:
        class Target:
            pass

        assert get_metadata(Target, "nonexistent") is None

    def test_get_from_target_without_metadata_returns_none(self) -> None:
        class Target:
            pass

        assert get_metadata(Target, "any") is None

    def test_method_level_metadata(self) -> None:
        class Target:
            pass

        set_metadata(Target, "http-method", "GET", prop="my_handler")
        assert get_metadata(Target, "http-method", prop="my_handler") == "GET"

    def test_method_level_does_not_leak_to_class(self) -> None:
        class Target:
            pass

        set_metadata(Target, "key", "method-value", prop="handler")
        assert get_metadata(Target, "key") is None

    def test_class_level_does_not_leak_to_method(self) -> None:
        class Target:
            pass

        set_metadata(Target, "key", "class-value")
        assert get_metadata(Target, "key", prop="handler") is None

    def test_multiple_keys_independent(self) -> None:
        class Target:
            pass

        set_metadata(Target, "key1", "a")
        set_metadata(Target, "key2", "b")
        assert get_metadata(Target, "key1") == "a"
        assert get_metadata(Target, "key2") == "b"

    def test_different_targets_independent(self) -> None:
        class A:
            pass

        class B:
            pass

        set_metadata(A, "key", "from-a")
        set_metadata(B, "key", "from-b")
        assert get_metadata(A, "key") == "from-a"
        assert get_metadata(B, "key") == "from-b"

    def test_overwrite_value(self) -> None:
        class Target:
            pass

        set_metadata(Target, "key", "old")
        set_metadata(Target, "key", "new")
        assert get_metadata(Target, "key") == "new"

    def test_metadata_on_function(self) -> None:
        def my_func() -> None:
            pass

        set_metadata(my_func, "http-method", "POST")
        assert get_metadata(my_func, "http-method") == "POST"

    def test_stores_complex_values(self) -> None:
        class Target:
            pass

        set_metadata(Target, "config", {"prefix": "/api", "version": 2})
        result = get_metadata(Target, "config")
        assert result == {"prefix": "/api", "version": 2}


class TestHandlerMetadataStore:
    def test_get_set(self) -> None:
        store = HandlerMetadataStore()
        store.set("body", {"name": "test"})
        assert store.get("body") == {"name": "test"}

    def test_get_unset_returns_none(self) -> None:
        store = HandlerMetadataStore()
        assert store.get("missing") is None

    def test_has(self) -> None:
        store = HandlerMetadataStore()
        assert store.has("key") is False
        store.set("key", "value")
        assert store.has("key") is True

    def test_initial_values(self) -> None:
        store = HandlerMetadataStore({"action": "create", "version": "v2"})
        assert store.get("action") == "create"
        assert store.get("version") == "v2"

    def test_overwrite(self) -> None:
        store = HandlerMetadataStore({"key": "old"})
        store.set("key", "new")
        assert store.get("key") == "new"
