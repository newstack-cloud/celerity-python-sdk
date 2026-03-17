"""Tests for celerity.di.dependency_tokens."""

from typing import Annotated

from celerity.decorators.injectable import inject, injectable
from celerity.di.dependency_tokens import get_class_dependency_tokens


class TypeA:
    pass


class TypeB:
    pass


class TestGetClassDependencyTokens:
    def test_plain_type_hints(self) -> None:
        @injectable()
        class Svc:
            def __init__(self, a: TypeA, b: TypeB) -> None:
                pass

        tokens = get_class_dependency_tokens(Svc)
        assert tokens == [TypeA, TypeB]

    def test_no_init(self) -> None:
        class Svc:
            pass

        tokens = get_class_dependency_tokens(Svc)
        assert tokens == []

    def test_object_init(self) -> None:
        class Svc:
            def __init__(self) -> None:
                pass

        tokens = get_class_dependency_tokens(Svc)
        assert tokens == []

    def test_annotated_inject_marker(self) -> None:
        @injectable()
        class Svc:
            def __init__(self, db: Annotated[TypeA, inject("DB_TOKEN")]) -> None:
                pass

        tokens = get_class_dependency_tokens(Svc)
        assert tokens == ["DB_TOKEN"]

    def test_class_level_inject_override(self) -> None:
        @injectable()
        @inject({0: "CUSTOM_TOKEN"})
        class Svc:
            def __init__(self, a: TypeA) -> None:
                pass

        tokens = get_class_dependency_tokens(Svc)
        assert tokens == ["CUSTOM_TOKEN"]

    def test_mixed_resolution(self) -> None:
        @injectable()
        @inject({1: "CACHE_TOKEN"})
        class Svc:
            def __init__(
                self,
                db: Annotated[TypeA, inject("DB_TOKEN")],
                cache: TypeB,
                logger: TypeA,
            ) -> None:
                pass

        tokens = get_class_dependency_tokens(Svc)
        assert tokens == ["DB_TOKEN", "CACHE_TOKEN", TypeA]

    def test_annotated_without_inject_uses_base_type(self) -> None:
        @injectable()
        class Svc:
            def __init__(self, a: Annotated[TypeA, "some-other-annotation"]) -> None:
                pass

        tokens = get_class_dependency_tokens(Svc)
        assert tokens == [TypeA]
