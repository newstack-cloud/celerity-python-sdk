"""Tests for DynamoDB expression builder."""

from __future__ import annotations

from celerity.resources.datastore.providers.dynamodb.expressions import (
    ExpressionResult,
    build_filter_expression,
    build_key_condition_expression,
    merge_expressions,
)
from celerity.resources.datastore.types import (
    AndGroup,
    Condition,
    ConditionOperator,
    KeyCondition,
    OrGroup,
    RangeCondition,
    RangeOperator,
)


class TestBuildKeyConditionExpression:
    def test_key_only(self) -> None:
        result = build_key_condition_expression(KeyCondition("userId", "user-1"))
        assert result.expression == "#k0 = :k0"
        assert result.attribute_names == {"#k0": "userId"}
        assert result.attribute_values == {":k0": "user-1"}

    def test_key_and_range_eq(self) -> None:
        result = build_key_condition_expression(
            KeyCondition("userId", "user-1"),
            RangeCondition("sk", RangeOperator.EQ, "profile"),
        )
        assert result.expression == "#k0 = :k0 AND #k1 = :k1"
        assert result.attribute_names == {"#k0": "userId", "#k1": "sk"}
        assert result.attribute_values == {":k0": "user-1", ":k1": "profile"}

    def test_key_and_range_lt(self) -> None:
        result = build_key_condition_expression(
            KeyCondition("pk", "x"),
            RangeCondition("sk", RangeOperator.LT, 100),
        )
        assert "#k1 < :k1" in result.expression

    def test_key_and_range_le(self) -> None:
        result = build_key_condition_expression(
            KeyCondition("pk", "x"),
            RangeCondition("sk", RangeOperator.LE, 100),
        )
        assert "#k1 <= :k1" in result.expression

    def test_key_and_range_gt(self) -> None:
        result = build_key_condition_expression(
            KeyCondition("pk", "x"),
            RangeCondition("sk", RangeOperator.GT, 50),
        )
        assert "#k1 > :k1" in result.expression

    def test_key_and_range_ge(self) -> None:
        result = build_key_condition_expression(
            KeyCondition("pk", "x"),
            RangeCondition("sk", RangeOperator.GE, "2024-01"),
        )
        assert "#k1 >= :k1" in result.expression
        assert result.attribute_values[":k1"] == "2024-01"

    def test_key_and_range_between(self) -> None:
        result = build_key_condition_expression(
            KeyCondition("userId", "user-1"),
            RangeCondition("sk", RangeOperator.BETWEEN, "a", "z"),
        )
        assert result.expression == "#k0 = :k0 AND #k1 BETWEEN :k1a AND :k1b"
        assert result.attribute_values[":k1a"] == "a"
        assert result.attribute_values[":k1b"] == "z"

    def test_key_and_range_starts_with(self) -> None:
        result = build_key_condition_expression(
            KeyCondition("userId", "user-1"),
            RangeCondition("title", RangeOperator.STARTS_WITH, "Hello"),
        )
        assert result.expression == "#k0 = :k0 AND begins_with(#k1, :k1)"
        assert result.attribute_names["#k1"] == "title"
        assert result.attribute_values[":k1"] == "Hello"


class TestBuildFilterExpression:
    """Tests for single-condition filter expressions."""

    def test_eq(self) -> None:
        result = build_filter_expression(Condition("status", ConditionOperator.EQ, "active"))
        assert result.expression == "#f0 = :f0"
        assert result.attribute_names == {"#f0": "status"}
        assert result.attribute_values == {":f0": "active"}

    def test_ne(self) -> None:
        result = build_filter_expression(Condition("status", ConditionOperator.NE, "deleted"))
        assert result.expression == "#f0 <> :f0"

    def test_lt(self) -> None:
        result = build_filter_expression(Condition("age", ConditionOperator.LT, 18))
        assert result.expression == "#f0 < :f0"
        assert result.attribute_values[":f0"] == 18

    def test_le(self) -> None:
        result = build_filter_expression(Condition("age", ConditionOperator.LE, 65))
        assert result.expression == "#f0 <= :f0"

    def test_gt(self) -> None:
        result = build_filter_expression(Condition("score", ConditionOperator.GT, 90))
        assert result.expression == "#f0 > :f0"

    def test_ge(self) -> None:
        result = build_filter_expression(Condition("score", ConditionOperator.GE, 50))
        assert result.expression == "#f0 >= :f0"

    def test_between(self) -> None:
        result = build_filter_expression(Condition("age", ConditionOperator.BETWEEN, 18, 65))
        assert result.expression == "#f0 BETWEEN :f0a AND :f0b"
        assert result.attribute_values[":f0a"] == 18
        assert result.attribute_values[":f0b"] == 65

    def test_starts_with(self) -> None:
        result = build_filter_expression(Condition("name", ConditionOperator.STARTS_WITH, "Jo"))
        assert result.expression == "begins_with(#f0, :f0)"

    def test_contains(self) -> None:
        result = build_filter_expression(Condition("tags", ConditionOperator.CONTAINS, "vip"))
        assert result.expression == "contains(#f0, :f0)"

    def test_exists(self) -> None:
        result = build_filter_expression(Condition("email", ConditionOperator.EXISTS))
        assert result.expression == "attribute_exists(#f0)"
        assert result.attribute_names == {"#f0": "email"}
        assert result.attribute_values == {}


class TestCompositeExpressions:
    """Tests for AND/OR composite condition expressions."""

    def test_list_implicit_and(self) -> None:
        result = build_filter_expression(
            [
                Condition("status", ConditionOperator.EQ, "active"),
                Condition("age", ConditionOperator.GT, 18),
            ]
        )
        assert result.expression == "#f0 = :f0 AND #f1 > :f1"
        assert result.attribute_names == {"#f0": "status", "#f1": "age"}
        assert result.attribute_values == {":f0": "active", ":f1": 18}

    def test_explicit_and_group(self) -> None:
        result = build_filter_expression(
            AndGroup(
                and_=[
                    Condition("status", ConditionOperator.EQ, "active"),
                    Condition("age", ConditionOperator.GT, 18),
                ]
            )
        )
        assert result.expression == "#f0 = :f0 AND #f1 > :f1"

    def test_or_group(self) -> None:
        result = build_filter_expression(
            OrGroup(
                or_=[
                    Condition("status", ConditionOperator.EQ, "active"),
                    Condition("status", ConditionOperator.EQ, "pending"),
                ]
            )
        )
        assert result.expression == "#f0 = :f0 OR #f1 = :f1"
        assert result.attribute_names == {"#f0": "status", "#f1": "status"}
        assert result.attribute_values == {":f0": "active", ":f1": "pending"}

    def test_nested_and_with_or(self) -> None:
        result = build_filter_expression(
            AndGroup(
                and_=[
                    Condition("age", ConditionOperator.GT, 18),
                    OrGroup(
                        or_=[
                            Condition("status", ConditionOperator.EQ, "active"),
                            Condition("status", ConditionOperator.EQ, "pending"),
                        ]
                    ),
                ]
            )
        )
        assert result.expression == "#f0 > :f0 AND (#f1 = :f1 OR #f2 = :f2)"

    def test_nested_or_with_and(self) -> None:
        result = build_filter_expression(
            OrGroup(
                or_=[
                    AndGroup(
                        and_=[
                            Condition("role", ConditionOperator.EQ, "admin"),
                            Condition("active", ConditionOperator.EQ, True),
                        ]
                    ),
                    Condition("superuser", ConditionOperator.EQ, True),
                ]
            )
        )
        assert result.expression == "(#f0 = :f0 AND #f1 = :f1) OR #f2 = :f2"

    def test_single_child_group_unwrapped(self) -> None:
        result = build_filter_expression(
            AndGroup(and_=[Condition("status", ConditionOperator.EQ, "active")])
        )
        assert result.expression == "#f0 = :f0"

    def test_single_child_or_group_unwrapped(self) -> None:
        result = build_filter_expression(OrGroup(or_=[Condition("x", ConditionOperator.EQ, 1)]))
        assert result.expression == "#f0 = :f0"

    def test_between_in_composite(self) -> None:
        result = build_filter_expression(
            [
                Condition("status", ConditionOperator.EQ, "active"),
                Condition("age", ConditionOperator.BETWEEN, 18, 65),
            ]
        )
        assert result.expression == "#f0 = :f0 AND #f1 BETWEEN :f1a AND :f1b"
        assert result.attribute_values == {":f0": "active", ":f1a": 18, ":f1b": 65}

    def test_exists_in_composite(self) -> None:
        result = build_filter_expression(
            [
                Condition("email", ConditionOperator.EXISTS),
                Condition("status", ConditionOperator.EQ, "active"),
            ]
        )
        assert result.expression == "attribute_exists(#f0) AND #f1 = :f1"
        assert result.attribute_names == {"#f0": "email", "#f1": "status"}
        assert ":f0" not in result.attribute_values

    def test_placeholder_uniqueness(self) -> None:
        result = build_filter_expression(
            [
                Condition("a", ConditionOperator.EQ, 1),
                Condition("b", ConditionOperator.EQ, 2),
                Condition("c", ConditionOperator.EQ, 3),
            ]
        )
        assert result.attribute_names == {"#f0": "a", "#f1": "b", "#f2": "c"}
        assert result.attribute_values == {":f0": 1, ":f1": 2, ":f2": 3}

    def test_deeply_nested(self) -> None:
        result = build_filter_expression(
            AndGroup(
                and_=[
                    Condition("a", ConditionOperator.EQ, 1),
                    OrGroup(
                        or_=[
                            Condition("b", ConditionOperator.EQ, 2),
                            AndGroup(
                                and_=[
                                    Condition("c", ConditionOperator.EQ, 3),
                                    Condition("d", ConditionOperator.EQ, 4),
                                ]
                            ),
                        ]
                    ),
                ]
            )
        )
        assert result.expression == "#f0 = :f0 AND (#f1 = :f1 OR (#f2 = :f2 AND #f3 = :f3))"


class TestMergeExpressions:
    def test_empty(self) -> None:
        result = merge_expressions()
        assert result.expression == ""

    def test_single(self) -> None:
        expr = ExpressionResult(
            expression="#k0 = :k0",
            attribute_names={"#k0": "userId"},
            attribute_values={":k0": "user-1"},
        )
        result = merge_expressions(expr)
        assert result is expr

    def test_two_expressions(self) -> None:
        a = ExpressionResult(
            expression="#k0 = :k0",
            attribute_names={"#k0": "userId"},
            attribute_values={":k0": "user-1"},
        )
        b = ExpressionResult(
            expression="#f0 = :f0",
            attribute_names={"#f0": "status"},
            attribute_values={":f0": "active"},
        )
        result = merge_expressions(a, b)
        assert result.expression == "#k0 = :k0 AND #f0 = :f0"
        assert result.attribute_names == {"#k0": "userId", "#f0": "status"}
        assert result.attribute_values == {":k0": "user-1", ":f0": "active"}
