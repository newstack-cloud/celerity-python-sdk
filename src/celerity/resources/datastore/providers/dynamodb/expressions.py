"""DynamoDB expression builder.

Converts condition dataclasses to DynamoDB expression syntax with
placeholder attribute names and values, avoiding reserved word conflicts.

Key conditions use ``#k``/``:k`` prefixed placeholders.
Filter conditions use ``#f``/``:f`` prefixed placeholders.
This prevents collisions when key and filter expressions are merged
into a single DynamoDB command.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

from celerity.resources.datastore.types import (
    AndGroup,
    Condition,
    ConditionExpression,
    ConditionOperator,
    KeyCondition,
    OrGroup,
    RangeCondition,
    RangeOperator,
)


@dataclass
class ExpressionResult:
    """Result of building a DynamoDB expression."""

    expression: str = ""
    attribute_names: dict[str, str] = field(default_factory=dict)
    attribute_values: dict[str, Any] = field(default_factory=dict)


_RANGE_OP_MAP: dict[RangeOperator, str] = {
    RangeOperator.EQ: "=",
    RangeOperator.LT: "<",
    RangeOperator.LE: "<=",
    RangeOperator.GT: ">",
    RangeOperator.GE: ">=",
}

_CONDITION_OP_MAP: dict[ConditionOperator, str] = {
    ConditionOperator.EQ: "=",
    ConditionOperator.NE: "<>",
    ConditionOperator.LT: "<",
    ConditionOperator.LE: "<=",
    ConditionOperator.GT: ">",
    ConditionOperator.GE: ">=",
}


def build_key_condition_expression(
    key_condition: KeyCondition,
    range_condition: RangeCondition | None = None,
) -> ExpressionResult:
    """Build a KeyConditionExpression from key and optional range conditions.

    Uses ``#k0``/``:k0`` indexed placeholders for the partition key and
    ``#k1``/``:k1`` for the sort key to avoid reserved word conflicts.
    """
    counter = 0
    pk_name = f"#k{counter}"
    pk_value = f":k{counter}"
    names: dict[str, str] = {pk_name: key_condition.key}
    values: dict[str, Any] = {pk_value: key_condition.value}
    expression = f"{pk_name} = {pk_value}"
    counter += 1

    if range_condition is not None:
        sk_name = f"#k{counter}"
        names[sk_name] = range_condition.key

        op = range_condition.operator
        if op == RangeOperator.BETWEEN:
            lo_val = f":k{counter}a"
            hi_val = f":k{counter}b"
            values[lo_val] = range_condition.value
            values[hi_val] = range_condition.value2
            expression += f" AND {sk_name} BETWEEN {lo_val} AND {hi_val}"
        elif op == RangeOperator.STARTS_WITH:
            sk_value = f":k{counter}"
            values[sk_value] = range_condition.value
            expression += f" AND begins_with({sk_name}, {sk_value})"
        else:
            sk_value = f":k{counter}"
            values[sk_value] = range_condition.value
            dynamo_op = _RANGE_OP_MAP[op]
            expression += f" AND {sk_name} {dynamo_op} {sk_value}"

    return ExpressionResult(
        expression=expression,
        attribute_names=names,
        attribute_values=values,
    )


def build_filter_expression(
    conditions: ConditionExpression,
) -> ExpressionResult:
    """Build a FilterExpression (or ConditionExpression for writes).

    Supports single conditions, lists of conditions (implicit AND),
    and explicit ``AndGroup``/``OrGroup`` with recursive nesting.
    Uses ``#f``/``:f`` prefixed placeholders with an incrementing counter
    for unique names across arbitrarily deep expression trees.
    """
    names: dict[str, str] = {}
    values: dict[str, Any] = {}
    counter = [0]

    expression = _build_expression_node(conditions, names, values, counter, depth=0)
    return ExpressionResult(
        expression=expression,
        attribute_names=names,
        attribute_values=values,
    )


def merge_expressions(*results: ExpressionResult) -> ExpressionResult:
    """Merge multiple ExpressionResults with AND, combining name/value maps."""
    if not results:
        return ExpressionResult()
    if len(results) == 1:
        return results[0]

    expressions: list[str] = []
    names: dict[str, str] = {}
    values: dict[str, Any] = {}

    for result in results:
        if result.expression:
            expressions.append(result.expression)
        names.update(result.attribute_names)
        values.update(result.attribute_values)

    return ExpressionResult(
        expression=" AND ".join(expressions),
        attribute_names=names,
        attribute_values=values,
    )


def _build_expression_node(
    expr: ConditionExpression,
    names: dict[str, str],
    values: dict[str, Any],
    counter: list[int],
    depth: int,
) -> str:
    """Recursively build an expression string from a condition expression tree."""
    if isinstance(expr, list):
        return _build_group(expr, "AND", names, values, counter, depth)
    if isinstance(expr, OrGroup):
        return _build_group(expr.or_, "OR", names, values, counter, depth)
    if isinstance(expr, AndGroup):
        return _build_group(expr.and_, "AND", names, values, counter, depth)
    return _build_single_condition(expr, names, values, counter)


def _build_group(
    children: Sequence[ConditionExpression],
    operator: str,
    names: dict[str, str],
    values: dict[str, Any],
    counter: list[int],
    depth: int,
) -> str:
    """Build an AND/OR group with parenthesization rules.

    - Root-level groups (depth 0): no parentheses
    - Nested groups with multiple children: wrapped in ``(...)``
    - Single-child groups: unwrapped (simplified)
    """
    parts = [_build_expression_node(child, names, values, counter, depth + 1) for child in children]

    if len(parts) == 1:
        return parts[0]

    joined = f" {operator} ".join(parts)
    return f"({joined})" if depth > 0 else joined


def _build_single_condition(
    cond: Condition,
    names: dict[str, str],
    values: dict[str, Any],
    counter: list[int],
) -> str:
    """Build a single condition expression with indexed placeholders."""
    i = counter[0]
    attr_name = f"#f{i}"
    names[attr_name] = cond.field
    counter[0] += 1

    op = cond.operator

    if op == ConditionOperator.EXISTS:
        return f"attribute_exists({attr_name})"

    if op == ConditionOperator.STARTS_WITH:
        val_key = f":f{i}"
        values[val_key] = cond.value
        return f"begins_with({attr_name}, {val_key})"

    if op == ConditionOperator.CONTAINS:
        val_key = f":f{i}"
        values[val_key] = cond.value
        return f"contains({attr_name}, {val_key})"

    if op == ConditionOperator.BETWEEN:
        lo_key = f":f{i}a"
        hi_key = f":f{i}b"
        values[lo_key] = cond.value
        values[hi_key] = cond.value2
        return f"{attr_name} BETWEEN {lo_key} AND {hi_key}"

    dynamo_op = _CONDITION_OP_MAP[op]
    val_key = f":f{i}"
    values[val_key] = cond.value
    return f"{attr_name} {dynamo_op} {val_key}"
