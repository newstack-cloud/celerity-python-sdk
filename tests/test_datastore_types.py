"""Tests for datastore types, errors, and option dataclasses."""

from __future__ import annotations

import pytest

from celerity.resources.datastore.errors import (
    ConditionalCheckFailedError,
    DatastoreError,
)
from celerity.resources.datastore.types import (
    AndGroup,
    BatchGetItemsOptions,
    Condition,
    ConditionOperator,
    DeleteItemOptions,
    DeleteOperation,
    GetItemOptions,
    KeyCondition,
    OrGroup,
    PutItemOptions,
    PutOperation,
    QueryParams,
    RangeCondition,
    RangeOperator,
    ScanParams,
)


class TestDatastoreError:
    def test_message(self) -> None:
        err = DatastoreError("something broke")
        assert str(err) == "something broke"

    def test_resource_property(self) -> None:
        err = DatastoreError("fail", resource="orders")
        assert err.resource == "orders"

    def test_resource_defaults_none(self) -> None:
        err = DatastoreError("fail")
        assert err.resource is None

    def test_cause_chaining(self) -> None:
        original = ValueError("original")
        err = DatastoreError("wrapped", cause=original)
        assert err.__cause__ is original


class TestConditionalCheckFailedError:
    def test_is_datastore_error(self) -> None:
        err = ConditionalCheckFailedError("condition failed")
        assert isinstance(err, DatastoreError)

    def test_message(self) -> None:
        err = ConditionalCheckFailedError("nope")
        assert str(err) == "nope"


class TestGetItemOptions:
    def test_defaults(self) -> None:
        opts = GetItemOptions()
        assert opts.consistent_read is False
        assert opts.projection is None

    def test_with_values(self) -> None:
        opts = GetItemOptions(consistent_read=True, projection=["pk", "name"])
        assert opts.consistent_read is True
        assert opts.projection == ["pk", "name"]


class TestPutItemOptions:
    def test_defaults(self) -> None:
        opts = PutItemOptions()
        assert opts.condition is None

    def test_with_condition(self) -> None:
        cond = Condition(field="pk", operator=ConditionOperator.EXISTS)
        opts = PutItemOptions(condition=cond)
        assert opts.condition is cond


class TestDeleteItemOptions:
    def test_defaults(self) -> None:
        opts = DeleteItemOptions()
        assert opts.condition is None


class TestBatchGetItemsOptions:
    def test_defaults(self) -> None:
        opts = BatchGetItemsOptions()
        assert opts.consistent_read is False
        assert opts.projection is None


class TestPutOperation:
    def test_construction(self) -> None:
        op = PutOperation(item={"pk": "1", "name": "test"})
        assert op.item == {"pk": "1", "name": "test"}


class TestDeleteOperation:
    def test_construction(self) -> None:
        op = DeleteOperation(key={"pk": "1"})
        assert op.key == {"pk": "1"}


class TestKeyCondition:
    def test_construction(self) -> None:
        kc = KeyCondition(key="userId", value="user-1")
        assert kc.key == "userId"
        assert kc.value == "user-1"


class TestRangeCondition:
    def test_each_operator(self) -> None:
        for op in RangeOperator:
            rc = RangeCondition(key="sk", operator=op, value="a")
            assert rc.operator == op

    def test_between(self) -> None:
        rc = RangeCondition(key="sk", operator=RangeOperator.BETWEEN, value="a", value2="z")
        assert rc.value2 == "z"

    def test_value2_defaults_none(self) -> None:
        rc = RangeCondition(key="sk", operator=RangeOperator.EQ, value="x")
        assert rc.value2 is None


class TestCondition:
    def test_each_operator(self) -> None:
        for op in ConditionOperator:
            c = Condition(field="f", operator=op)
            assert c.operator == op

    def test_between(self) -> None:
        c = Condition(field="age", operator=ConditionOperator.BETWEEN, value=18, value2=65)
        assert c.value2 == 65

    def test_exists_no_value(self) -> None:
        c = Condition(field="email", operator=ConditionOperator.EXISTS)
        assert c.value is None


class TestAndGroup:
    def test_construction(self) -> None:
        conds = [
            Condition(field="a", operator=ConditionOperator.EQ, value=1),
            Condition(field="b", operator=ConditionOperator.EQ, value=2),
        ]
        group = AndGroup(and_=conds)
        assert len(group.and_) == 2

    def test_frozen(self) -> None:
        group = AndGroup(and_=[Condition(field="a", operator=ConditionOperator.EQ, value=1)])
        with pytest.raises(AttributeError):
            group.and_ = []  # type: ignore[misc]


class TestOrGroup:
    def test_construction(self) -> None:
        conds = [
            Condition(field="status", operator=ConditionOperator.EQ, value="a"),
            Condition(field="status", operator=ConditionOperator.EQ, value="b"),
        ]
        group = OrGroup(or_=conds)
        assert len(group.or_) == 2

    def test_frozen(self) -> None:
        group = OrGroup(or_=[Condition(field="x", operator=ConditionOperator.EQ, value=1)])
        with pytest.raises(AttributeError):
            group.or_ = []  # type: ignore[misc]

    def test_nested_groups(self) -> None:
        nested = AndGroup(
            and_=[
                Condition(field="a", operator=ConditionOperator.EQ, value=1),
                OrGroup(
                    or_=[
                        Condition(field="b", operator=ConditionOperator.EQ, value=2),
                        Condition(field="c", operator=ConditionOperator.EQ, value=3),
                    ]
                ),
            ]
        )
        assert isinstance(nested.and_[1], OrGroup)


class TestQueryParams:
    def test_minimal(self) -> None:
        qp = QueryParams(key_condition=KeyCondition("pk", "user-1"))
        assert qp.key_condition.key == "pk"
        assert qp.range_condition is None
        assert qp.filter_condition is None
        assert qp.index_name is None
        assert qp.scan_forward is True
        assert qp.consistent_read is False
        assert qp.limit is None
        assert qp.cursor is None
        assert qp.projection is None

    def test_full(self) -> None:
        qp = QueryParams(
            key_condition=KeyCondition("pk", "user-1"),
            range_condition=RangeCondition("sk", RangeOperator.GE, "2024"),
            filter_condition=Condition("status", ConditionOperator.EQ, "active"),
            index_name="gsi-status",
            scan_forward=False,
            consistent_read=True,
            limit=10,
            cursor="abc123",
            projection=["pk", "sk", "status"],
        )
        assert qp.index_name == "gsi-status"
        assert qp.scan_forward is False
        assert qp.limit == 10


class TestScanParams:
    def test_defaults(self) -> None:
        sp = ScanParams()
        assert sp.filter_condition is None
        assert sp.limit is None
        assert sp.cursor is None
        assert sp.projection is None

    def test_with_filter(self) -> None:
        sp = ScanParams(
            filter_condition=Condition("status", ConditionOperator.EQ, "active"),
            limit=50,
        )
        assert sp.limit == 50
