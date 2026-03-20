"""Datastore abstract types and option dataclasses."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence


type ItemKey = dict[str, Any]
"""Partition key and optional sort key, e.g. ``{"pk": "user-1", "sk": "profile"}``."""


class RangeOperator(StrEnum):
    """Operators for sort-key range conditions in queries."""

    EQ = "eq"
    LT = "lt"
    LE = "le"
    GT = "gt"
    GE = "ge"
    BETWEEN = "between"
    STARTS_WITH = "starts_with"


class ConditionOperator(StrEnum):
    """Operators for filter conditions and conditional writes."""

    EQ = "eq"
    NE = "ne"
    LT = "lt"
    LE = "le"
    GT = "gt"
    GE = "ge"
    BETWEEN = "between"
    STARTS_WITH = "starts_with"
    CONTAINS = "contains"
    EXISTS = "exists"


@dataclass(frozen=True)
class KeyCondition:
    """Partition key equality condition (always required for query)."""

    key: str
    value: Any


@dataclass(frozen=True)
class RangeCondition:
    """Sort key condition for queries."""

    key: str
    operator: RangeOperator
    value: Any
    value2: Any | None = None  # Only for BETWEEN


@dataclass(frozen=True)
class Condition:
    """Filter condition for query/scan or conditional writes."""

    field: str
    operator: ConditionOperator
    value: Any = None  # Not used for EXISTS
    value2: Any | None = None  # Only for BETWEEN


@dataclass(frozen=True)
class AndGroup:
    """A group of conditions combined with AND logic. All conditions must be true."""

    and_: Sequence[ConditionExpression]


@dataclass(frozen=True)
class OrGroup:
    """A group of conditions combined with OR logic. At least one must be true."""

    or_: Sequence[ConditionExpression]


type ConditionExpression = Condition | list[Condition] | AndGroup | OrGroup
"""One or more conditions combined with logical operators.

A single ``Condition``, a list of Conditions (implicit AND), or explicit
``AndGroup``/``OrGroup`` for compound logic with recursive nesting.
"""


@dataclass(frozen=True)
class GetItemOptions:
    """Options for ``get_item``."""

    consistent_read: bool = False
    projection: list[str] | None = None


@dataclass(frozen=True)
class PutItemOptions:
    """Options for ``put_item``."""

    condition: ConditionExpression | None = None


@dataclass(frozen=True)
class DeleteItemOptions:
    """Options for ``delete_item``."""

    condition: ConditionExpression | None = None


@dataclass(frozen=True)
class BatchGetItemsOptions:
    """Options for ``batch_get_items``."""

    consistent_read: bool = False
    projection: list[str] | None = None


@dataclass(frozen=True)
class PutOperation:
    """A put operation for ``batch_write_items``."""

    item: dict[str, Any]


@dataclass(frozen=True)
class DeleteOperation:
    """A delete operation for ``batch_write_items``."""

    key: ItemKey


@dataclass(frozen=True)
class QueryParams:
    """Parameters for ``query``."""

    key_condition: KeyCondition
    range_condition: RangeCondition | None = None
    filter_condition: ConditionExpression | None = None
    index_name: str | None = None
    scan_forward: bool = True
    consistent_read: bool = False
    limit: int | None = None
    cursor: str | None = None
    projection: list[str] | None = None


@dataclass(frozen=True)
class ScanParams:
    """Parameters for ``scan``."""

    filter_condition: ConditionExpression | None = None
    limit: int | None = None
    cursor: str | None = None
    projection: list[str] | None = None


class ItemListing(ABC):
    """Async iterator over query/scan results with cursor-based pagination."""

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[dict[str, Any]]: ...

    @abstractmethod
    async def __anext__(self) -> dict[str, Any]: ...

    @abstractmethod
    async def items(self) -> list[dict[str, Any]]:
        """Fetch all remaining items into a list."""

    @abstractmethod
    def cursor(self) -> str | None:
        """Return the cursor for resuming pagination, or ``None`` if exhausted."""


class Datastore(ABC):
    """Per-table datastore handle with full DynamoDB-like API."""

    @abstractmethod
    async def get_item(
        self,
        key: ItemKey,
        options: GetItemOptions | None = None,
    ) -> dict[str, Any] | None: ...

    @abstractmethod
    async def put_item(
        self,
        item: dict[str, Any],
        options: PutItemOptions | None = None,
    ) -> None: ...

    @abstractmethod
    async def delete_item(
        self,
        key: ItemKey,
        options: DeleteItemOptions | None = None,
    ) -> None: ...

    @abstractmethod
    def query(
        self,
        params: QueryParams,
    ) -> ItemListing: ...

    @abstractmethod
    def scan(
        self,
        params: ScanParams | None = None,
    ) -> ItemListing: ...

    @abstractmethod
    async def batch_get_items(
        self,
        keys: list[ItemKey],
        options: BatchGetItemsOptions | None = None,
    ) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def batch_write_items(
        self,
        operations: list[PutOperation | DeleteOperation],
    ) -> None: ...


class DatastoreClient(ABC):
    """Top-level datastore client managing the provider connection.

    Resource name to provider-specific identifier mapping (e.g. DynamoDB
    table name) is resolved at construction time so ``datastore()`` takes
    only the logical resource name.
    """

    @abstractmethod
    def datastore(self, name: str) -> Datastore:
        """Get a datastore handle for a named resource.

        Args:
            name: The logical resource name. The provider maps this
                to a physical identifier internally.
        """

    @abstractmethod
    async def close(self) -> None:
        """Close the underlying client session."""
