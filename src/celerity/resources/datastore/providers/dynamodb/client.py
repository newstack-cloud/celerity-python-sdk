"""DynamoDB datastore client and per-table datastore implementation."""

from __future__ import annotations

import asyncio
import logging
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any

from celerity.resources.datastore.errors import (
    ConditionalCheckFailedError,
    DatastoreError,
)
from celerity.resources.datastore.providers.dynamodb.expressions import (
    build_filter_expression,
    build_key_condition_expression,
)
from celerity.resources.datastore.providers.dynamodb.listing import (
    DynamoDBItemListing,
)
from celerity.resources.datastore.providers.dynamodb.marshall import (
    marshall_item,
    unmarshall_item,
)
from celerity.resources.datastore.types import (
    BatchGetItemsOptions,
    ConditionExpression,
    Datastore,
    DatastoreClient,
    DeleteItemOptions,
    DeleteOperation,
    GetItemOptions,
    ItemListing,
    PutItemOptions,
    PutOperation,
    QueryParams,
    ScanParams,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    import aioboto3
    from types_aiobotocore_dynamodb.client import DynamoDBClient

    from celerity.resources.datastore.providers.dynamodb.types import (
        DynamoDBDatastoreConfig,
    )

logger = logging.getLogger("celerity.datastore.dynamodb")

_MAX_BATCH_RETRIES = 5
_BATCH_BASE_DELAY = 0.05  # 50ms


class DynamoDBDatastoreClient(DatastoreClient):
    """DatastoreClient backed by aioboto3 DynamoDB."""

    def __init__(self, session: aioboto3.Session, config: DynamoDBDatastoreConfig) -> None:
        self._session = session
        self._config = config
        self._exit_stack = AsyncExitStack()
        self._client: DynamoDBClient | None = None

    async def _ensure_client(self) -> DynamoDBClient:
        """Lazily create the DynamoDB client on first use."""
        if self._client is None:
            kwargs: dict[str, Any] = {}
            if self._config.region:
                kwargs["region_name"] = self._config.region
            if self._config.endpoint_url:
                kwargs["endpoint_url"] = self._config.endpoint_url
            self._client = await self._exit_stack.enter_async_context(
                self._session.client("dynamodb", **kwargs)
            )
        return self._client

    def datastore(self, name: str, table_name: str) -> Datastore:
        """Get a datastore handle for a named resource backed by a specific table."""
        return DynamoDBDatastore(
            client_provider=self._ensure_client,
            table_name=table_name,
        )

    async def close(self) -> None:
        """Close the underlying client session."""
        await self._exit_stack.aclose()
        self._client = None


class DynamoDBDatastore(Datastore):
    """Per-table Datastore implementation using DynamoDB."""

    def __init__(
        self,
        client_provider: Callable[[], Awaitable[DynamoDBClient]],
        table_name: str,
    ) -> None:
        self._client_provider = client_provider
        self._table_name = table_name

    async def get_item(
        self,
        key: dict[str, Any],
        options: GetItemOptions | None = None,
    ) -> dict[str, Any] | None:
        client = await self._client_provider()
        params: dict[str, Any] = {
            "TableName": self._table_name,
            "Key": marshall_item(key),
        }
        if options:
            if options.consistent_read:
                params["ConsistentRead"] = True
            if options.projection:
                params["ProjectionExpression"] = ", ".join(options.projection)

        try:
            response = await client.get_item(**params)
        except Exception as exc:
            raise _wrap_error(exc, "get_item") from exc

        raw_item: dict[str, Any] | None = response.get("Item")
        return unmarshall_item(raw_item) if raw_item else None

    async def put_item(
        self,
        item: dict[str, Any],
        options: PutItemOptions | None = None,
    ) -> None:
        client = await self._client_provider()
        params: dict[str, Any] = {
            "TableName": self._table_name,
            "Item": marshall_item(item),
        }
        if options and options.condition:
            _apply_condition(params, options.condition, "ConditionExpression")

        try:
            await client.put_item(**params)
        except client.exceptions.ConditionalCheckFailedException as exc:
            raise ConditionalCheckFailedError(str(exc), cause=exc) from exc
        except Exception as exc:
            raise _wrap_error(exc, "put_item") from exc

    async def delete_item(
        self,
        key: dict[str, Any],
        options: DeleteItemOptions | None = None,
    ) -> None:
        client = await self._client_provider()
        params: dict[str, Any] = {
            "TableName": self._table_name,
            "Key": marshall_item(key),
        }
        if options and options.condition:
            _apply_condition(params, options.condition, "ConditionExpression")

        try:
            await client.delete_item(**params)
        except client.exceptions.ConditionalCheckFailedException as exc:
            raise ConditionalCheckFailedError(str(exc), cause=exc) from exc
        except Exception as exc:
            raise _wrap_error(exc, "delete_item") from exc

    def query(self, params: QueryParams) -> ItemListing:
        request_params = self._build_query_params(params)
        return DynamoDBItemListing(
            client=self,
            operation="_execute_query",
            request_params=request_params,
            limit=params.limit,
            initial_cursor=params.cursor,
        )

    def scan(self, params: ScanParams | None = None) -> ItemListing:
        request_params = self._build_scan_params(params)
        return DynamoDBItemListing(
            client=self,
            operation="_execute_scan",
            request_params=request_params,
            limit=params.limit if params else None,
            initial_cursor=params.cursor if params else None,
        )

    async def batch_get_items(
        self,
        keys: list[dict[str, Any]],
        options: BatchGetItemsOptions | None = None,
    ) -> list[dict[str, Any]]:
        client = await self._client_provider()
        marshalled_keys = [marshall_item(k) for k in keys]
        keys_and_attrs: dict[str, Any] = {"Keys": marshalled_keys}
        if options:
            if options.consistent_read:
                keys_and_attrs["ConsistentRead"] = True
            if options.projection:
                keys_and_attrs["ProjectionExpression"] = ", ".join(options.projection)

        request_items: dict[str, Any] = {self._table_name: keys_and_attrs}
        all_items: list[dict[str, Any]] = []

        try:
            for attempt in range(_MAX_BATCH_RETRIES):
                response = await client.batch_get_item(RequestItems=request_items)
                table_items = response.get("Responses", {}).get(self._table_name, [])
                all_items.extend(unmarshall_item(item) for item in table_items)

                unprocessed = response.get("UnprocessedKeys", {})
                if not unprocessed or self._table_name not in unprocessed:
                    break

                request_items = {self._table_name: unprocessed[self._table_name]}
                if attempt < _MAX_BATCH_RETRIES - 1:
                    delay = _BATCH_BASE_DELAY * (2**attempt)
                    await asyncio.sleep(delay)
            else:
                raise DatastoreError(
                    f"batch_get_item: unprocessed keys remain after {_MAX_BATCH_RETRIES} retries"
                )
        except DatastoreError:
            raise
        except Exception as exc:
            raise _wrap_error(exc, "batch_get_items") from exc

        return all_items

    async def batch_write_items(
        self,
        operations: list[PutOperation | DeleteOperation],
    ) -> None:
        client = await self._client_provider()
        write_requests: list[Any] = []
        for op in operations:
            if isinstance(op, PutOperation):
                write_requests.append({"PutRequest": {"Item": marshall_item(op.item)}})
            else:
                write_requests.append({"DeleteRequest": {"Key": marshall_item(op.key)}})

        request_items: dict[str, list[Any]] = {
            self._table_name: write_requests,
        }

        try:
            for attempt in range(_MAX_BATCH_RETRIES):
                response = await client.batch_write_item(RequestItems=request_items)
                unprocessed = response.get("UnprocessedItems", {})
                if not unprocessed or self._table_name not in unprocessed:
                    break

                request_items = {self._table_name: list(unprocessed[self._table_name])}
                if attempt < _MAX_BATCH_RETRIES - 1:
                    delay = _BATCH_BASE_DELAY * (2**attempt)
                    await asyncio.sleep(delay)
            else:
                raise DatastoreError(
                    f"batch_write_item: unprocessed items remain after {_MAX_BATCH_RETRIES} retries"
                )
        except DatastoreError:
            raise
        except Exception as exc:
            raise _wrap_error(exc, "batch_write_items") from exc

    async def _execute_query(self, **params: Any) -> dict[str, Any]:
        """Execute a DynamoDB query. Called by DynamoDBItemListing."""
        client = await self._client_provider()
        try:
            response = await client.query(**params)
            result: dict[str, Any] = dict(response)
            raw_items: list[dict[str, Any]] = result.get("Items", [])
            result["Items"] = [unmarshall_item(item) for item in raw_items]
            return result
        except Exception as exc:
            raise _wrap_error(exc, "query") from exc

    async def _execute_scan(self, **params: Any) -> dict[str, Any]:
        """Execute a DynamoDB scan. Called by DynamoDBItemListing."""
        client = await self._client_provider()
        try:
            response = await client.scan(**params)
            result: dict[str, Any] = dict(response)
            raw_items: list[dict[str, Any]] = result.get("Items", [])
            result["Items"] = [unmarshall_item(item) for item in raw_items]
            return result
        except Exception as exc:
            raise _wrap_error(exc, "scan") from exc

    def _build_query_params(self, params: QueryParams) -> dict[str, Any]:
        """Build the DynamoDB query request parameters."""
        key_expr = build_key_condition_expression(
            params.key_condition,
            params.range_condition,
        )

        request: dict[str, Any] = {
            "TableName": self._table_name,
            "KeyConditionExpression": key_expr.expression,
            "ExpressionAttributeNames": {**key_expr.attribute_names},
            "ExpressionAttributeValues": _marshall_expression_values(key_expr.attribute_values),
            "ScanIndexForward": params.scan_forward,
        }

        if params.index_name:
            request["IndexName"] = params.index_name
        if params.consistent_read:
            request["ConsistentRead"] = True
        if params.projection:
            request["ProjectionExpression"] = ", ".join(params.projection)

        if params.filter_condition:
            filter_expr = build_filter_expression(params.filter_condition)
            request["FilterExpression"] = filter_expr.expression
            request["ExpressionAttributeNames"].update(filter_expr.attribute_names)
            request["ExpressionAttributeValues"].update(
                _marshall_expression_values(filter_expr.attribute_values)
            )

        return request

    def _build_scan_params(self, params: ScanParams | None) -> dict[str, Any]:
        """Build the DynamoDB scan request parameters."""
        request: dict[str, Any] = {"TableName": self._table_name}

        if params is None:
            return request

        if params.projection:
            request["ProjectionExpression"] = ", ".join(params.projection)

        if params.filter_condition:
            filter_expr = build_filter_expression(params.filter_condition)
            request["FilterExpression"] = filter_expr.expression
            if filter_expr.attribute_names:
                request["ExpressionAttributeNames"] = filter_expr.attribute_names
            if filter_expr.attribute_values:
                request["ExpressionAttributeValues"] = _marshall_expression_values(
                    filter_expr.attribute_values
                )

        return request


def _apply_condition(
    params: dict[str, Any],
    condition: ConditionExpression,
    expression_key: str,
) -> None:
    """Build a condition expression and merge it into the request params."""
    expr = build_filter_expression(condition)
    params[expression_key] = expr.expression
    existing_names = params.get("ExpressionAttributeNames", {})
    merged_names = {**existing_names, **expr.attribute_names}
    if merged_names:
        params["ExpressionAttributeNames"] = merged_names
    existing_values = params.get("ExpressionAttributeValues", {})
    merged_values = {**existing_values, **_marshall_expression_values(expr.attribute_values)}
    if merged_values:
        params["ExpressionAttributeValues"] = merged_values


def _marshall_expression_values(values: dict[str, Any]) -> dict[str, Any]:
    """Marshall expression attribute values to DynamoDB typed format."""
    return marshall_item(values)


def _wrap_error(exc: Exception, operation: str) -> DatastoreError:
    """Wrap a provider exception in DatastoreError."""
    return DatastoreError(f"DynamoDB {operation} failed: {exc}", cause=exc)
