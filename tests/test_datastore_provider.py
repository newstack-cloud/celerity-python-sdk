"""Tests for DynamoDB datastore provider (mock DynamoDB client)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from celerity.resources.datastore.errors import (
    ConditionalCheckFailedError,
    DatastoreError,
)
from celerity.resources.datastore.providers.dynamodb.client import (
    DynamoDBDatastore,
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
    ScanParams,
)


@pytest.fixture
def mock_client() -> AsyncMock:
    """Create a mock DynamoDB low-level client."""
    client = AsyncMock()
    client.exceptions = MagicMock()
    client.exceptions.ConditionalCheckFailedException = type(
        "ConditionalCheckFailedException", (Exception,), {}
    )
    return client


@pytest.fixture
def datastore(mock_client: AsyncMock) -> DynamoDBDatastore:
    """Create a DynamoDBDatastore with a mock client provider."""

    async def provider() -> AsyncMock:
        return mock_client

    return DynamoDBDatastore(client_provider=provider, table_name="test-table")


# ---------------------------------------------------------------------------
# get_item
# ---------------------------------------------------------------------------


class TestGetItem:
    @pytest.mark.asyncio
    async def test_basic(self, datastore: DynamoDBDatastore, mock_client: AsyncMock) -> None:
        mock_client.get_item.return_value = {"Item": {"pk": {"S": "1"}, "name": {"S": "test"}}}
        result = await datastore.get_item({"pk": "1"})
        assert result == {"pk": "1", "name": "test"}
        mock_client.get_item.assert_awaited_once_with(
            TableName="test-table", Key={"pk": {"S": "1"}}
        )

    @pytest.mark.asyncio
    async def test_not_found(self, datastore: DynamoDBDatastore, mock_client: AsyncMock) -> None:
        mock_client.get_item.return_value = {}
        result = await datastore.get_item({"pk": "missing"})
        assert result is None

    @pytest.mark.asyncio
    async def test_consistent_read(
        self, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        mock_client.get_item.return_value = {"Item": {"pk": {"S": "1"}}}
        await datastore.get_item({"pk": "1"}, GetItemOptions(consistent_read=True))
        call_kwargs = mock_client.get_item.call_args.kwargs
        assert call_kwargs["ConsistentRead"] is True

    @pytest.mark.asyncio
    async def test_projection(self, datastore: DynamoDBDatastore, mock_client: AsyncMock) -> None:
        mock_client.get_item.return_value = {"Item": {"pk": {"S": "1"}}}
        await datastore.get_item({"pk": "1"}, GetItemOptions(projection=["pk", "name"]))
        call_kwargs = mock_client.get_item.call_args.kwargs
        assert call_kwargs["ProjectionExpression"] == "pk, name"


# ---------------------------------------------------------------------------
# put_item
# ---------------------------------------------------------------------------


class TestPutItem:
    @pytest.mark.asyncio
    async def test_basic(self, datastore: DynamoDBDatastore, mock_client: AsyncMock) -> None:
        await datastore.put_item({"pk": "1", "name": "test"})
        mock_client.put_item.assert_awaited_once_with(
            TableName="test-table", Item={"pk": {"S": "1"}, "name": {"S": "test"}}
        )

    @pytest.mark.asyncio
    async def test_with_condition(
        self, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        cond = Condition("pk", ConditionOperator.EXISTS)
        await datastore.put_item({"pk": "1"}, PutItemOptions(condition=cond))
        call_kwargs = mock_client.put_item.call_args.kwargs
        assert "ConditionExpression" in call_kwargs
        assert call_kwargs["ConditionExpression"] == "attribute_exists(#f0)"

    @pytest.mark.asyncio
    async def test_with_composite_condition(
        self, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        cond = AndGroup(
            and_=[
                Condition("pk", ConditionOperator.EXISTS),
                Condition("version", ConditionOperator.EQ, 1),
            ]
        )
        await datastore.put_item({"pk": "1"}, PutItemOptions(condition=cond))
        call_kwargs = mock_client.put_item.call_args.kwargs
        assert call_kwargs["ConditionExpression"] == "attribute_exists(#f0) AND #f1 = :f1"
        assert call_kwargs["ExpressionAttributeValues"][":f1"] == {"N": "1"}

    @pytest.mark.asyncio
    async def test_conditional_check_failed(
        self, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        mock_client.put_item.side_effect = mock_client.exceptions.ConditionalCheckFailedException(
            "check failed"
        )
        cond = Condition("pk", ConditionOperator.EXISTS)
        with pytest.raises(ConditionalCheckFailedError):
            await datastore.put_item({"pk": "1"}, PutItemOptions(condition=cond))


# ---------------------------------------------------------------------------
# delete_item
# ---------------------------------------------------------------------------


class TestDeleteItem:
    @pytest.mark.asyncio
    async def test_basic(self, datastore: DynamoDBDatastore, mock_client: AsyncMock) -> None:
        await datastore.delete_item({"pk": "1"})
        mock_client.delete_item.assert_awaited_once_with(
            TableName="test-table", Key={"pk": {"S": "1"}}
        )

    @pytest.mark.asyncio
    async def test_conditional_delete(
        self, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        cond = Condition("status", ConditionOperator.EQ, "inactive")
        await datastore.delete_item({"pk": "1"}, DeleteItemOptions(condition=cond))
        call_kwargs = mock_client.delete_item.call_args.kwargs
        assert "ConditionExpression" in call_kwargs
        assert call_kwargs["ExpressionAttributeValues"][":f0"] == {"S": "inactive"}

    @pytest.mark.asyncio
    async def test_conditional_check_failed(
        self, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        mock_client.delete_item.side_effect = (
            mock_client.exceptions.ConditionalCheckFailedException("check failed")
        )
        cond = Condition("pk", ConditionOperator.EXISTS)
        with pytest.raises(ConditionalCheckFailedError):
            await datastore.delete_item({"pk": "1"}, DeleteItemOptions(condition=cond))


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------


class TestQuery:
    @pytest.mark.asyncio
    async def test_builds_key_condition(
        self, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        mock_client.query.return_value = {"Items": [{"pk": {"S": "1"}}]}
        listing = datastore.query(QueryParams(key_condition=KeyCondition("pk", "user-1")))

        items = await listing.items()
        assert items == [{"pk": "1"}]
        call_kwargs = mock_client.query.call_args.kwargs
        assert call_kwargs["KeyConditionExpression"] == "#k0 = :k0"
        assert call_kwargs["ExpressionAttributeValues"][":k0"] == {"S": "user-1"}
        assert call_kwargs["TableName"] == "test-table"

    @pytest.mark.asyncio
    async def test_index_name(self, datastore: DynamoDBDatastore, mock_client: AsyncMock) -> None:
        mock_client.query.return_value = {"Items": []}
        listing = datastore.query(
            QueryParams(
                key_condition=KeyCondition("status", "active"),
                index_name="gsi-status",
            )
        )
        await listing.items()
        call_kwargs = mock_client.query.call_args.kwargs
        assert call_kwargs["IndexName"] == "gsi-status"

    @pytest.mark.asyncio
    async def test_scan_forward_false(
        self, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        mock_client.query.return_value = {"Items": []}
        listing = datastore.query(
            QueryParams(key_condition=KeyCondition("pk", "x"), scan_forward=False)
        )
        await listing.items()
        call_kwargs = mock_client.query.call_args.kwargs
        assert call_kwargs["ScanIndexForward"] is False

    @pytest.mark.asyncio
    async def test_with_filter(self, datastore: DynamoDBDatastore, mock_client: AsyncMock) -> None:
        mock_client.query.return_value = {"Items": []}
        listing = datastore.query(
            QueryParams(
                key_condition=KeyCondition("pk", "user-1"),
                filter_condition=Condition("status", ConditionOperator.EQ, "active"),
            )
        )
        await listing.items()
        call_kwargs = mock_client.query.call_args.kwargs
        assert call_kwargs["FilterExpression"] == "#f0 = :f0"
        assert call_kwargs["ExpressionAttributeValues"][":f0"] == {"S": "active"}

    @pytest.mark.asyncio
    async def test_with_composite_filter(
        self, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        mock_client.query.return_value = {"Items": []}
        listing = datastore.query(
            QueryParams(
                key_condition=KeyCondition("pk", "user-1"),
                filter_condition=OrGroup(
                    or_=[
                        Condition("status", ConditionOperator.EQ, "active"),
                        Condition("status", ConditionOperator.EQ, "pending"),
                    ]
                ),
            )
        )
        await listing.items()
        call_kwargs = mock_client.query.call_args.kwargs
        assert call_kwargs["FilterExpression"] == "#f0 = :f0 OR #f1 = :f1"

    @pytest.mark.asyncio
    async def test_consistent_read(
        self, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        mock_client.query.return_value = {"Items": []}
        listing = datastore.query(
            QueryParams(key_condition=KeyCondition("pk", "x"), consistent_read=True)
        )
        await listing.items()
        call_kwargs = mock_client.query.call_args.kwargs
        assert call_kwargs["ConsistentRead"] is True


# ---------------------------------------------------------------------------
# scan
# ---------------------------------------------------------------------------


class TestScan:
    @pytest.mark.asyncio
    async def test_basic(self, datastore: DynamoDBDatastore, mock_client: AsyncMock) -> None:
        mock_client.scan.return_value = {"Items": [{"pk": {"S": "1"}}, {"pk": {"S": "2"}}]}
        listing = datastore.scan()
        items = await listing.items()
        assert items == [{"pk": "1"}, {"pk": "2"}]
        call_kwargs = mock_client.scan.call_args.kwargs
        assert call_kwargs["TableName"] == "test-table"

    @pytest.mark.asyncio
    async def test_with_filter(self, datastore: DynamoDBDatastore, mock_client: AsyncMock) -> None:
        mock_client.scan.return_value = {"Items": []}
        listing = datastore.scan(
            ScanParams(filter_condition=Condition("status", ConditionOperator.EQ, "active"))
        )
        await listing.items()
        call_kwargs = mock_client.scan.call_args.kwargs
        assert "FilterExpression" in call_kwargs


# ---------------------------------------------------------------------------
# batch_get_items
# ---------------------------------------------------------------------------


class TestBatchGetItems:
    @pytest.mark.asyncio
    async def test_basic(self, datastore: DynamoDBDatastore, mock_client: AsyncMock) -> None:
        mock_client.batch_get_item.return_value = {
            "Responses": {
                "test-table": [
                    {"pk": {"S": "1"}},
                    {"pk": {"S": "2"}},
                ]
            },
        }
        result = await datastore.batch_get_items([{"pk": "1"}, {"pk": "2"}])
        assert result == [{"pk": "1"}, {"pk": "2"}]
        call_kwargs = mock_client.batch_get_item.call_args.kwargs
        keys = call_kwargs["RequestItems"]["test-table"]["Keys"]
        assert keys == [{"pk": {"S": "1"}}, {"pk": {"S": "2"}}]

    @pytest.mark.asyncio
    @patch(
        "celerity.resources.datastore.providers.dynamodb.client.asyncio.sleep",
        new_callable=AsyncMock,
    )
    async def test_retries_unprocessed_keys(
        self, mock_sleep: AsyncMock, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        mock_client.batch_get_item.side_effect = [
            {
                "Responses": {"test-table": [{"pk": {"S": "1"}}]},
                "UnprocessedKeys": {"test-table": {"Keys": [{"pk": {"S": "2"}}]}},
            },
            {
                "Responses": {"test-table": [{"pk": {"S": "2"}}]},
            },
        ]
        result = await datastore.batch_get_items([{"pk": "1"}, {"pk": "2"}])
        assert len(result) == 2
        assert mock_client.batch_get_item.await_count == 2

    @pytest.mark.asyncio
    async def test_consistent_read(
        self, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        mock_client.batch_get_item.return_value = {
            "Responses": {"test-table": [{"pk": {"S": "1"}}]},
        }
        await datastore.batch_get_items([{"pk": "1"}], BatchGetItemsOptions(consistent_read=True))
        call_kwargs = mock_client.batch_get_item.call_args.kwargs
        request_items = call_kwargs["RequestItems"]["test-table"]
        assert request_items["ConsistentRead"] is True


# ---------------------------------------------------------------------------
# batch_write_items
# ---------------------------------------------------------------------------


class TestBatchWriteItems:
    @pytest.mark.asyncio
    async def test_put_and_delete(
        self, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        mock_client.batch_write_item.return_value = {}
        ops = [
            PutOperation(item={"pk": "1", "name": "new"}),
            DeleteOperation(key={"pk": "2"}),
        ]
        await datastore.batch_write_items(ops)
        call_kwargs = mock_client.batch_write_item.call_args.kwargs
        requests = call_kwargs["RequestItems"]["test-table"]
        assert requests[0] == {"PutRequest": {"Item": {"pk": {"S": "1"}, "name": {"S": "new"}}}}
        assert requests[1] == {"DeleteRequest": {"Key": {"pk": {"S": "2"}}}}

    @pytest.mark.asyncio
    @patch(
        "celerity.resources.datastore.providers.dynamodb.client.asyncio.sleep",
        new_callable=AsyncMock,
    )
    async def test_retries_unprocessed_items(
        self, mock_sleep: AsyncMock, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        mock_client.batch_write_item.side_effect = [
            {
                "UnprocessedItems": {"test-table": [{"PutRequest": {"Item": {"pk": {"S": "2"}}}}]},
            },
            {},
        ]
        await datastore.batch_write_items(
            [
                PutOperation(item={"pk": "1"}),
                PutOperation(item={"pk": "2"}),
            ]
        )
        assert mock_client.batch_write_item.await_count == 2


# ---------------------------------------------------------------------------
# Error wrapping
# ---------------------------------------------------------------------------


class TestErrorWrapping:
    @pytest.mark.asyncio
    async def test_get_item_error_wrapped(
        self, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        mock_client.get_item.side_effect = RuntimeError("connection lost")
        with pytest.raises(DatastoreError, match="connection lost"):
            await datastore.get_item({"pk": "1"})

    @pytest.mark.asyncio
    async def test_put_item_error_wrapped(
        self, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        mock_client.put_item.side_effect = RuntimeError("timeout")
        with pytest.raises(DatastoreError, match="timeout"):
            await datastore.put_item({"pk": "1"})

    @pytest.mark.asyncio
    async def test_error_preserves_cause(
        self, datastore: DynamoDBDatastore, mock_client: AsyncMock
    ) -> None:
        original = RuntimeError("original")
        mock_client.get_item.side_effect = original
        with pytest.raises(DatastoreError) as exc_info:
            await datastore.get_item({"pk": "1"})
        assert exc_info.value.__cause__ is original
