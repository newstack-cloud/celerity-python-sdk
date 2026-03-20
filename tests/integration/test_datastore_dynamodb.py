"""Integration tests for DynamoDB datastore against LocalStack."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import aioboto3
import pytest

from celerity.resources.datastore.errors import ConditionalCheckFailedError
from celerity.resources.datastore.providers.dynamodb.client import (
    DynamoDBDatastoreClient,
)
from celerity.resources.datastore.providers.dynamodb.types import (
    DynamoDBDatastoreConfig,
)
from celerity.resources.datastore.types import (
    Condition,
    ConditionOperator,
    DeleteOperation,
    GetItemOptions,
    KeyCondition,
    PutItemOptions,
    PutOperation,
    QueryParams,
    RangeCondition,
    RangeOperator,
    ScanParams,
)

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from collections.abc import AsyncGenerator, Generator

    from celerity.resources.datastore.types import Datastore

TABLE_NAME = "celerity-test-datastore"
ENDPOINT_URL = "http://localhost:4566"
REGION = "us-east-1"


@pytest.fixture(scope="module")
def event_loop() -> Generator[AbstractEventLoop]:
    """Module-scoped event loop for async fixtures."""
    import asyncio

    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
async def dynamodb_table() -> AsyncGenerator[str]:
    """Create the test table with a GSI, seed data, and clean up after."""
    session = aioboto3.Session()
    async with session.client(
        "dynamodb",
        region_name=REGION,
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    ) as client:
        # Create table
        try:  # noqa: SIM105
            await client.create_table(
                TableName=TABLE_NAME,
                KeySchema=[
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                    {"AttributeName": "status", "AttributeType": "S"},
                    {"AttributeName": "createdAt", "AttributeType": "S"},
                ],
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": "gsi-status",
                        "KeySchema": [
                            {"AttributeName": "status", "KeyType": "HASH"},
                            {"AttributeName": "createdAt", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    },
                ],
                BillingMode="PAY_PER_REQUEST",
            )
        except client.exceptions.ResourceInUseException:
            pass  # Table already exists

        # Seed data: 20 items for user-1, 5 for user-2
        for i in range(20):
            await client.put_item(
                TableName=TABLE_NAME,
                Item={
                    "pk": {"S": "user-1"},
                    "sk": {"S": f"item-{i:02d}"},
                    "status": {"S": "active" if i % 2 == 0 else "inactive"},
                    "createdAt": {"S": f"2024-01-{i + 1:02d}"},
                    "data": {"S": f"value-{i}"},
                },
            )
        for i in range(5):
            await client.put_item(
                TableName=TABLE_NAME,
                Item={
                    "pk": {"S": "user-2"},
                    "sk": {"S": f"item-{i:02d}"},
                    "status": {"S": "active"},
                    "createdAt": {"S": f"2024-02-{i + 1:02d}"},
                    "data": {"S": f"u2-value-{i}"},
                },
            )

        yield TABLE_NAME

        # Teardown
        await client.delete_table(TableName=TABLE_NAME)


@pytest.fixture
async def datastore(dynamodb_table: str) -> AsyncGenerator[Datastore]:
    """Create a DynamoDBDatastore for the test table."""
    config = DynamoDBDatastoreConfig(
        region=REGION,
        endpoint_url=ENDPOINT_URL,
    )
    session = aioboto3.Session()
    client = DynamoDBDatastoreClient(session=session, config=config)
    ds = client.datastore("test", table_name=dynamodb_table)
    yield ds
    await client.close()


class TestGetItem:
    @pytest.mark.asyncio
    async def test_get_existing(self, datastore: Datastore) -> None:
        item = await datastore.get_item({"pk": "user-1", "sk": "item-00"})
        assert item is not None
        assert item["pk"] == "user-1"
        assert item["sk"] == "item-00"
        assert "data" in item

    @pytest.mark.asyncio
    async def test_get_not_found(self, datastore: Datastore) -> None:
        item = await datastore.get_item({"pk": "user-999", "sk": "nope"})
        assert item is None

    @pytest.mark.asyncio
    async def test_consistent_read(self, datastore: Datastore) -> None:
        item = await datastore.get_item(
            {"pk": "user-1", "sk": "item-00"},
            GetItemOptions(consistent_read=True),
        )
        assert item is not None


class TestPutItem:
    @pytest.mark.asyncio
    async def test_upsert(self, datastore: Datastore) -> None:
        new_item = {"pk": "user-put", "sk": "new-item", "data": "hello"}
        await datastore.put_item(new_item)
        result = await datastore.get_item({"pk": "user-put", "sk": "new-item"})
        assert result is not None
        assert result["data"] == "hello"

        # Cleanup
        await datastore.delete_item({"pk": "user-put", "sk": "new-item"})

    @pytest.mark.asyncio
    async def test_conditional_put_fails(self, datastore: Datastore) -> None:
        # item-00 already exists, condition requires field "nonexistent" to exist
        cond = Condition("nonexistent", ConditionOperator.EXISTS)
        with pytest.raises(ConditionalCheckFailedError):
            await datastore.put_item(
                {"pk": "user-1", "sk": "item-00", "data": "x"},
                PutItemOptions(condition=cond),
            )


class TestDeleteItem:
    @pytest.mark.asyncio
    async def test_delete_and_verify(self, datastore: Datastore) -> None:
        # Put a temporary item
        await datastore.put_item({"pk": "user-del", "sk": "temp", "data": "bye"})

        await datastore.delete_item({"pk": "user-del", "sk": "temp"})
        result = await datastore.get_item({"pk": "user-del", "sk": "temp"})
        assert result is None


class TestQuery:
    @pytest.mark.asyncio
    async def test_key_only(self, datastore: Datastore) -> None:
        listing = datastore.query(QueryParams(key_condition=KeyCondition("pk", "user-1")))
        items = await listing.items()
        assert len(items) == 20

    @pytest.mark.asyncio
    async def test_range_condition(self, datastore: Datastore) -> None:
        listing = datastore.query(
            QueryParams(
                key_condition=KeyCondition("pk", "user-1"),
                range_condition=RangeCondition("sk", RangeOperator.GE, "item-10"),
            )
        )
        items = await listing.items()
        assert len(items) == 10  # item-10 through item-19

    @pytest.mark.asyncio
    async def test_filter_condition(self, datastore: Datastore) -> None:
        listing = datastore.query(
            QueryParams(
                key_condition=KeyCondition("pk", "user-1"),
                filter_condition=Condition("status", ConditionOperator.EQ, "active"),
            )
        )
        items = await listing.items()
        assert len(items) == 10  # Even-numbered items are active

    @pytest.mark.asyncio
    async def test_descending(self, datastore: Datastore) -> None:
        listing = datastore.query(
            QueryParams(
                key_condition=KeyCondition("pk", "user-1"),
                scan_forward=False,
            )
        )
        items = await listing.items()
        assert items[0]["sk"] == "item-19"
        assert items[-1]["sk"] == "item-00"

    @pytest.mark.asyncio
    async def test_pagination(self, datastore: Datastore) -> None:
        listing = datastore.query(
            QueryParams(
                key_condition=KeyCondition("pk", "user-1"),
                limit=5,
            )
        )
        all_items = await listing.items()
        assert len(all_items) == 20  # All items fetched across pages

    @pytest.mark.asyncio
    async def test_cursor_resume(self, datastore: Datastore) -> None:
        # First page
        listing1 = datastore.query(
            QueryParams(
                key_condition=KeyCondition("pk", "user-1"),
                limit=5,
            )
        )
        first_batch: list[dict[str, Any]] = []
        count = 0
        async for item in listing1:
            first_batch.append(item)
            count += 1
            if count >= 5:
                break

        cursor = listing1.cursor()
        assert cursor is not None

        # Resume from cursor
        listing2 = datastore.query(
            QueryParams(
                key_condition=KeyCondition("pk", "user-1"),
                limit=5,
                cursor=cursor,
            )
        )
        second_batch = await listing2.items()
        assert len(second_batch) > 0

        # No duplicates
        first_sks = {item["sk"] for item in first_batch}
        second_sks = {item["sk"] for item in second_batch}
        assert first_sks.isdisjoint(second_sks)

    @pytest.mark.asyncio
    async def test_gsi(self, datastore: Datastore) -> None:
        listing = datastore.query(
            QueryParams(
                key_condition=KeyCondition("status", "active"),
                index_name="gsi-status",
            )
        )
        items = await listing.items()
        # 10 active user-1 items + 5 active user-2 items = 15
        assert len(items) == 15

    @pytest.mark.asyncio
    async def test_consistent_read(self, datastore: Datastore) -> None:
        listing = datastore.query(
            QueryParams(
                key_condition=KeyCondition("pk", "user-1"),
                consistent_read=True,
                limit=1,
            )
        )
        items = await listing.items()
        assert len(items) >= 1


class TestScan:
    @pytest.mark.asyncio
    async def test_all_items(self, datastore: Datastore) -> None:
        listing = datastore.scan()
        items = await listing.items()
        assert len(items) == 25  # 20 + 5

    @pytest.mark.asyncio
    async def test_with_filter(self, datastore: Datastore) -> None:
        listing = datastore.scan(
            ScanParams(filter_condition=Condition("status", ConditionOperator.EQ, "inactive"))
        )
        items = await listing.items()
        assert len(items) == 10  # Only user-1 odd-numbered items

    @pytest.mark.asyncio
    async def test_pagination(self, datastore: Datastore) -> None:
        listing = datastore.scan(ScanParams(limit=10))
        items = await listing.items()
        assert len(items) == 25  # All items fetched across pages


class TestBatchGetItems:
    @pytest.mark.asyncio
    async def test_batch_get(self, datastore: Datastore) -> None:
        keys = [{"pk": "user-1", "sk": f"item-{i:02d}"} for i in range(5)]
        items = await datastore.batch_get_items(keys)
        assert len(items) == 5


class TestBatchWriteItems:
    @pytest.mark.asyncio
    async def test_put_and_delete(self, datastore: Datastore) -> None:
        # Batch put 3 items
        ops: list[PutOperation | DeleteOperation] = [
            PutOperation(item={"pk": "user-batch", "sk": f"bw-{i}", "data": f"batch-{i}"})
            for i in range(3)
        ]
        await datastore.batch_write_items(ops)

        # Verify they exist
        for i in range(3):
            item = await datastore.get_item({"pk": "user-batch", "sk": f"bw-{i}"})
            assert item is not None

        # Batch delete them
        delete_ops: list[PutOperation | DeleteOperation] = [
            DeleteOperation(key={"pk": "user-batch", "sk": f"bw-{i}"}) for i in range(3)
        ]
        await datastore.batch_write_items(delete_ops)

        # Verify deleted
        for i in range(3):
            item = await datastore.get_item({"pk": "user-batch", "sk": f"bw-{i}"})
            assert item is None
