"""Tests for bucket helper functions."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from celerity.resources.bucket.params import (
    DEFAULT_BUCKET_TOKEN,
    bucket_token,
    get_bucket,
)


class TestGetBucket:
    @pytest.mark.asyncio
    async def test_default(self) -> None:
        mock_bucket = AsyncMock()
        container = AsyncMock()
        container.resolve = AsyncMock(return_value=mock_bucket)

        result = await get_bucket(container)
        assert result is mock_bucket
        container.resolve.assert_awaited_once_with(DEFAULT_BUCKET_TOKEN)

    @pytest.mark.asyncio
    async def test_named(self) -> None:
        mock_bucket = AsyncMock()
        container = AsyncMock()
        container.resolve = AsyncMock(return_value=mock_bucket)

        result = await get_bucket(container, "images")
        assert result is mock_bucket
        container.resolve.assert_awaited_once_with(bucket_token("images"))

    @pytest.mark.asyncio
    async def test_raises_when_not_registered(self) -> None:
        container = AsyncMock()
        container.resolve = AsyncMock(side_effect=KeyError("not found"))

        with pytest.raises(KeyError):
            await get_bucket(container)
