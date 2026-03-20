"""Bucket abstract types and option dataclasses."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from datetime import datetime


@dataclass(frozen=True)
class ObjectInfo:
    """Metadata about a stored object."""

    key: str
    size: int
    content_type: str
    last_modified: datetime
    etag: str
    metadata: dict[str, str]


@dataclass
class GetObjectResult:
    """Result of a get operation with streaming body."""

    body: AsyncIterator[bytes]
    info: ObjectInfo

    async def read(self) -> bytes:
        """Consume the entire body into bytes."""
        chunks: list[bytes] = []
        async for chunk in self.body:
            chunks.append(chunk)
        return b"".join(chunks)

    async def read_text(self, encoding: str = "utf-8") -> str:
        """Consume the entire body into a string."""
        return (await self.read()).decode(encoding)


@dataclass(frozen=True)
class GetObjectOptions:
    """Options for get operations."""

    range_start: int | None = None
    range_end: int | None = None


@dataclass(frozen=True)
class PutObjectOptions:
    """Options for put operations."""

    content_type: str | None = None
    metadata: dict[str, str] | None = None


@dataclass(frozen=True)
class ListObjectsOptions:
    """Options for list operations."""

    prefix: str | None = None
    max_keys: int | None = None
    cursor: str | None = None


@dataclass(frozen=True)
class CopyObjectOptions:
    """Options for copy operations."""

    dest_bucket: str | None = None
    metadata: dict[str, str] | None = None
    content_type: str | None = None


@dataclass(frozen=True)
class SignUrlOptions:
    """Options for presigned URL generation."""

    expires_in: int = 3600
    method: str = "get"


class ObjectListing(ABC):
    """Async iterator over object listing results with cursor-based pagination."""

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[ObjectInfo]: ...

    @abstractmethod
    async def __anext__(self) -> ObjectInfo: ...

    @property
    @abstractmethod
    def cursor(self) -> str | None:
        """The continuation token for resuming pagination."""


class Bucket(ABC):
    """Per-bucket handle with full object storage API."""

    @abstractmethod
    async def get(
        self,
        key: str,
        options: GetObjectOptions | None = None,
    ) -> GetObjectResult | None:
        """Get an object by key. Returns None if not found."""

    @abstractmethod
    async def put(
        self,
        key: str,
        body: str | bytes,
        options: PutObjectOptions | None = None,
    ) -> None:
        """Put an object. String bodies are encoded to UTF-8."""

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete an object by key. No error if the key does not exist."""

    @abstractmethod
    async def info(self, key: str) -> ObjectInfo | None:
        """Get object metadata (head). Returns None if not found."""

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Check if an object exists."""

    @abstractmethod
    def list(
        self,
        options: ListObjectsOptions | None = None,
    ) -> ObjectListing:
        """List objects with cursor-based pagination."""

    @abstractmethod
    async def copy(
        self,
        source_key: str,
        dest_key: str,
        options: CopyObjectOptions | None = None,
    ) -> None:
        """Copy an object within the same bucket or across buckets."""

    @abstractmethod
    async def sign_url(
        self,
        key: str,
        options: SignUrlOptions | None = None,
    ) -> str:
        """Generate a presigned URL for reading or writing an object."""


class ObjectStorage(ABC):
    """Top-level object storage client managing the S3 session."""

    @abstractmethod
    def bucket(self, name: str) -> Bucket:
        """Get a bucket handle for the given bucket name."""

    @abstractmethod
    async def close(self) -> None:
        """Close the underlying S3 session."""
