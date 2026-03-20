"""S3 object storage client and per-bucket implementation."""

from __future__ import annotations

import logging
from contextlib import AsyncExitStack
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from celerity.resources.bucket.errors import BucketError
from celerity.resources.bucket.providers.s3.listing import S3ObjectListing
from celerity.resources.bucket.types import (
    Bucket,
    CopyObjectOptions,
    GetObjectOptions,
    GetObjectResult,
    ListObjectsOptions,
    ObjectInfo,
    ObjectListing,
    ObjectStorage,
    PutObjectOptions,
    SignUrlOptions,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    import aioboto3
    from types_aiobotocore_s3.client import S3Client

    from celerity.resources.bucket.providers.s3.types import S3ObjectStorageConfig
    from celerity.types.telemetry import CelerityTracer

logger = logging.getLogger("celerity.bucket.s3")


class S3ObjectStorage(ObjectStorage):
    """ObjectStorage backed by S3 via aioboto3."""

    def __init__(
        self,
        session: aioboto3.Session,
        config: S3ObjectStorageConfig,
        tracer: CelerityTracer | None = None,
        resource_ids: dict[str, str] | None = None,
    ) -> None:
        self._session = session
        self._config = config
        self._tracer = tracer
        self._resource_ids = resource_ids or {}
        self._exit_stack = AsyncExitStack()
        self._client: S3Client | None = None

    async def _ensure_client(self) -> S3Client:
        """Lazily create the S3 client on first use."""
        if self._client is None:
            kwargs: dict[str, Any] = {}
            if self._config.endpoint_url:
                kwargs["endpoint_url"] = self._config.endpoint_url
            if self._config.region:
                kwargs["region_name"] = self._config.region
            if self._config.force_path_style:
                from botocore.config import Config as BotoConfig

                kwargs["config"] = BotoConfig(s3={"addressing_style": "path"})
            self._client = await self._exit_stack.enter_async_context(
                self._session.client("s3", **kwargs)
            )
            logger.debug(
                "created S3 client region=%s endpoint=%s",
                self._config.region,
                self._config.endpoint_url,
            )
        return self._client

    def bucket(self, name: str) -> Bucket:
        bucket_name = self._resource_ids.get(name, name)
        return S3Bucket(
            client_provider=self._ensure_client,
            bucket_name=bucket_name,
            tracer=self._tracer,
        )

    async def close(self) -> None:
        """Close the underlying client session."""
        await self._exit_stack.aclose()
        self._client = None


class S3Bucket(Bucket):
    """Per-bucket S3 implementation."""

    def __init__(
        self,
        client_provider: Callable[[], Awaitable[S3Client]],
        bucket_name: str,
        tracer: CelerityTracer | None = None,
    ) -> None:
        self._client_provider = client_provider
        self._bucket_name = bucket_name
        self._tracer = tracer

    async def _traced(
        self,
        name: str,
        fn: Callable[..., Awaitable[Any]],
        attributes: dict[str, Any] | None = None,
    ) -> Any:
        """Execute *fn* within a tracer span if a tracer is available."""
        if not self._tracer:
            return await fn()
        return await self._tracer.with_span(name, lambda _span: fn(), attributes=attributes)

    async def get(
        self,
        key: str,
        options: GetObjectOptions | None = None,
    ) -> GetObjectResult | None:
        result: GetObjectResult | None = await self._traced(
            "celerity.bucket.get",
            lambda: self._get(key, options),
            attributes={"bucket.name": self._bucket_name, "bucket.key": key},
        )
        return result

    async def put(
        self,
        key: str,
        body: str | bytes,
        options: PutObjectOptions | None = None,
    ) -> None:
        await self._traced(
            "celerity.bucket.put",
            lambda: self._put(key, body, options),
            attributes={"bucket.name": self._bucket_name, "bucket.key": key},
        )

    async def delete(self, key: str) -> None:
        await self._traced(
            "celerity.bucket.delete",
            lambda: self._delete(key),
            attributes={"bucket.name": self._bucket_name, "bucket.key": key},
        )

    async def info(self, key: str) -> ObjectInfo | None:
        result: ObjectInfo | None = await self._traced(
            "celerity.bucket.info",
            lambda: self._info(key),
            attributes={"bucket.name": self._bucket_name, "bucket.key": key},
        )
        return result

    async def exists(self, key: str) -> bool:
        return await self.info(key) is not None

    def list(
        self,
        options: ListObjectsOptions | None = None,
    ) -> ObjectListing:
        return S3ObjectListing(
            client_provider=self._client_provider,
            bucket_name=self._bucket_name,
            prefix=options.prefix if options else None,
            max_keys=options.max_keys if options else None,
            cursor=options.cursor if options else None,
            tracer=self._tracer,
        )

    async def copy(
        self,
        source_key: str,
        dest_key: str,
        options: CopyObjectOptions | None = None,
    ) -> None:
        await self._traced(
            "celerity.bucket.copy",
            lambda: self._copy(source_key, dest_key, options),
            attributes={
                "bucket.name": self._bucket_name,
                "bucket.key": source_key,
                "bucket.dest_key": dest_key,
                "bucket.dest_bucket": (
                    options.dest_bucket if options and options.dest_bucket else self._bucket_name
                ),
            },
        )

    async def sign_url(
        self,
        key: str,
        options: SignUrlOptions | None = None,
    ) -> str:
        result: str = await self._traced(
            "celerity.bucket.sign_url",
            lambda: self._sign_url(key, options),
            attributes={"bucket.name": self._bucket_name, "bucket.key": key},
        )
        return result

    async def _get(
        self,
        key: str,
        options: GetObjectOptions | None,
    ) -> GetObjectResult | None:
        logger.debug("get %s %s", self._bucket_name, key)
        client = await self._client_provider()
        params: dict[str, Any] = {"Bucket": self._bucket_name, "Key": key}
        if options:
            range_header = _build_range_header(options.range_start, options.range_end)
            if range_header:
                params["Range"] = range_header

        try:
            response = await client.get_object(**params)
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise BucketError(f"S3 get_object failed: {exc}", cause=exc) from exc

        body_stream = response["Body"]
        info = _parse_head_response(key, response)
        return GetObjectResult(
            body=_wrap_stream(body_stream),
            info=info,
        )

    async def _put(
        self,
        key: str,
        body: str | bytes,
        options: PutObjectOptions | None,
    ) -> None:
        logger.debug("put %s %s", self._bucket_name, key)
        client = await self._client_provider()
        raw_body = body.encode("utf-8") if isinstance(body, str) else body
        params: dict[str, Any] = {
            "Bucket": self._bucket_name,
            "Key": key,
            "Body": raw_body,
        }
        if options:
            if options.content_type:
                params["ContentType"] = options.content_type
            if options.metadata:
                params["Metadata"] = options.metadata

        try:
            await client.put_object(**params)
        except Exception as exc:
            raise BucketError(f"S3 put_object failed: {exc}", cause=exc) from exc

    async def _delete(self, key: str) -> None:
        logger.debug("delete %s %s", self._bucket_name, key)
        client = await self._client_provider()
        try:
            await client.delete_object(Bucket=self._bucket_name, Key=key)
        except Exception as exc:
            raise BucketError(f"S3 delete_object failed: {exc}", cause=exc) from exc

    async def _info(self, key: str) -> ObjectInfo | None:
        logger.debug("info %s %s", self._bucket_name, key)
        client = await self._client_provider()
        try:
            response = await client.head_object(Bucket=self._bucket_name, Key=key)
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise BucketError(f"S3 head_object failed: {exc}", cause=exc) from exc

        return _parse_head_response(key, response)

    async def _copy(
        self,
        source_key: str,
        dest_key: str,
        options: CopyObjectOptions | None,
    ) -> None:
        logger.debug("copy %s %s -> %s", self._bucket_name, source_key, dest_key)
        client = await self._client_provider()
        dest_bucket = options.dest_bucket if options and options.dest_bucket else self._bucket_name
        copy_source = f"{self._bucket_name}/{source_key}"
        params: dict[str, Any] = {
            "Bucket": dest_bucket,
            "Key": dest_key,
            "CopySource": copy_source,
        }
        if options:
            if options.metadata is not None:
                params["Metadata"] = options.metadata
                params["MetadataDirective"] = "REPLACE"
            if options.content_type is not None:
                params["ContentType"] = options.content_type
                if "MetadataDirective" not in params:
                    params["MetadataDirective"] = "REPLACE"

        try:
            await client.copy_object(**params)
        except Exception as exc:
            raise BucketError(f"S3 copy_object failed: {exc}", cause=exc) from exc

    async def _sign_url(
        self,
        key: str,
        options: SignUrlOptions | None,
    ) -> str:
        logger.debug("sign_url %s %s", self._bucket_name, key)
        client = await self._client_provider()
        opts = options or SignUrlOptions()
        client_method = "get_object" if opts.method == "get" else "put_object"
        try:
            url: str = await client.generate_presigned_url(
                ClientMethod=client_method,
                Params={"Bucket": self._bucket_name, "Key": key},
                ExpiresIn=opts.expires_in,
            )
        except Exception as exc:
            raise BucketError(f"S3 generate_presigned_url failed: {exc}", cause=exc) from exc
        return url


def _build_range_header(start: int | None, end: int | None) -> str | None:
    """Build an HTTP Range header value."""
    if start is not None and end is not None:
        return f"bytes={start}-{end}"
    if start is not None:
        return f"bytes={start}-"
    if end is not None:
        return f"bytes=0-{end}"
    return None


def _is_not_found(exc: Exception) -> bool:
    """Check if an exception is a 404 / NoSuchKey."""
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    code = response.get("Error", {}).get("Code", "")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
    return code in ("NoSuchKey", "404", "Not Found") or status == 404


def _parse_head_response(key: str, response: Any) -> ObjectInfo:
    """Parse an S3 GetObject/HeadObject response into ObjectInfo."""
    last_modified = response.get("LastModified")
    if last_modified is None:
        last_modified = datetime.now(tz=UTC)
    return ObjectInfo(
        key=key,
        size=response.get("ContentLength", 0),
        content_type=response.get("ContentType", ""),
        last_modified=last_modified,
        etag=response.get("ETag", "").strip('"'),
        metadata=response.get("Metadata", {}),
    )


async def _wrap_stream(body: Any) -> AsyncIterator[bytes]:
    """Wrap an S3 streaming body as an AsyncIterator[bytes]."""
    async for chunk in body:
        yield chunk
