"""Tests for bucket DI parameter types and helpers."""

from __future__ import annotations

from typing import Annotated, get_args, get_type_hints

from celerity.resources.bucket.params import (
    DEFAULT_BUCKET_TOKEN,
    BucketParam,
    BucketResource,
    bucket_token,
)
from celerity.resources.bucket.types import Bucket


class TestBucketParam:
    def test_default(self) -> None:
        param = BucketParam()
        assert param.resource_type == "bucket"
        assert param.resource_name is None

    def test_named(self) -> None:
        param = BucketParam("images")
        assert param.resource_type == "bucket"
        assert param.resource_name == "images"


class TestBucketToken:
    def test_bucket_token(self) -> None:
        assert bucket_token("my-bucket") == "celerity:bucket:my-bucket"

    def test_default_bucket_token(self) -> None:
        assert DEFAULT_BUCKET_TOKEN == "celerity:bucket:default"


class TestBucketResource:
    def test_alias_resolves_to_annotated(self) -> None:
        args = get_args(BucketResource)
        assert args[0] is Bucket
        assert isinstance(args[1], BucketParam)
        assert args[1].resource_name is None

    def test_named_annotated(self) -> None:
        images_bucket = Annotated[Bucket, BucketParam("images")]
        args = get_args(images_bucket)
        assert args[0] is Bucket
        assert isinstance(args[1], BucketParam)
        assert args[1].resource_name == "images"

    def test_type_hint_resolution(self) -> None:
        class Service:
            def __init__(self, bucket: BucketResource) -> None:
                self.bucket = bucket

        hints = get_type_hints(Service.__init__, include_extras=True)
        args = get_args(hints["bucket"])
        assert args[0] is Bucket
