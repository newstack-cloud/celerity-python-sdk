"""DynamoDB item marshalling/unmarshalling.

Converts between plain Python dicts and DynamoDB's low-level typed
attribute format so the rest of the SDK works with plain values.

    Plain:    {"pk": "user-1", "age": 25, "active": True}
    DynamoDB: {"pk": {"S": "user-1"}, "age": {"N": "25"}, "active": {"BOOL": True}}
"""

from __future__ import annotations

from typing import Any

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

_serializer = TypeSerializer()
_deserializer = TypeDeserializer()


def marshall_item(item: dict[str, Any]) -> dict[str, Any]:
    """Convert a plain Python dict to DynamoDB's typed attribute format."""
    return {k: _serializer.serialize(v) for k, v in item.items()}


def unmarshall_item(item: dict[str, Any]) -> dict[str, Any]:
    """Convert a DynamoDB typed attribute dict to a plain Python dict."""
    return {k: _deserializer.deserialize(v) for k, v in item.items()}
