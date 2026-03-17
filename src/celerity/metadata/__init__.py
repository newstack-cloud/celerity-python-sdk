"""Metadata storage primitives."""

from celerity.metadata.keys import get_metadata, set_metadata
from celerity.metadata.store import HandlerMetadataStore

__all__ = ["HandlerMetadataStore", "get_metadata", "set_metadata"]
