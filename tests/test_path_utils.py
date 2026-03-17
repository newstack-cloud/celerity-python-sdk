"""Tests for celerity.common.path_utils."""

import pytest

from celerity.common.path_utils import join_handler_path


@pytest.mark.parametrize(
    ("prefix", "path", "expected"),
    [
        ("/orders", "/{id}", "/orders/{id}"),
        ("/orders", "/", "/orders"),
        ("", "/{id}", "/{id}"),
        ("/", "/health", "/health"),
        ("/api", "items", "/api/items"),
        ("/api/", "/{id}", "/api/{id}"),
        ("", "/", "/"),
        ("/v1/orders", "/{id}/items", "/v1/orders/{id}/items"),
    ],
)
def test_join_handler_path(prefix: str, path: str, expected: str) -> None:
    assert join_handler_path(prefix, path) == expected
