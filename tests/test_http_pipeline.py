"""Tests for celerity.handlers.http_pipeline response normalisation."""

import dataclasses
import json

from pydantic import BaseModel

from celerity.handlers.http_pipeline import _normalise_response
from celerity.types.http import HttpResponse


class UserModel(BaseModel):
    id: int
    name: str


@dataclasses.dataclass
class OrderResult:
    order_id: str
    total: float


class TestNormaliseResponsePassthrough:
    def test_http_response_unchanged(self) -> None:
        resp = HttpResponse(status=200, body="ok")
        assert _normalise_response(resp) is resp

    def test_none_returns_204(self) -> None:
        resp = _normalise_response(None)
        assert resp.status == 204
        assert resp.body is None


class TestNormaliseResponseDict:
    def test_dict_returns_json_200(self) -> None:
        resp = _normalise_response({"key": "value"})
        assert resp.status == 200
        assert json.loads(resp.body) == {"key": "value"}  # type: ignore[arg-type]
        assert resp.headers == {"content-type": "application/json"}

    def test_dict_post_returns_201(self) -> None:
        resp = _normalise_response({"id": 1}, method="POST")
        assert resp.status == 201
        assert json.loads(resp.body) == {"id": 1}  # type: ignore[arg-type]

    def test_dict_get_returns_200(self) -> None:
        resp = _normalise_response({"id": 1}, method="GET")
        assert resp.status == 200


class TestNormaliseResponseStr:
    def test_str_returns_text_200(self) -> None:
        resp = _normalise_response("hello")
        assert resp.status == 200
        assert resp.body == "hello"
        assert resp.headers is None

    def test_str_post_returns_201(self) -> None:
        resp = _normalise_response("created", method="POST")
        assert resp.status == 201


class TestNormaliseResponsePydantic:
    def test_pydantic_model_serialised(self) -> None:
        user = UserModel(id=1, name="Alice")
        resp = _normalise_response(user)
        assert resp.status == 200
        assert json.loads(resp.body) == {"id": 1, "name": "Alice"}  # type: ignore[arg-type]
        assert resp.headers == {"content-type": "application/json"}

    def test_pydantic_model_post_returns_201(self) -> None:
        user = UserModel(id=1, name="Alice")
        resp = _normalise_response(user, method="POST")
        assert resp.status == 201


class TestNormaliseResponseDataclass:
    def test_dataclass_serialised(self) -> None:
        order = OrderResult(order_id="ord-1", total=29.99)
        resp = _normalise_response(order)
        assert resp.status == 200
        assert json.loads(resp.body) == {"order_id": "ord-1", "total": 29.99}  # type: ignore[arg-type]
        assert resp.headers == {"content-type": "application/json"}

    def test_dataclass_post_returns_201(self) -> None:
        order = OrderResult(order_id="ord-1", total=29.99)
        resp = _normalise_response(order, method="POST")
        assert resp.status == 201


class TestNormaliseResponseTuple:
    def test_tuple_status_dict(self) -> None:
        resp = _normalise_response((201, {"id": "123"}))
        assert resp.status == 201
        assert json.loads(resp.body) == {"id": "123"}  # type: ignore[arg-type]

    def test_tuple_status_string(self) -> None:
        resp = _normalise_response((200, "ok"))
        assert resp.status == 200
        assert resp.body == "ok"

    def test_tuple_status_none(self) -> None:
        resp = _normalise_response((204, None))
        assert resp.status == 204
        assert resp.body is None

    def test_tuple_status_pydantic(self) -> None:
        user = UserModel(id=2, name="Bob")
        resp = _normalise_response((201, user))
        assert resp.status == 201
        assert json.loads(resp.body) == {"id": 2, "name": "Bob"}  # type: ignore[arg-type]

    def test_tuple_status_dataclass(self) -> None:
        order = OrderResult(order_id="ord-2", total=15.0)
        resp = _normalise_response((201, order))
        assert resp.status == 201
        assert json.loads(resp.body) == {"order_id": "ord-2", "total": 15.0}  # type: ignore[arg-type]


class TestStatusCodeInference:
    def test_post_defaults_to_201(self) -> None:
        resp = _normalise_response({"created": True}, method="POST")
        assert resp.status == 201

    def test_get_defaults_to_200(self) -> None:
        resp = _normalise_response({"data": "x"}, method="GET")
        assert resp.status == 200

    def test_put_defaults_to_200(self) -> None:
        resp = _normalise_response({"updated": True}, method="PUT")
        assert resp.status == 200

    def test_delete_none_returns_204(self) -> None:
        resp = _normalise_response(None, method="DELETE")
        assert resp.status == 204

    def test_no_method_defaults_to_200(self) -> None:
        resp = _normalise_response({"ok": True})
        assert resp.status == 200
