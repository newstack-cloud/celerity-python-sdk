"""Common type definitions shared across the SDK."""

from typing import Any, Protocol, runtime_checkable

type InjectionToken = type | str
"""A DI token: a class reference or a string identifier."""

type Type[T] = type[T]
"""A class constructor type."""


@runtime_checkable
class Schema[T](Protocol):
    """Validation protocol for parsing and validating data.

    Compatible with Pydantic v2 models via ``model_validate``.

    Pydantic v2 models satisfy this protocol natively::

        class OrderInput(BaseModel):
            name: str
            quantity: int

        schema: Schema[OrderInput] = OrderInput
        validated = schema.model_validate({"name": "Widget", "quantity": 5})

    Custom validators can also implement this protocol::

        class MyValidator:
            @classmethod
            def model_validate(cls, obj: Any) -> MyOutput:
                ...
    """

    @classmethod
    def model_validate(cls, obj: Any, **kwargs: Any) -> T: ...
