"""Utility functions."""

from typing import Any

from apache_beam.options.value_provider import RuntimeValueProvider


def get_runtime_value(value: Any) -> Any:
    if isinstance(value, RuntimeValueProvider):
        return value.get()
    else:
        return value


def cleanse_query(query: str) -> str:
    return query.strip(";")
