from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, order=True, slots=True)
class TableName:
    schema: str
    name: str

    @classmethod
    def parse(cls, text: str) -> TableName:
        """Parse ``schema.table``; a bare ``table`` is assumed to live in ``public``."""
        schema, sep, name = text.partition(".")
        if not sep:
            schema, name = "public", schema
        if not schema or not name or "." in name:
            raise ValueError(f"Malformed table name: {text!r}")
        return cls(schema, name)

    def __str__(self) -> str:
        return f"{self.schema}.{self.name}"
