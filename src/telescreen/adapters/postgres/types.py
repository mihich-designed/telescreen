"""Conversion of PostgreSQL text-format values into JSON-friendly Python values.

Only types with an unambiguous JSON representation are converted. Everything else,
including ``numeric`` (to keep precision) and date/time types, stays a string in the
exact form PostgreSQL produced it.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

BOOL = 16
INT8 = 20
INT2 = 21
INT4 = 23
OID = 26
JSON = 114
FLOAT4 = 700
FLOAT8 = 701
JSONB = 3802


def _bool(text: str) -> bool:
    return text == "t"


_CONVERTERS: dict[int, Callable[[str], Any]] = {
    BOOL: _bool,
    INT2: int,
    INT4: int,
    INT8: int,
    OID: int,
    FLOAT4: float,
    FLOAT8: float,
    JSON: json.loads,
    JSONB: json.loads,
}


def decode_text(type_oid: int, text: str) -> Any:
    converter = _CONVERTERS.get(type_oid)
    return converter(text) if converter else text
