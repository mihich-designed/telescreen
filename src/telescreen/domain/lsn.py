from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True, order=True, slots=True)
class Lsn:
    """Log sequence number: a 64-bit position in the PostgreSQL write-ahead log."""

    value: int

    ZERO: ClassVar[Lsn]

    def __post_init__(self) -> None:
        if not 0 <= self.value < 2**64:
            raise ValueError(f"LSN out of range: {self.value}")

    @classmethod
    def parse(cls, text: str) -> Lsn:
        """Parse the textual form used by PostgreSQL, e.g. ``16/B374D848``."""
        high, sep, low = text.partition("/")
        if not sep or not high or not low:
            raise ValueError(f"Malformed LSN: {text!r}")
        return cls((int(high, 16) << 32) | int(low, 16))

    def __str__(self) -> str:
        return f"{self.value >> 32:X}/{self.value & 0xFFFFFFFF:X}"


Lsn.ZERO = Lsn(0)
