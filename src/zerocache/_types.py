"""
zerocache._types
~~~~~~~~~~~~~~~~
DataType IntEnum and type resolution helpers.

IntEnum vs plain string:
  • Faster equality  — int compare vs string compare
  • Less memory      — small int (CPython cached) vs str heap object
  • O(1) resolution  — single dict lookup via _TYPE_MAP
"""

from __future__ import annotations

from enum  import IntEnum
from typing import Any, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    pass

__all__ = ["DataType"]


class DataType(IntEnum):
    """Enumeration of supported cache value types."""

    STRING = 1
    HASH   = 2
    LIST   = 3
    SET    = 4
    ZSET   = 5

    def __str__(self) -> str:
        return self.name.lower()   # keeps Redis-style string representation


# Resolve built-in container types in O(1) without chained isinstance()
_TYPE_MAP: Dict[type, DataType] = {
    dict: DataType.HASH,
    list: DataType.LIST,
    set:  DataType.SET,
}


def _resolve_dtype(value: Any) -> DataType:
    """Return DataType for *value* — O(1) for built-ins, O(1) for SortedSet."""
    # Import here to avoid circular import
    from zerocache._sorted_set import SortedSet

    dt = _TYPE_MAP.get(type(value))
    if dt is not None:
        return dt
    if isinstance(value, SortedSet):
        return DataType.ZSET
    return DataType.STRING
