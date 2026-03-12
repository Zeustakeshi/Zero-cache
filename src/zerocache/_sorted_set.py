"""
zerocache._sorted_set
~~~~~~~~~~~~~~~~~~~~~
Score-ordered container backed by a sorted list + dict index.

Complexity:
  zadd          → O(log n) search + O(n) shift
  zrange        → O(1) slice
  zrank         → O(log n) binary search
  zrangebyscore → O(log n + k)
"""

from __future__ import annotations

import bisect
from typing import Dict, List, Optional, Tuple

__all__ = ["SortedSet"]


class SortedSet:
    """Score-ordered container backed by a sorted list + dict index.

    Equivalent to Redis ZSET — supports zadd, zrem, zscore, zrank,
    zrange, zrangebyscore, and zcard.
    """

    __slots__ = ("_scores", "_sl")

    def __init__(self) -> None:
        self._scores: Dict[str, float]        = {}
        self._sl:     List[Tuple[float, str]] = []   # always sorted (score, member)

    # ── write ──────────────────────────────────────────────────────────

    def zadd(self, member: str, score: float) -> None:
        """Insert or update *member* with *score* — O(log n) find + O(n) list shift."""
        if member in self._scores:
            old = self._scores[member]
            if old == score:
                return
            pos = bisect.bisect_left(self._sl, (old, member))
            if pos < len(self._sl) and self._sl[pos] == (old, member):
                self._sl.pop(pos)
        self._scores[member] = score
        bisect.insort(self._sl, (score, member))

    def zrem(self, member: str) -> bool:
        """Remove *member*. Returns ``True`` if it existed."""
        if member not in self._scores:
            return False
        score = self._scores.pop(member)
        pos   = bisect.bisect_left(self._sl, (score, member))
        if pos < len(self._sl) and self._sl[pos] == (score, member):
            self._sl.pop(pos)
        return True

    # ── read ───────────────────────────────────────────────────────────

    def zscore(self, member: str) -> Optional[float]:
        """Return the score of *member*, or ``None`` if not present."""
        return self._scores.get(member)

    def zrank(self, member: str) -> Optional[int]:
        """Return 0-based ascending rank of *member* — O(log n)."""
        if member not in self._scores:
            return None
        score = self._scores[member]
        pos   = bisect.bisect_left(self._sl, (score, member))
        return pos if pos < len(self._sl) and self._sl[pos] == (score, member) else None

    def zrange(self, start: int, end: int, with_scores: bool = False) -> List:
        """Slice by rank range (inclusive *end*) — O(k).

        Args:
            start:       Start rank (0-based).
            end:         End rank inclusive; -1 means last element.
            with_scores: If ``True`` returns list of ``(score, member)`` tuples.
        """
        end    = end + 1 if end >= 0 else None
        sliced = self._sl[start:end]
        return sliced if with_scores else [m for _, m in sliced]

    def zrangebyscore(self, min_s: float, max_s: float) -> List[str]:
        """Return members with scores in ``[min_s, max_s]`` — O(log n + k)."""
        lo = bisect.bisect_left (self._sl, (min_s, ""))
        hi = bisect.bisect_right(self._sl, (max_s, "\xff\xff\xff\xff"))
        return [m for _, m in self._sl[lo:hi]]

    def zcard(self) -> int:
        """Return the number of members."""
        return len(self._scores)

    def __len__(self) -> int:
        return len(self._scores)

    def __repr__(self) -> str:
        return f"SortedSet({self._scores!r})"
