"""In-memory event collector that suppresses duplicate event IDs within a window.

Producers hand events to :class:`EventCollector`, which forwards them to a
consumer unless the same ``event_id`` was already seen less than
``window_minutes`` ago.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from typing import Any, Callable


class EventCollector:
    """Forwards events to a consumer, dropping IDs repeated within a window.

    Deduplication is by ``event_id`` alone; payloads are never compared.
    Every receipt -- forwarded or dropped -- refreshes that ID's last-seen
    time, so a steady stream of duplicates keeps the ID suppressed.

    Not thread-safe: calls are assumed to be sequential.
    """

    def __init__(
        self,
        consumer: Callable[[str, Any], Any],
        window_minutes: float = 10,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if window_minutes <= 0:
            raise ValueError(f"window_minutes must be positive, got {window_minutes!r}")
        self._consumer = consumer
        self._window_seconds = window_minutes * 60.0
        self._clock = clock
        # event_id -> last-seen time, ordered oldest last-seen first.
        self._last_seen: "OrderedDict[str, float]" = OrderedDict()

    @property
    def window_seconds(self) -> float:
        return self._window_seconds

    def process(self, event_id: str, payload: Any = None) -> bool:
        """Receive one event. Returns True if forwarded, False if dropped.

        If the consumer raises, the exception propagates and the ID is not
        recorded, so an identical retry is still treated as a first occurrence.
        """
        now = self._clock()
        self._evict_expired(now)

        if event_id in self._last_seen:
            # Still inside the window: drop, but refresh the last-seen time.
            self._last_seen[event_id] = now
            self._last_seen.move_to_end(event_id)
            return False

        self._consumer(event_id, payload)
        self._last_seen[event_id] = now
        return True

    def _evict_expired(self, now: float) -> None:
        """Drop IDs whose window has elapsed, oldest first.

        Entries are ordered by last-seen time, so the first entry that has not
        expired proves none of the later ones have either -- we can stop there
        instead of scanning the whole map.
        """
        while self._last_seen:
            event_id, last_seen = next(iter(self._last_seen.items()))
            if now - last_seen < self._window_seconds:
                return
            del self._last_seen[event_id]

    def tracked_ids(self) -> int:
        """How many IDs are currently retained. Exposed for tests and metrics."""
        return len(self._last_seen)
