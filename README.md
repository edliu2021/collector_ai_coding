# EventCollector

An in-memory event collector that forwards events to a consumer while
suppressing duplicate event IDs seen within a configurable time window.
Python standard library only.

## The problem

Multiple producers send events to a collector. Each event has an `event_id` and a
payload. The collector forwards events to a consumer, but must suppress duplicate
IDs within N minutes.

## Rules

1. If an ID has never been received, forward the event.
2. If the same ID was received less than N minutes ago, drop the event.
3. Every received event, including a dropped duplicate, updates that ID's
   last-seen time.
4. If N minutes or more have elapsed since the last receipt, forward the event
   again.
5. Deduplicate by ID only, regardless of payload.

Worked example with a 10-minute window:

| Time  | ID  | Result                             |
|-------|-----|------------------------------------|
| 12:00 | `a` | forward                            |
| 12:03 | `a` | drop; last-seen becomes 12:03      |
| 12:04 | `b` | forward                            |
| 12:08 | `a` | drop; last-seen becomes 12:08      |
| 12:19 | `a` | forward — 11 minutes have elapsed  |

## Files

| File | Purpose |
|------|---------|
| `event_collector.py` | The `EventCollector` implementation |
| `test_event_collector.py` | 22 deterministic tests |

Python standard library only; no third-party dependencies.

## Usage

```python
from event_collector import EventCollector

collector = EventCollector(consumer, window_minutes=10)
collector.process("event-123", {"kind": "order.created"})  # True if forwarded
```

`consumer` is a synchronous callable taking `(event_id, payload)`. The clock is
injectable (`clock=time.monotonic` by default) so tests control time without
sleeping. A non-positive `window_minutes` raises `ValueError`.

## Running the tests

```sh
python3 -m unittest test_event_collector -v
```

## Design

A single `OrderedDict` maps `event_id` to its last-seen timestamp, ordered
oldest-receipt-first. Each call to `process()`:

1. **Sweeps the front** — deletes entries where `now - last_seen >= window`,
   stopping at the first live one. Every insert and refresh appends to the back,
   so the ordering invariant holds and the first live entry proves everything
   behind it is live too. No full scan.
2. **Checks membership** — anything surviving the sweep is inside the window, so
   a hit means drop: restamp with `now`, `move_to_end()`, return `False`.
3. **Otherwise forwards** — calls the consumer, then records `now` at the back.

Two deliberate choices:

- The ID is recorded *after* the consumer returns. If the consumer raises, the
  exception propagates and nothing is recorded, so a later retry is treated as a
  first occurrence and gets forwarded.
- Expiry uses `>=`, so exactly `window_minutes` elapsed forwards; a hair under
  drops. Both sides of the boundary are tested.

Not thread-safe — calls are assumed sequential, per the brief.

## Complexity

- **Time:** O(1) amortized per `process()`. Lookup, insert and `move_to_end` are
  O(1); the sweep costs O(k) for the k entries it evicts, and each ID is evicted
  at most once per insertion, so it amortizes to O(1). A single call is O(k) in
  the worst case, when a large backlog expires at once.
- **Space:** O(d), where d is the number of distinct IDs seen within one window —
  one key and one float each. Bounded by the window, not by total traffic, since
  expired IDs are swept rather than accumulated.
