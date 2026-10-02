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

## Test case walkthrough

### How the clock is controlled

No test sleeps. Time is a parameter, set in three layers:

1. **The test declares minutes.** `build(window_minutes=10, start=0.0)` constructs a
   `FakeClock` at `0.0`, a `RecordingConsumer`, and the collector.
2. **The constructor converts to seconds.** `self._window_seconds = window_minutes * 60.0`,
   so a 10-minute window is `600.0`. Minutes are only the public API unit; all
   internal comparisons are float seconds.
3. **The clock is injected, replacing `time.monotonic`.** `process()` reads
   `now = self._clock()`, which returns `FakeClock.now`. `advance_minutes(n)` does
   `now += n * 60.0`.

`FakeClock` deliberately mirrors the `time.monotonic` contract — float seconds,
monotonically non-decreasing, arbitrary origin — so swapping the real clock back in
for production requires no change to the logic under test.

### Which test guards which decision

Established by mutating each decision in turn and recording what turned red:

| Decision in the implementation | Test that guards it | Notes |
|---|---|---|
| A dropped duplicate still refreshes its timestamp (rule 3) | `test_dropped_duplicate_refreshes_last_seen` | 3 tests catch it |
| The refresh calls `move_to_end`, preserving the ordering invariant | `test_refreshed_duplicate_moves_to_the_back_of_the_order` | **only test that catches it** |
| Expiry compares with `>=`, so exactly N minutes forwards | `test_exactly_at_window_forwards` + `test_just_before_window_drops` | 5 tests catch it |
| The sweep stops at the first live entry | `test_sweep_stops_at_the_first_live_entry` | breaking it reddens 14 tests |
| The ID is recorded only after the consumer returns | `test_failed_event_is_not_recorded_so_retry_is_forwarded` | **only test that catches it** |
| A non-positive window raises | `test_rejects_nonpositive_window` | **only test that catches it** |

### Rule 3: the window runs from the last receipt

`test_dropped_duplicate_refreshes_last_seen`, with a 600-second window:

| Call | `clock.now` | `last_seen["a"]` | Delta | vs 600 | Outcome |
|---|---|---|---|---|---|
| 1st | `0.0` | absent | — | — | forward, record `0.0` |
| 2nd | `540.0` | `0.0` | `540.0` | `< 600` | drop, **refresh to `540.0`** |
| 3rd | `840.0` | `540.0` | `300.0` | `< 600` | drop |

The third row is the discriminating one. Fourteen minutes have passed since the
event was *forwarded*, but only five since it was last *received*. An
implementation that skipped the refresh would still hold `0.0`, compute
`840 - 0 = 840 >= 600`, and wrongly forward.

Worth noting: **the worked example in the problem statement does not exercise this
rule.** Delete the refresh entirely and the 12:00/12:03/12:04/12:08/12:19 sequence
still passes, because at 12:08 both readings drop (8 min vs 5 min, both under 10)
and at 12:19 both forward (19 min vs 11 min, both at or over 10). The two semantics
only diverge for a sequence like 0 / 9 / 14, which is why that test exists
alongside `test_scenario_from_the_brief`.

### The expiration boundary

`TestExpirationBoundary` pins the three regions around the boundary. The line under
test is `if now - last_seen < self._window_seconds: return` in `_evict_expired`.

`test_just_before_window_drops` — a hair short does not expire:

| Step | `clock.now` | `last_seen["a"]` | Delta | Sweep | Outcome |
|---|---|---|---|---|---|
| 1st call | `0.0` | absent | — | map empty | forward, record `0.0` |
| +10 min, −0.001 | `599.999` | `0.0` | `599.999` | `< 600`, keep | hit → **drop** |

`clock.now -= 0.001` pokes the clock directly because `advance_minutes` cannot
express "a hair under".

`test_exactly_at_window_forwards` — the boundary itself forwards:

| Step | `clock.now` | `last_seen["a"]` | Delta | Sweep | Outcome |
|---|---|---|---|---|---|
| 1st call | `0.0` | absent | — | map empty | forward, record `0.0` |
| +10 min | `600.0` | `0.0` | `600.0` | `< 600` false, evict | miss → **forward** |

This is the load-bearing case. `600.0` is not less than `600.0`, so the ID is
evicted *before* the membership check runs and the call takes the never-seen branch.
That matches rule 4 literally — "N minutes **or more**" — which is why expiry is
`>=` and not `>`.

`test_after_window_forwards` covers the other half of "≥": `now = 630.0`, so
`630 - 0 >= 600` evicts and forwards.

`test_boundary_restarts_a_fresh_window` — a re-forward restarts the window:

| Step | `clock.now` | `last_seen["a"]` | Delta | Outcome |
|---|---|---|---|---|
| 1st call | `0.0` | absent | — | forward, record `0.0` |
| +10 min | `600.0` | `0.0` | `600.0` | evict → **forward**, record `600.0` |
| +9 min | `1140.0` | `600.0` | `540.0` | `< 600` → **drop** |

This guards a regression: it proves the second forward moved the window origin to
`600.0` rather than still referencing `0.0`, which would compute `1140 - 0 = 1140`,
read as expired, and forward a third time.

On precision: `10 * 60.0` and `0.0 + 600.0` are both exact in binary floating point,
so the equality at the boundary is strict rather than incidental. It would turn
brittle if the clock advanced by a value with no exact representation, such as `0.1`.

### Eviction from the front

`test_expired_ids_are_dropped_from_the_map` seeds three IDs, advances past the
window, and asserts via `tracked_ids()` that an unrelated event sweeps all three.

`test_sweep_stops_at_the_first_live_entry` places `old` at t=0 and `new` at t=6, then
checks at t=10 that `old` is gone while `new` survives with 6 minutes left — the
early stop, not a full scan.

`test_refreshed_duplicate_moves_to_the_back_of_the_order` is the subtle one. `a` at
t=0, `b` at t=1, then `a` again at t=2, which refreshes `a` and must move it behind
`b`. At t=11 the sweep has to evict `b` while keeping `a`, which only works if the
map is ordered by last receipt. Without `move_to_end`, a continuously refreshed ID
would sit at the head of the queue and block eviction of genuinely older IDs behind
it — a slow memory leak that no other test in the suite detects.
