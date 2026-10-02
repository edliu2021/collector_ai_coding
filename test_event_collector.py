"""Deterministic tests for EventCollector -- time is injected, never slept on."""

import unittest

from event_collector import EventCollector


class FakeClock:
    """A clock the tests advance by hand, so no test ever sleeps."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance_minutes(self, minutes: float) -> None:
        self.now += minutes * 60.0


class RecordingConsumer:
    """Collects the (event_id, payload) pairs it was forwarded."""

    def __init__(self) -> None:
        self.calls = []

    def __call__(self, event_id, payload):
        self.calls.append((event_id, payload))


def build(window_minutes=10, start=0.0):
    clock = FakeClock(start)
    consumer = RecordingConsumer()
    return EventCollector(consumer, window_minutes, clock), consumer, clock


class TestConstruction(unittest.TestCase):
    def test_rejects_nonpositive_window(self):
        for bad in (0, -1, -0.5):
            with self.subTest(window=bad):
                with self.assertRaises(ValueError):
                    EventCollector(lambda event_id, payload: None, bad)

    def test_window_minutes_converted_to_seconds(self):
        collector, _, _ = build(window_minutes=10)
        self.assertEqual(collector.window_seconds, 600.0)

    def test_default_window_is_ten_minutes(self):
        collector = EventCollector(lambda event_id, payload: None)
        self.assertEqual(collector.window_seconds, 600.0)


class TestFirstOccurrence(unittest.TestCase):
    def test_first_event_is_forwarded_with_payload(self):
        collector, consumer, _ = build()
        self.assertTrue(collector.process("a", {"n": 1}))
        self.assertEqual(consumer.calls, [("a", {"n": 1})])

    def test_distinct_ids_are_each_forwarded(self):
        collector, consumer, clock = build()
        self.assertTrue(collector.process("a", "p1"))
        clock.advance_minutes(1)
        self.assertTrue(collector.process("b", "p2"))
        clock.advance_minutes(1)
        self.assertTrue(collector.process("c", "p3"))
        self.assertEqual(consumer.calls, [("a", "p1"), ("b", "p2"), ("c", "p3")])


class TestDuplicates(unittest.TestCase):
    def test_duplicate_inside_window_is_dropped(self):
        collector, consumer, clock = build()
        self.assertTrue(collector.process("a", "first"))
        clock.advance_minutes(3)
        self.assertFalse(collector.process("a", "second"))
        self.assertEqual(consumer.calls, [("a", "first")])

    def test_dedup_ignores_payload_differences(self):
        collector, consumer, clock = build()
        self.assertTrue(collector.process("a", {"version": 1}))
        clock.advance_minutes(1)
        self.assertFalse(collector.process("a", {"version": 2}))
        clock.advance_minutes(1)
        self.assertFalse(collector.process("a", None))
        self.assertEqual(consumer.calls, [("a", {"version": 1})])

    def test_immediate_duplicate_at_same_instant_is_dropped(self):
        collector, consumer, _ = build()
        self.assertTrue(collector.process("a", "p"))
        self.assertFalse(collector.process("a", "p"))
        self.assertEqual(len(consumer.calls), 1)

    def test_one_id_suppression_does_not_affect_another(self):
        collector, consumer, clock = build()
        collector.process("a", "p")
        clock.advance_minutes(3)
        self.assertFalse(collector.process("a", "p"))
        self.assertTrue(collector.process("b", "p"))
        self.assertEqual(consumer.calls, [("a", "p"), ("b", "p")])


class TestWindowRefresh(unittest.TestCase):
    def test_dropped_duplicate_refreshes_last_seen(self):
        """The window is measured from the last receipt, not the last forward."""
        collector, consumer, clock = build()
        self.assertTrue(collector.process("a", "p"))      # t=0  forward
        clock.advance_minutes(9)
        self.assertFalse(collector.process("a", "p"))     # t=9  drop, refresh
        clock.advance_minutes(5)
        # t=14: 14 min since the forward, but only 5 min since the last receipt.
        self.assertFalse(collector.process("a", "p"))
        self.assertEqual(len(consumer.calls), 1)

    def test_scenario_from_the_brief(self):
        collector, consumer, clock = build(window_minutes=10)

        self.assertTrue(collector.process("a", "p"))    # 12:00 forward
        clock.advance_minutes(3)
        self.assertFalse(collector.process("a", "p"))   # 12:03 drop, last-seen 12:03
        clock.advance_minutes(1)
        self.assertTrue(collector.process("b", "p"))    # 12:04 forward
        clock.advance_minutes(4)
        self.assertFalse(collector.process("a", "p"))   # 12:08 drop, last-seen 12:08
        clock.advance_minutes(11)
        self.assertTrue(collector.process("a", "p"))    # 12:19 forward, 11 min elapsed

        self.assertEqual(consumer.calls, [("a", "p"), ("b", "p"), ("a", "p")])

    def test_steady_duplicates_suppress_indefinitely(self):
        collector, consumer, clock = build()
        collector.process("a", "p")
        for _ in range(20):
            clock.advance_minutes(9)
            self.assertFalse(collector.process("a", "p"))
        self.assertEqual(len(consumer.calls), 1)


class TestExpirationBoundary(unittest.TestCase):
    def test_just_before_window_drops(self):
        collector, consumer, clock = build(window_minutes=10)
        collector.process("a", "p")
        clock.advance_minutes(10)
        clock.now -= 0.001  # a hair under 10 minutes
        self.assertFalse(collector.process("a", "p"))
        self.assertEqual(len(consumer.calls), 1)

    def test_exactly_at_window_forwards(self):
        """`now - last_seen >= window` expires, so the boundary itself forwards."""
        collector, consumer, clock = build(window_minutes=10)
        collector.process("a", "p")
        clock.advance_minutes(10)
        self.assertTrue(collector.process("a", "p"))
        self.assertEqual(len(consumer.calls), 2)

    def test_after_window_forwards(self):
        collector, consumer, clock = build(window_minutes=10)
        collector.process("a", "p")
        clock.advance_minutes(10.5)
        self.assertTrue(collector.process("a", "p"))
        self.assertEqual(len(consumer.calls), 2)

    def test_boundary_restarts_a_fresh_window(self):
        collector, consumer, clock = build(window_minutes=10)
        collector.process("a", "p")
        clock.advance_minutes(10)
        self.assertTrue(collector.process("a", "p"))   # forwarded again
        clock.advance_minutes(9)
        self.assertFalse(collector.process("a", "p"))  # inside the new window
        self.assertEqual(len(consumer.calls), 2)


class TestExpiredEntriesRemoved(unittest.TestCase):
    def test_expired_ids_are_dropped_from_the_map(self):
        collector, _, clock = build(window_minutes=10)
        collector.process("a", "p")
        collector.process("b", "p")
        collector.process("c", "p")
        self.assertEqual(collector.tracked_ids(), 3)

        clock.advance_minutes(10)
        # An unrelated event triggers the sweep of all three expired IDs.
        self.assertTrue(collector.process("z", "p"))
        self.assertEqual(collector.tracked_ids(), 1)

    def test_sweep_stops_at_the_first_live_entry(self):
        collector, _, clock = build(window_minutes=10)
        collector.process("old", "p")      # t=0
        clock.advance_minutes(6)
        collector.process("new", "p")      # t=6
        clock.advance_minutes(4)           # t=10: "old" expired, "new" has 6 min left
        self.assertTrue(collector.process("other", "p"))
        self.assertEqual(collector.tracked_ids(), 2)   # "new" and "other"
        self.assertFalse(collector.process("new", "p"))  # "new" survived the sweep

    def test_refreshed_duplicate_moves_to_the_back_of_the_order(self):
        """A refreshed ID must not block eviction of genuinely older IDs."""
        collector, _, clock = build(window_minutes=10)
        collector.process("a", "p")        # t=0
        clock.advance_minutes(1)
        collector.process("b", "p")        # t=1
        clock.advance_minutes(1)
        self.assertFalse(collector.process("a", "p"))  # t=2: "a" refreshed, now newest
        clock.advance_minutes(9)           # t=11: "b" expired, "a" seen 9 min ago
        self.assertTrue(collector.process("c", "p"))
        self.assertEqual(collector.tracked_ids(), 2)   # "a" and "c"; "b" swept
        self.assertFalse(collector.process("a", "p"))


class TestConsumerFailure(unittest.TestCase):
    def test_exception_propagates(self):
        def boom(event_id, payload):
            raise RuntimeError("consumer down")

        collector = EventCollector(boom, 10, FakeClock())
        with self.assertRaises(RuntimeError):
            collector.process("a", "p")

    def test_failed_event_is_not_recorded_so_retry_is_forwarded(self):
        clock = FakeClock()
        calls = []
        fail_next = {"flag": True}

        def flaky(event_id, payload):
            calls.append((event_id, payload))
            if fail_next["flag"]:
                fail_next["flag"] = False
                raise RuntimeError("transient failure")

        collector = EventCollector(flaky, 10, clock)

        with self.assertRaises(RuntimeError):
            collector.process("a", "p")
        self.assertEqual(collector.tracked_ids(), 0)

        clock.advance_minutes(1)
        self.assertTrue(collector.process("a", "p"))   # retry succeeds
        self.assertEqual(calls, [("a", "p"), ("a", "p")])

        clock.advance_minutes(1)
        self.assertFalse(collector.process("a", "p"))  # now deduplicated normally
        self.assertEqual(len(calls), 2)

    def test_failure_does_not_disturb_other_ids(self):
        clock = FakeClock()
        forwarded = []

        def consumer(event_id, payload):
            if event_id == "bad":
                raise RuntimeError("nope")
            forwarded.append(event_id)

        collector = EventCollector(consumer, 10, clock)
        self.assertTrue(collector.process("good", "p"))
        with self.assertRaises(RuntimeError):
            collector.process("bad", "p")
        clock.advance_minutes(1)
        self.assertFalse(collector.process("good", "p"))
        self.assertEqual(forwarded, ["good"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
