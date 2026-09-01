"""
tests/test_rabbitmq.py
=======================
Integration tests for RabbitMQAdapter against a REAL local RabbitMQ broker
(no mocking -- unlike test_kafka.py, a real broker is genuinely available
here: `rabbitmq-server -detached`, reachable at RABBITMQ_URL / localhost).

  RabbitMQAdapter → get_queue(name="rabbitmq")
"""

from __future__ import annotations

import os
import uuid

import pytest

pika = pytest.importorskip("pika")

from polydb.adapters.RabbitMQAdapter import RabbitMQAdapter
from polydb.cloudDatabaseFactory import CloudDatabaseFactory
from polydb.models import CloudProvider, RabbitMQConfig


def _broker_available() -> bool:
    """Best-effort connectivity probe so this suite skips cleanly (rather
    than erroring) on a machine with no RabbitMQ running, same spirit as
    conftest.py's `_require()` skip helpers for the other cloud providers."""
    try:
        url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
        conn = pika.BlockingConnection(pika.URLParameters(url))
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _broker_available(),
    reason="No local RabbitMQ broker reachable (start one with `rabbitmq-server -detached`)",
)


def _queue_name() -> str:
    return f"polydb_test_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def adapter():
    url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
    a = RabbitMQAdapter(url=url)
    yield a
    a.close()


# ────────────────────────────────────────────────────────────────────────────
# Core send -> receive -> ack round trip (the "done" criterion from the task)
# ────────────────────────────────────────────────────────────────────────────

class TestRabbitMQRoundTrip:
    def test_send_returns_a_message_id(self, adapter):
        queue = _queue_name()
        msg_id = adapter.send({"hello": "world"}, queue_name=queue)
        assert isinstance(msg_id, str)
        assert msg_id  # non-empty

    def test_receive_returns_the_sent_body(self, adapter):
        queue = _queue_name()
        payload = {"order_id": uuid.uuid4().hex, "amount": 42}
        adapter.send(payload, queue_name=queue)

        received = adapter.receive(queue_name=queue, max_messages=1)
        assert len(received) == 1
        assert received[0]["body"] == payload
        assert received[0]["id"]

    def test_send_receive_ack_round_trip_removes_message(self, adapter):
        """The full round trip the task asks to prove for real: after
        ack(), the message must be gone from the queue (not redelivered),
        proving basic_ack actually reached the broker on the right
        delivery tag."""
        queue = _queue_name()
        payload = {"task": "process", "n": 7}

        sent_id = adapter.send(payload, queue_name=queue)

        received = adapter.receive(queue_name=queue, max_messages=1)
        assert len(received) == 1
        assert received[0]["body"] == payload
        # Our own generated id round-trips through AMQP message properties.
        assert received[0]["id"] == sent_id

        ack_id = received[0]["receipt_handle"]
        assert adapter.ack(ack_id, queue_name=queue) is True

        # Nothing left to receive -- the broker actually removed it.
        again = adapter.receive(queue_name=queue, max_messages=1)
        assert again == []

    def test_unacked_message_is_redelivered_on_new_connection(self):
        """basic_get(auto_ack=False) leaves the message unacked; a fresh
        connection/consumer that reconnects should still be able to see it
        (proves ack() -- not receive() -- is what actually removes a
        message, matching the at-least-once contract documented in
        RabbitMQAdapter.receive())."""
        url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
        queue = _queue_name()
        payload = {"redelivery": "check"}

        a1 = RabbitMQAdapter(url=url)
        try:
            a1.send(payload, queue_name=queue)
            received = a1.receive(queue_name=queue, max_messages=1)
            assert len(received) == 1
            # Deliberately do NOT ack -- close the connection with the
            # message still outstanding, so RabbitMQ requeues it.
        finally:
            a1.close()

        a2 = RabbitMQAdapter(url=url)
        try:
            redelivered = a2.receive(queue_name=queue, max_messages=1)
            assert len(redelivered) == 1
            assert redelivered[0]["body"] == payload
            assert redelivered[0]["redelivered"] is True
            # Clean up so the queue doesn't leak an unacked message.
            a2.ack(redelivered[0]["receipt_handle"], queue_name=queue)
        finally:
            a2.close()

    def test_delete_is_equivalent_to_ack(self, adapter):
        queue = _queue_name()
        adapter.send({"x": 1}, queue_name=queue)
        received = adapter.receive(queue_name=queue, max_messages=1)
        assert adapter.delete(received[0]["id"], queue_name=queue) is True
        assert adapter.receive(queue_name=queue, max_messages=1) == []

    def test_ack_of_unknown_id_is_a_harmless_no_op(self, adapter):
        assert adapter.ack("not-a-real-id", queue_name=_queue_name()) is False

    def test_receive_multiple_messages(self, adapter):
        queue = _queue_name()
        for i in range(3):
            adapter.send({"n": i}, queue_name=queue)

        received = adapter.receive(queue_name=queue, max_messages=5)
        assert len(received) == 3
        assert {m["body"]["n"] for m in received} == {0, 1, 2}

        for m in received:
            adapter.ack(m["receipt_handle"], queue_name=queue)

    def test_receive_on_empty_queue_returns_empty_list(self, adapter):
        assert adapter.receive(queue_name=_queue_name(), max_messages=5) == []

    def test_queue_auto_created(self, adapter):
        """send() must not require the queue to already exist."""
        queue = _queue_name()
        # No prior queue_declare from the test itself -- adapter.send()
        # is solely responsible for creating it.
        adapter.send({"auto": "created"}, queue_name=queue)
        received = adapter.receive(queue_name=queue, max_messages=1)
        assert len(received) == 1
        adapter.ack(received[0]["receipt_handle"], queue_name=queue)


# ────────────────────────────────────────────────────────────────────────────
# Wiring through CloudDatabaseFactory.get_queue()
# ────────────────────────────────────────────────────────────────────────────

class TestRabbitMQFactoryWiring:
    def test_get_queue_returns_rabbitmq_adapter(self):
        url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
        factory = CloudDatabaseFactory(
            provider=CloudProvider.RABBITMQ,
            storage_configs=[RabbitMQConfig(name="rabbitmq", url=url)],
        )
        adapter = factory.get_queue("rabbitmq")
        assert isinstance(adapter, RabbitMQAdapter)

    def test_send_receive_ack_through_factory(self):
        url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
        factory = CloudDatabaseFactory(
            provider=CloudProvider.RABBITMQ,
            storage_configs=[RabbitMQConfig(name="rabbitmq", url=url)],
        )
        queue = factory.get_queue("rabbitmq")
        qname = _queue_name()

        msg_id = queue.send({"via": "factory"}, queue_name=qname)
        received = queue.receive(queue_name=qname, max_messages=1)
        assert received[0]["id"] == msg_id
        assert queue.ack(received[0]["receipt_handle"], queue_name=qname) is True

    def test_get_queue_returns_the_same_cached_instance_across_calls(self):
        # A real bug, found by exercising send/receive/ack through
        # CloudDatabaseFactory.get_queue() end to end (universal_engine's
        # queue.call connector, not this file directly): get_queue()'s
        # instance cache used to be written as
        # `self.instances["queue"] = instance` -- a hardcoded literal key,
        # not `self.instances[name]`. The read side correctly checked
        # `if name in self.instances`, so with the write always landing
        # under "queue" instead, that check was never true for any real
        # adapter name ("rabbitmq", "kafka", "vercel_queue", ...) --
        # every single get_queue(name) call quietly built a brand new
        # adapter instance instead of reusing one. That's invisible for
        # an adapter whose state lives entirely in the external service
        # (VercelQueueAdapter's local-redis mode, SQS, ...), but
        # RabbitMQAdapter/KafkaQueueAdapter track pending delivery
        # tags/offsets in `self._pending` on the *instance itself* --
        # receive() on instance A followed by ack() on a fresh instance B
        # (same name, same broker) found nothing to ack and silently
        # returned False instead of acking the real message.
        factory = CloudDatabaseFactory(
            provider=CloudProvider.RABBITMQ,
            storage_configs=[RabbitMQConfig(name="rabbitmq")],
        )
        first = factory.get_queue("rabbitmq")
        second = factory.get_queue("rabbitmq")
        assert first is second


# ────────────────────────────────────────────────────────────────────────────
# nack / purge / declare / status -- real broker coverage for the new
# QueueAdapter methods
# ────────────────────────────────────────────────────────────────────────────

class TestRabbitMQNack:
    def test_nack_requeues_for_immediate_redelivery(self, adapter):
        queue = _queue_name()
        payload = {"nack": "me"}
        adapter.send(payload, queue_name=queue)

        received = adapter.receive(queue_name=queue, max_messages=1)
        assert len(received) == 1
        assert adapter.nack(received[0]["receipt_handle"], queue_name=queue) is True

        # requeue=True puts it straight back -- available again without
        # needing a new connection the way an unacked message does.
        again = adapter.receive(queue_name=queue, max_messages=1)
        assert len(again) == 1
        assert again[0]["body"] == payload
        assert again[0]["redelivered"] is True
        adapter.ack(again[0]["receipt_handle"], queue_name=queue)

    def test_nack_of_unknown_id_is_a_harmless_no_op(self, adapter):
        assert adapter.nack("not-a-real-id", queue_name=_queue_name()) is False

    def test_nack_removes_message_from_pending_so_it_cannot_be_double_acked(self, adapter):
        queue = _queue_name()
        adapter.send({"x": 1}, queue_name=queue)
        received = adapter.receive(queue_name=queue, max_messages=1)

        assert adapter.nack(received[0]["receipt_handle"], queue_name=queue) is True
        # The delivery tag was already handed back to the broker by nack()
        # -- a second ack() against the same (now-stale) id must be a
        # no-op, not a double-ack of a tag pika no longer recognizes as
        # ours to acknowledge.
        assert adapter.ack(received[0]["receipt_handle"], queue_name=queue) is False

        # Drain the requeued copy so the queue doesn't leak it.
        again = adapter.receive(queue_name=queue, max_messages=1)
        adapter.ack(again[0]["receipt_handle"], queue_name=queue)


class TestRabbitMQPurge:
    def test_purge_removes_all_messages_and_returns_the_real_count(self, adapter):
        queue = _queue_name()
        for i in range(4):
            adapter.send({"n": i}, queue_name=queue)

        purged = adapter.purge(queue_name=queue)
        assert purged == 4
        assert adapter.receive(queue_name=queue, max_messages=10) == []

    def test_purge_on_empty_queue_returns_zero(self, adapter):
        assert adapter.purge(queue_name=_queue_name()) == 0


class TestRabbitMQDeclare:
    def test_declare_provisions_the_queue_ahead_of_first_send(self, adapter):
        queue = _queue_name()
        assert adapter.declare(queue, durable=True) is True

        status = adapter.status(queue)
        assert status["message_count"] == 0

    def test_declare_then_send_does_not_redeclare_with_conflicting_arguments(self, adapter):
        """The real regression this guards: declare() with dead-letter
        arguments followed by send() (which internally calls
        _ensure_queue()) must not attempt a second, argument-less
        queue_declare on the same name -- that's a channel-level
        PRECONDITION_FAILED in real AMQP. If _ensure_queue() ever stops
        respecting _declared_queues for a declare()-provisioned queue,
        this raises instead of silently regressing."""
        queue = _queue_name()
        dlq = _queue_name()
        adapter.declare(queue, dead_letter_queue=dlq)

        # Would raise QueueError (wrapping pika's ChannelClosedByBroker)
        # if _ensure_queue() re-declared queue_name without the
        # dead-letter arguments declare() already set.
        adapter.send({"after": "declare"}, queue_name=queue)
        received = adapter.receive(queue_name=queue, max_messages=1)
        assert len(received) == 1
        adapter.ack(received[0]["receipt_handle"], queue_name=queue)


class TestRabbitMQStatus:
    def test_status_reports_message_and_consumer_counts(self, adapter):
        import time

        queue = _queue_name()
        adapter.send({"a": 1}, queue_name=queue)
        adapter.send({"a": 2}, queue_name=queue)
        # basic_publish is fire-and-forget (no publisher confirms) --
        # queue_declare(passive=True) is a synchronous RPC that can beat
        # a just-published message to the broker's own queue state, same
        # real timing gap the DLQ tests below already account for.
        time.sleep(0.2)

        status = adapter.status(queue)
        assert status["message_count"] == 2
        assert status["consumer_count"] == 0

        adapter.receive(queue_name=queue, max_messages=10)


# ────────────────────────────────────────────────────────────────────────────
# Dead-letter queue -- real end-to-end AMQP dead-lettering, not a
# hand-rolled shadow implementation
# ────────────────────────────────────────────────────────────────────────────

class TestRabbitMQDeadLetterQueue:
    def test_rejected_message_lands_in_the_real_dlq_and_is_receivable_there(self, adapter):
        """
        What actually triggers RabbitMQ's DLX was checked against the
        real broker, not assumed: `nack(requeue=True)` (this adapter's
        own nack() contract -- "redeliver now") does NOT dead-letter a
        message, by AMQP's own semantics; only a reject/nack with
        requeue=False (or a TTL/max-length policy neither of which
        declare() exposes) does. Since the adapter's public nack()
        always requeues, this test reaches the delivery tag pika/AMQP
        actually needs directly (same private-attribute access pattern
        polydb's own test_kafka.py already uses for adapter internals,
        e.g. `a._pending["orphan-id"] = ...`) to issue the real
        requeue=False reject that triggers dead-lettering, then proves
        the message is genuinely sitting in the separate DLQ queue
        declare() wired up -- not a hand-rolled re-route inside this
        adapter.
        """
        source = _queue_name()
        dlq = _queue_name()
        payload = {"doomed": True}

        assert adapter.declare(source, dead_letter_queue=dlq) is True

        adapter.send(payload, queue_name=source)
        received = adapter.receive(queue_name=source, max_messages=1)
        assert len(received) == 1

        delivery_tag = adapter._pending.pop(received[0]["id"])
        adapter._channel.basic_nack(delivery_tag=delivery_tag, requeue=False)

        # Give RabbitMQ a moment to route the dead-lettered message.
        import time

        time.sleep(0.3)

        assert adapter.receive(queue_name=source, max_messages=1) == []

        dlq_status = adapter.status(dlq)
        assert dlq_status["message_count"] == 1

        dlq_received = adapter.receive(queue_name=dlq, max_messages=1)
        assert len(dlq_received) == 1
        assert dlq_received[0]["body"] == payload
        assert adapter.ack(dlq_received[0]["receipt_handle"], queue_name=dlq) is True

    def test_dlq_replay_pattern_moves_a_message_back_to_the_main_queue(self, adapter):
        """Proves the "dlq_list/dlq_replay are just receive/send+ack
        against the DLQ queue name" convention this repo documents for
        universal_engine's connectors/queue.py end to end at the polydb
        layer: receive from the DLQ, re-send to the main queue, ack the
        DLQ copy."""
        source = _queue_name()
        dlq = _queue_name()
        payload = {"replay": "me"}

        adapter.declare(source, dead_letter_queue=dlq)
        adapter.send(payload, queue_name=source)
        received = adapter.receive(queue_name=source, max_messages=1)
        delivery_tag = adapter._pending.pop(received[0]["id"])
        adapter._channel.basic_nack(delivery_tag=delivery_tag, requeue=False)

        import time

        time.sleep(0.3)

        dlq_received = adapter.receive(queue_name=dlq, max_messages=1)
        assert len(dlq_received) == 1

        # dlq_replay: re-send to the main queue, then ack the DLQ copy.
        adapter.send(dlq_received[0]["body"], queue_name=source)
        assert adapter.ack(dlq_received[0]["receipt_handle"], queue_name=dlq) is True

        replayed = adapter.receive(queue_name=source, max_messages=1)
        assert len(replayed) == 1
        assert replayed[0]["body"] == payload
        adapter.ack(replayed[0]["receipt_handle"], queue_name=source)
