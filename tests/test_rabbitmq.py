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
