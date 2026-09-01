"""
tests/test_kafka.py
=====================
Tests for KafkaQueueAdapter against a MOCKED kafka-python client.

Why mocked, not a real broker (unlike test_rabbitmq.py, which runs
against a real local RabbitMQ server): a real Kafka broker was not
obtainable in this sandbox -- no downloadable Kafka binary release is
reachable through the network proxy here, and building it from source
needs a full Gradle + Maven Central build that isn't practical in this
environment. This is a genuine environment limitation, not a design
choice -- if a real broker (or testcontainers-kafka) becomes available,
these should be replaced/supplemented with a real integration suite the
same way test_rabbitmq.py and test_secrets_aws.py (moto) already do for
their respective services.

Every test here patches kafka.KafkaProducer / kafka.KafkaConsumer at the
point KafkaQueueAdapter imports them (`from kafka import KafkaProducer`
inside the adapter method, so patching the attribute on the real `kafka`
package is what the adapter actually picks up) and asserts the adapter
calls the *real* kafka-python API shape (constructor args, .send().get(),
.poll(), .commit() with real OffsetAndMetadata/TopicPartition instances)
correctly -- not just that "some mock was called".
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

kafka = pytest.importorskip("kafka")
from kafka import OffsetAndMetadata, TopicPartition  # noqa: E402

from polydb.adapters.KafkaQueueAdapter import KafkaQueueAdapter  # noqa: E402
from polydb.cloudDatabaseFactory import CloudDatabaseFactory  # noqa: E402
from polydb.errors import QueueError  # noqa: E402
from polydb.models import CloudProvider, KafkaQueueConfig  # noqa: E402


def _record_metadata(partition=0, offset=0):
    m = MagicMock()
    m.partition = partition
    m.offset = offset
    return m


def _consumer_record(topic="orders", partition=0, offset=0, value=b'{"ok": true}'):
    rec = MagicMock()
    rec.partition = partition
    rec.offset = offset
    rec.value = value
    return rec


@pytest.fixture
def adapter():
    return KafkaQueueAdapter(bootstrap_servers="localhost:9092", group_id="test-group")


# ────────────────────────────────────────────────────────────────────────────
# Construction / config
# ────────────────────────────────────────────────────────────────────────────

class TestKafkaQueueAdapterConfig:
    def test_bootstrap_servers_parsed_into_a_list(self):
        a = KafkaQueueAdapter(bootstrap_servers="host1:9092, host2:9092")
        assert a.bootstrap_servers == ["host1:9092", "host2:9092"]

    def test_default_group_id_is_random_per_instance(self):
        a1 = KafkaQueueAdapter(bootstrap_servers="localhost:9092")
        a2 = KafkaQueueAdapter(bootstrap_servers="localhost:9092")
        assert a1.group_id != a2.group_id
        assert a1.group_id.startswith("polydb-")

    def test_explicit_group_id_is_respected(self):
        a = KafkaQueueAdapter(bootstrap_servers="localhost:9092", group_id="shared-workers")
        assert a.group_id == "shared-workers"

    def test_default_security_protocol_is_plaintext(self):
        a = KafkaQueueAdapter(bootstrap_servers="localhost:9092")
        assert a.security_protocol == "PLAINTEXT"

    def test_sasl_credentials_are_optional(self):
        a = KafkaQueueAdapter(bootstrap_servers="localhost:9092")
        assert a.sasl_mechanism is None
        assert a.sasl_plain_username is None
        assert a.sasl_plain_password is None

    def test_sasl_and_tls_kwargs_flow_into_client_kwargs(self):
        a = KafkaQueueAdapter(
            bootstrap_servers="broker:9093",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username="alice",
            sasl_plain_password="secret",
            ssl_cafile="/etc/ssl/ca.pem",
        )
        kwargs = a._client_kwargs()
        assert kwargs["security_protocol"] == "SASL_SSL"
        assert kwargs["sasl_mechanism"] == "PLAIN"
        assert kwargs["sasl_plain_username"] == "alice"
        assert kwargs["sasl_plain_password"] == "secret"
        assert kwargs["ssl_cafile"] == "/etc/ssl/ca.pem"

    def test_plaintext_client_kwargs_omit_sasl_and_ssl(self):
        a = KafkaQueueAdapter(bootstrap_servers="localhost:9092")
        kwargs = a._client_kwargs()
        assert "sasl_mechanism" not in kwargs
        assert "ssl_cafile" not in kwargs


# ────────────────────────────────────────────────────────────────────────────
# send()
# ────────────────────────────────────────────────────────────────────────────

class TestKafkaQueueAdapterSend:
    def test_send_constructs_producer_with_bootstrap_servers(self, adapter):
        with patch("kafka.KafkaProducer") as MockProducer:
            instance = MockProducer.return_value
            instance.send.return_value.get.return_value = _record_metadata(0, 5)

            adapter.send({"a": 1}, queue_name="orders")

            MockProducer.assert_called_once()
            _, kwargs = MockProducer.call_args
            assert kwargs["bootstrap_servers"] == ["localhost:9092"]
            assert kwargs["security_protocol"] == "PLAINTEXT"

    def test_send_publishes_json_encoded_body_to_the_topic(self, adapter):
        with patch("kafka.KafkaProducer") as MockProducer:
            instance = MockProducer.return_value
            instance.send.return_value.get.return_value = _record_metadata(0, 5)

            adapter.send({"order_id": "abc123"}, queue_name="orders")

            instance.send.assert_called_once()
            args, kwargs = instance.send.call_args
            assert args[0] == "orders"
            sent_body = kwargs["value"]
            assert json.loads(sent_body.decode("utf-8")) == {"order_id": "abc123"}

    def test_send_blocks_on_the_produce_future_and_returns_partition_offset_id(self, adapter):
        with patch("kafka.KafkaProducer") as MockProducer:
            instance = MockProducer.return_value
            future = instance.send.return_value
            future.get.return_value = _record_metadata(partition=2, offset=17)

            msg_id = adapter.send({"a": 1}, queue_name="orders")

            future.get.assert_called_once_with(timeout=10)
            assert msg_id == "2-17"

    def test_producer_is_created_once_and_reused_across_sends(self, adapter):
        with patch("kafka.KafkaProducer") as MockProducer:
            instance = MockProducer.return_value
            instance.send.return_value.get.return_value = _record_metadata(0, 0)

            adapter.send({"a": 1}, queue_name="orders")
            adapter.send({"a": 2}, queue_name="orders")

            MockProducer.assert_called_once()
            assert instance.send.call_count == 2

    def test_send_wraps_broker_errors_in_queue_error(self, adapter):
        with patch("kafka.KafkaProducer") as MockProducer:
            MockProducer.return_value.send.side_effect = RuntimeError("broker unreachable")
            with pytest.raises(QueueError):
                adapter.send({"a": 1}, queue_name="orders")


# ────────────────────────────────────────────────────────────────────────────
# receive()
# ────────────────────────────────────────────────────────────────────────────

class TestKafkaQueueAdapterReceive:
    def test_receive_constructs_consumer_with_group_id_and_manual_commit(self, adapter):
        with patch("kafka.KafkaConsumer") as MockConsumer:
            MockConsumer.return_value.poll.return_value = {}

            adapter.receive(queue_name="orders", max_messages=1)

            MockConsumer.assert_called_once()
            args, kwargs = MockConsumer.call_args
            assert args == ("orders",)
            assert kwargs["group_id"] == "test-group"
            # This is the whole point of the design: receive() must never
            # let the client auto-commit on its own schedule.
            assert kwargs["enable_auto_commit"] is False

    def test_receive_decodes_json_message_bodies(self, adapter):
        with patch("kafka.KafkaConsumer") as MockConsumer:
            tp = TopicPartition("orders", 0)
            rec = _consumer_record(topic="orders", partition=0, offset=3, value=b'{"n": 42}')
            MockConsumer.return_value.poll.return_value = {tp: [rec]}

            out = adapter.receive(queue_name="orders", max_messages=1)

            assert len(out) == 1
            assert out[0]["body"] == {"n": 42}
            assert out[0]["id"] == "0-3"
            assert out[0]["partition"] == 0
            assert out[0]["offset"] == 3

    def test_receive_stops_once_max_messages_reached_even_if_more_are_polled(self, adapter):
        with patch("kafka.KafkaConsumer") as MockConsumer:
            tp = TopicPartition("orders", 0)
            records = [_consumer_record(partition=0, offset=i) for i in range(5)]
            MockConsumer.return_value.poll.return_value = {tp: records}

            out = adapter.receive(queue_name="orders", max_messages=2)

            assert len(out) == 2

    def test_receive_does_not_commit_offsets(self, adapter):
        """The core design decision this task calls out explicitly:
        receive() must leave commit() untouched so a crash before ack()
        results in redelivery."""
        with patch("kafka.KafkaConsumer") as MockConsumer:
            tp = TopicPartition("orders", 0)
            MockConsumer.return_value.poll.return_value = {tp: [_consumer_record()]}

            adapter.receive(queue_name="orders", max_messages=1)

            MockConsumer.return_value.commit.assert_not_called()

    def test_receive_tracks_pending_offset_as_offset_plus_one(self, adapter):
        with patch("kafka.KafkaConsumer") as MockConsumer:
            tp = TopicPartition("orders", 0)
            MockConsumer.return_value.poll.return_value = {tp: [_consumer_record(offset=9)]}

            out = adapter.receive(queue_name="orders", max_messages=1)

            tracked_tp, next_offset = adapter._pending[out[0]["id"]]
            assert tracked_tp == tp
            assert next_offset == 10  # committed offset = next record to read

    def test_receive_on_empty_topic_returns_empty_list(self, adapter):
        with patch("kafka.KafkaConsumer") as MockConsumer:
            MockConsumer.return_value.poll.return_value = {}
            assert adapter.receive(queue_name="orders", max_messages=5) == []

    def test_consumer_is_created_once_per_topic_and_reused(self, adapter):
        with patch("kafka.KafkaConsumer") as MockConsumer:
            MockConsumer.return_value.poll.return_value = {}

            adapter.receive(queue_name="orders", max_messages=1)
            adapter.receive(queue_name="orders", max_messages=1)

            MockConsumer.assert_called_once()
            assert MockConsumer.return_value.poll.call_count == 2


# ────────────────────────────────────────────────────────────────────────────
# ack() / delete()
# ────────────────────────────────────────────────────────────────────────────

class TestKafkaQueueAdapterAckDelete:
    def _received_message(self, adapter, MockConsumer, offset=4):
        tp = TopicPartition("orders", 0)
        MockConsumer.return_value.poll.return_value = {tp: [_consumer_record(offset=offset)]}
        out = adapter.receive(queue_name="orders", max_messages=1)
        MockConsumer.return_value.poll.return_value = {}  # don't reuse for further polls
        return out[0]

    def test_ack_commits_the_correct_offset_and_metadata_type(self, adapter):
        with patch("kafka.KafkaConsumer") as MockConsumer:
            msg = self._received_message(adapter, MockConsumer, offset=4)

            result = adapter.ack(msg["receipt_handle"], queue_name="orders")

            assert result is True
            MockConsumer.return_value.commit.assert_called_once()
            (committed,), _ = MockConsumer.return_value.commit.call_args
            tp = TopicPartition("orders", 0)
            assert committed[tp] == OffsetAndMetadata(5, None)

    def test_delete_is_equivalent_to_ack_commit(self, adapter):
        with patch("kafka.KafkaConsumer") as MockConsumer:
            msg = self._received_message(adapter, MockConsumer, offset=7)

            result = adapter.delete(msg["id"], queue_name="orders")

            assert result is True
            MockConsumer.return_value.commit.assert_called_once()

    def test_ack_removes_message_from_pending_so_it_cannot_be_double_committed(self, adapter):
        with patch("kafka.KafkaConsumer") as MockConsumer:
            msg = self._received_message(adapter, MockConsumer, offset=1)

            adapter.ack(msg["receipt_handle"], queue_name="orders")
            second = adapter.ack(msg["receipt_handle"], queue_name="orders")

            assert second is False
            assert MockConsumer.return_value.commit.call_count == 1

    def test_ack_with_empty_id_raises(self, adapter):
        with pytest.raises(QueueError):
            adapter.ack("", queue_name="orders")

    def test_ack_of_unknown_message_id_is_a_harmless_no_op(self, adapter):
        with patch("kafka.KafkaConsumer"):
            assert adapter.ack("99-99", queue_name="orders") is False

    def test_ack_without_a_prior_receive_raises_missing_consumer_error(self):
        """ack() referencing a topic this adapter instance never
        consumed from at all (no consumer ever built) can't have a real
        pending offset -- covered by the no-op path above -- but if
        somehow _pending held an entry for a topic with no live consumer
        object, commit must fail loudly instead of silently doing
        nothing."""
        a = KafkaQueueAdapter(bootstrap_servers="localhost:9092", group_id="g")
        tp = TopicPartition("orphaned-topic", 0)
        a._pending["orphan-id"] = (tp, 1)
        with pytest.raises(QueueError):
            a.ack("orphan-id", queue_name="orphaned-topic")


# ────────────────────────────────────────────────────────────────────────────
# close()
# ────────────────────────────────────────────────────────────────────────────

class TestKafkaQueueAdapterClose:
    def test_close_flushes_producer_and_closes_consumers(self, adapter):
        with patch("kafka.KafkaProducer") as MockProducer, patch(
            "kafka.KafkaConsumer"
        ) as MockConsumer:
            MockProducer.return_value.send.return_value.get.return_value = _record_metadata()
            adapter.send({"a": 1}, queue_name="orders")

            MockConsumer.return_value.poll.return_value = {}
            adapter.receive(queue_name="orders", max_messages=1)

            adapter.close()

            MockProducer.return_value.close.assert_called_once()
            MockConsumer.return_value.close.assert_called_once()

    def test_close_swallows_errors_from_underlying_clients(self, adapter):
        with patch("kafka.KafkaProducer") as MockProducer:
            MockProducer.return_value.send.return_value.get.return_value = _record_metadata()
            adapter.send({"a": 1}, queue_name="orders")
            MockProducer.return_value.close.side_effect = RuntimeError("already closed")

            adapter.close()  # must not raise


# ────────────────────────────────────────────────────────────────────────────
# Wiring through CloudDatabaseFactory.get_queue()
# ────────────────────────────────────────────────────────────────────────────

class TestKafkaFactoryWiring:
    def test_get_queue_returns_kafka_adapter_with_configured_bootstrap_servers(self):
        factory = CloudDatabaseFactory(
            provider=CloudProvider.KAFKA,
            storage_configs=[
                KafkaQueueConfig(
                    name="kafka_queue",
                    bootstrap_servers="broker1:9092,broker2:9092",
                    group_id="workers",
                )
            ],
        )
        adapter = factory.get_queue("kafka_queue")
        assert isinstance(adapter, KafkaQueueAdapter)
        assert adapter.bootstrap_servers == ["broker1:9092", "broker2:9092"]
        assert adapter.group_id == "workers"

    def test_send_receive_ack_through_factory_with_mocked_client(self):
        factory = CloudDatabaseFactory(
            provider=CloudProvider.KAFKA,
            storage_configs=[
                KafkaQueueConfig(name="kafka_queue", bootstrap_servers="localhost:9092")
            ],
        )
        queue = factory.get_queue("kafka_queue")

        with patch("kafka.KafkaProducer") as MockProducer, patch(
            "kafka.KafkaConsumer"
        ) as MockConsumer:
            MockProducer.return_value.send.return_value.get.return_value = _record_metadata(
                0, 0
            )
            msg_id = queue.send({"via": "factory"}, queue_name="orders")
            assert msg_id == "0-0"

            tp = TopicPartition("orders", 0)
            MockConsumer.return_value.poll.return_value = {
                tp: [_consumer_record(offset=0, value=b'{"via": "factory"}')]
            }
            received = queue.receive(queue_name="orders", max_messages=1)
            assert received[0]["body"] == {"via": "factory"}

            assert queue.ack(received[0]["receipt_handle"], queue_name="orders") is True
