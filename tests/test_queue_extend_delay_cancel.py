"""
tests/test_queue_extend_delay_cancel.py
========================================
Mock-only (no live SQS/Azure/GCP/RabbitMQ) coverage for the real,
per-backend `extend`/`delay`/`cancel` queue operations, plus
`databaseFactory.py`'s own `QUEUE_RECEIVE_MAX_MESSAGES_CAP` clamp and
`extend_queue`/`delay_queue`/`cancel_queue` wrapper methods.

Each adapter only implements the operations its own real backend
genuinely supports (see QueueAdapter.py's own module comment for the
full feasibility matrix) -- these tests prove the real ones actually
call the right underlying SDK method with the right arguments, and that
the base class's own `NotImplementedError` still fires for whichever
operation a given backend doesn't support.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

for _name in list(sys.modules):
    if _name == "azure" or _name.startswith("azure."):
        del sys.modules[_name]

from polydb.adapters.AzureQueueAdapter import AzureQueueAdapter
from polydb.adapters.GCPPubSubAdapter import GCPPubSubAdapter
from polydb.adapters.RabbitMQAdapter import RabbitMQAdapter
from polydb.adapters.SQSAdapter import SQSAdapter
from polydb.base.QueueAdapter import QueueAdapter
from polydb.errors import QueueError

# ──────────────────────────────────────────────────────────────────────
# Base class: unsupported operations still raise NotImplementedError
# ──────────────────────────────────────────────────────────────────────


class TestBaseQueueAdapterUnsupportedOps:
    def test_extend_not_implemented_by_default(self):
        class _Bare(QueueAdapter):
            def send(self, *a, **k): ...
            def receive(self, *a, **k): ...
            def delete(self, *a, **k): ...

        with pytest.raises(NotImplementedError):
            _Bare().extend("ack-id")

    def test_delay_not_implemented_by_default(self):
        class _Bare(QueueAdapter):
            def send(self, *a, **k): ...
            def receive(self, *a, **k): ...
            def delete(self, *a, **k): ...

        with pytest.raises(NotImplementedError):
            _Bare().delay({"hello": "world"})

    def test_cancel_not_implemented_by_default(self):
        class _Bare(QueueAdapter):
            def send(self, *a, **k): ...
            def receive(self, *a, **k): ...
            def delete(self, *a, **k): ...

        with pytest.raises(NotImplementedError):
            _Bare().cancel("msg-id")


# ──────────────────────────────────────────────────────────────────────
# SQS: extend + delay real; cancel unsupported
# ──────────────────────────────────────────────────────────────────────


def _make_sqs_adapter() -> SQSAdapter:
    with patch.object(SQSAdapter, "_initialize_client", lambda self: None):
        adapter = SQSAdapter(queue_name="test-queue")
    adapter._client = MagicMock()
    adapter._queue_url = "https://sqs.example.com/123/test-queue"
    return adapter


class TestSQSExtendDelayCancel:
    def test_extend_calls_change_message_visibility(self):
        adapter = _make_sqs_adapter()
        result = adapter.extend("receipt-handle-1", "default", visibility_timeout=45)
        assert result is True
        adapter._client.change_message_visibility.assert_called_once_with(
            QueueUrl=adapter._queue_url,
            ReceiptHandle="receipt-handle-1",
            VisibilityTimeout=45,
        )

    def test_extend_requires_ack_id(self):
        adapter = _make_sqs_adapter()
        with pytest.raises(QueueError):
            adapter.extend("", "default")

    def test_delay_calls_send_message_with_delay_seconds(self):
        adapter = _make_sqs_adapter()
        adapter._client.send_message.return_value = {"MessageId": "msg-123"}
        message_id = adapter.delay({"hello": "world"}, "default", delay_seconds=120)
        assert message_id == "msg-123"
        _, kwargs = adapter._client.send_message.call_args
        assert kwargs["DelaySeconds"] == 120
        assert kwargs["QueueUrl"] == adapter._queue_url

    def test_delay_rejects_out_of_range_delay_seconds(self):
        adapter = _make_sqs_adapter()
        with pytest.raises(QueueError):
            adapter.delay({"a": 1}, "default", delay_seconds=901)
        with pytest.raises(QueueError):
            adapter.delay({"a": 1}, "default", delay_seconds=-1)

    def test_cancel_is_not_implemented(self):
        adapter = _make_sqs_adapter()
        with pytest.raises(NotImplementedError):
            adapter.cancel("msg-id")


# ──────────────────────────────────────────────────────────────────────
# Azure Queue: all three real
# ──────────────────────────────────────────────────────────────────────


def _make_azure_adapter() -> AzureQueueAdapter:
    with patch.object(AzureQueueAdapter, "_initialize_client", lambda self: None):
        adapter = AzureQueueAdapter(connection_string="UseDevelopmentStorage=true")
    adapter._client = MagicMock()
    return adapter


def _mock_azure_queue_client(adapter: AzureQueueAdapter, queue_name: str = "default") -> MagicMock:
    client = MagicMock()
    adapter._queues[adapter._normalize_queue_name(queue_name)] = client
    return client


class TestAzureExtendDelayCancel:
    def test_extend_calls_update_message(self):
        adapter = _make_azure_adapter()
        client = _mock_azure_queue_client(adapter)
        receipt_handle = AzureQueueAdapter._encode_receipt("msg-123", "pop-abc")

        result = adapter.extend(receipt_handle, "default", visibility_timeout=60)

        assert result is True
        client.update_message.assert_called_once_with(
            "msg-123", pop_receipt="pop-abc", visibility_timeout=60
        )

    def test_extend_raises_for_a_malformed_receipt(self):
        adapter = _make_azure_adapter()
        _mock_azure_queue_client(adapter)
        with pytest.raises(QueueError):
            adapter.extend("", "default")

    def test_delay_calls_send_message_with_visibility_timeout_and_returns_receipt(self):
        adapter = _make_azure_adapter()
        client = _mock_azure_queue_client(adapter)
        response = MagicMock()
        response.id = "msg-456"
        response.pop_receipt = "pop-def"
        client.send_message.return_value = response

        receipt = adapter.delay({"hello": "world"}, "default", delay_seconds=90)

        _, kwargs = client.send_message.call_args
        assert kwargs["visibility_timeout"] == 90
        message_id, pop_receipt = AzureQueueAdapter._decode_receipt(receipt)
        assert message_id == "msg-456"
        assert pop_receipt == "pop-def"

    def test_cancel_deletes_the_delayed_message(self):
        adapter = _make_azure_adapter()
        client = _mock_azure_queue_client(adapter)
        receipt = AzureQueueAdapter._encode_receipt("msg-789", "pop-ghi")

        result = adapter.cancel(receipt, "default")

        assert result is True
        client.delete_message.assert_called_once_with("msg-789", "pop-ghi")

    def test_cancel_raises_for_a_malformed_receipt(self):
        adapter = _make_azure_adapter()
        _mock_azure_queue_client(adapter)
        with pytest.raises(QueueError):
            adapter.cancel("", "default")


# ──────────────────────────────────────────────────────────────────────
# GCP Pub/Sub: extend only, capped at the real 600s ack-deadline ceiling
# ──────────────────────────────────────────────────────────────────────


def _make_gcp_adapter() -> GCPPubSubAdapter:
    with patch.object(GCPPubSubAdapter, "_initialize_clients", lambda self: None):
        adapter = GCPPubSubAdapter(project_id="test-project")
    adapter._subscriber = MagicMock()
    adapter._publisher = MagicMock()
    return adapter


class TestGCPExtendOnly:
    def test_extend_calls_modify_ack_deadline(self):
        adapter = _make_gcp_adapter()
        result = adapter.extend("ack-id-1", "default", visibility_timeout=120)
        assert result is True
        adapter._subscriber.modify_ack_deadline.assert_called_once()
        _, kwargs = adapter._subscriber.modify_ack_deadline.call_args
        request = kwargs["request"]
        assert request["ack_ids"] == ["ack-id-1"]
        assert request["ack_deadline_seconds"] == 120

    def test_extend_rejects_a_deadline_past_the_real_max(self):
        adapter = _make_gcp_adapter()
        with pytest.raises(QueueError):
            adapter.extend("ack-id-1", "default", visibility_timeout=601)

    def test_extend_requires_ack_id(self):
        adapter = _make_gcp_adapter()
        with pytest.raises(QueueError):
            adapter.extend("", "default")

    def test_delay_is_not_implemented(self):
        adapter = _make_gcp_adapter()
        with pytest.raises(NotImplementedError):
            adapter.delay({"a": 1}, "default")

    def test_cancel_is_not_implemented(self):
        adapter = _make_gcp_adapter()
        with pytest.raises(NotImplementedError):
            adapter.cancel("msg-id")


# ──────────────────────────────────────────────────────────────────────
# RabbitMQ: delay + cancel real via TTL+DLX; extend unsupported
# ──────────────────────────────────────────────────────────────────────


def _make_rabbitmq_adapter() -> RabbitMQAdapter:
    with patch.object(RabbitMQAdapter, "_initialize_connection", lambda self: None):
        adapter = RabbitMQAdapter(url="amqp://guest:guest@localhost:5672/")
    adapter._connection = MagicMock()
    adapter._connection.is_open = True
    adapter._connection.is_closed = False
    adapter._channel = MagicMock()
    adapter._channel.is_open = True
    adapter._channel.is_closed = False
    return adapter


class TestRabbitMQExtendDelayCancel:
    def test_extend_is_not_implemented(self):
        adapter = _make_rabbitmq_adapter()
        with pytest.raises(NotImplementedError):
            adapter.extend("ack-id")

    def test_delay_declares_a_ttl_dlx_queue_and_publishes_into_it(self):
        adapter = _make_rabbitmq_adapter()

        message_id = adapter.delay({"hello": "world"}, "default", delay_seconds=30)

        assert message_id
        # A delay queue was declared with the real TTL + dead-letter args.
        declare_calls = adapter._channel.queue_declare.call_args_list
        assert any(
            call.kwargs.get("arguments", {}).get("x-message-ttl") == 30_000
            for call in declare_calls
        )
        adapter._channel.basic_publish.assert_called_once()

    def test_cancel_acks_the_matching_message_and_nacks_the_rest(self):
        adapter = _make_rabbitmq_adapter()
        target_id = "target-msg-id"

        import json as _json

        properties_match = MagicMock()
        properties_match.message_id = target_id
        properties_other = MagicMock()
        properties_other.message_id = "some-other-id"

        method_match = MagicMock(delivery_tag=1)
        method_other = MagicMock(delivery_tag=2)

        adapter._channel.basic_get.side_effect = [
            (method_other, properties_other, _json.dumps({"a": 1}).encode()),
            (method_match, properties_match, _json.dumps({"b": 2}).encode()),
            (None, None, None),
        ]

        result = adapter.cancel(target_id, "default", delay_seconds=30)

        assert result is True
        adapter._channel.basic_ack.assert_called_once_with(delivery_tag=1)
        adapter._channel.basic_nack.assert_called_once_with(delivery_tag=2, requeue=True)

    def test_cancel_requires_delay_seconds(self):
        adapter = _make_rabbitmq_adapter()
        with pytest.raises(QueueError):
            adapter.cancel("some-id", "default")


# ──────────────────────────────────────────────────────────────────────
# databaseFactory.py: receive cap + extend_queue/delay_queue/cancel_queue
# ──────────────────────────────────────────────────────────────────────


class TestDatabaseFactoryQueueCapAndWrappers:
    def _make_factory_with_fake_queue(self):
        from polydb.databaseFactory import DatabaseFactory

        fake_queue = MagicMock()
        fake_cloud_factory = MagicMock()
        fake_cloud_factory.get_queue.return_value = fake_queue

        fake_engine = MagicMock()
        fake_engine.cloud_factory = fake_cloud_factory

        factory = DatabaseFactory.__new__(DatabaseFactory)
        factory._engines = [fake_engine]
        return factory, fake_queue

    def test_receive_queue_clamps_a_request_above_the_configured_cap(self, monkeypatch):
        from polydb import databaseFactory as db_factory_module

        monkeypatch.setattr(db_factory_module, "QUEUE_RECEIVE_MAX_MESSAGES_CAP", 5)
        factory, fake_queue = self._make_factory_with_fake_queue()
        fake_queue.receive.return_value = []

        factory.receive_queue(max_messages=1000)

        _, kwargs = fake_queue.receive.call_args
        assert kwargs["max_messages"] == 5

    def test_receive_queue_never_raises_the_cap_for_a_smaller_request(self):
        factory, fake_queue = self._make_factory_with_fake_queue()
        fake_queue.receive.return_value = []

        factory.receive_queue(max_messages=10)

        _, kwargs = fake_queue.receive.call_args
        assert kwargs["max_messages"] == 10

    def test_extend_queue_delegates_to_the_real_adapter(self):
        factory, fake_queue = self._make_factory_with_fake_queue()
        fake_queue.extend.return_value = True

        result = factory.extend_queue("ack-id", queue_name="q1", visibility_timeout=45)

        assert result is True
        fake_queue.extend.assert_called_once_with("ack-id", "q1", visibility_timeout=45)

    def test_delay_queue_delegates_to_the_real_adapter(self):
        factory, fake_queue = self._make_factory_with_fake_queue()
        fake_queue.delay.return_value = "delayed-msg-id"

        result = factory.delay_queue({"a": 1}, queue_name="q1", delay_seconds=30)

        assert result == "delayed-msg-id"
        fake_queue.delay.assert_called_once_with({"a": 1}, "q1", delay_seconds=30)

    def test_cancel_queue_delegates_to_the_real_adapter(self):
        factory, fake_queue = self._make_factory_with_fake_queue()
        fake_queue.cancel.return_value = True

        result = factory.cancel_queue("msg-id", queue_name="q1")

        assert result is True
        fake_queue.cancel.assert_called_once_with("msg-id", "q1")

    def test_cancel_queue_forwards_rabbitmq_style_delay_seconds_kwarg(self):
        factory, fake_queue = self._make_factory_with_fake_queue()
        fake_queue.cancel.return_value = True

        factory.cancel_queue("msg-id", queue_name="q1", delay_seconds=30)

        fake_queue.cancel.assert_called_once_with("msg-id", "q1", delay_seconds=30)
