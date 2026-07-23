"""
tests/test_azure_queue_receipt_handle.py
=========================================
Regression coverage for the receipt_handle contract on AzureQueueAdapter.

Bug this guards against: `receive()` used to return only `id` and
`pop_receipt`, never the `receipt_handle` key that WorkerPool (and every
other adapter's ack contract) reads. That made
`WorkerPool._process_message`'s `msg.get("receipt_handle")` always `None`,
so the ack branch was silently skipped, the message was never deleted, and
Azure redelivered it once its visibility timeout elapsed — replaying the
same task forever even after it had already run to completion.

These tests mock the Azure SDK entirely (no live Azurite needed) and only
exercise the adapter's own encode/decode + ack/nack wiring.
"""

from __future__ import annotations

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

# test_atomic_decrement.py stubs sys.modules["azure"] et al. with bare,
# path-less ModuleType objects (guarded by `if _mod not in sys.modules`) so
# it can run without cloud SDKs installed. If it collects before this file
# (alphabetically it does), that stub wins the module cache for the rest of
# the process, and the adapter's lazy `from azure.core.exceptions import
# ...` imports then fail with "'azure' is not a package" -- even though the
# real azure SDK is installed here. Drop any such stubs so this file always
# imports the genuine package fresh.
for _name in list(sys.modules):
    if _name == "azure" or _name.startswith("azure."):
        del sys.modules[_name]

from polydb.adapters.AzureQueueAdapter import AzureQueueAdapter


def _make_adapter() -> AzureQueueAdapter:
    # Bypass the real Azure SDK entirely -- when the full suite runs
    # together, other providers' tests leave azure.storage.queue in a state
    # where importing QueueServiceClient from it fails (a pre-existing
    # cross-test import-cache issue, unrelated to this adapter). Skipping
    # `_initialize_client` sidesteps that import altogether; these tests
    # only care about the adapter's own encode/decode + ack/nack wiring.
    with patch.object(AzureQueueAdapter, "_initialize_client", lambda self: None):
        adapter = AzureQueueAdapter(connection_string="UseDevelopmentStorage=true")
    adapter._client = MagicMock()
    return adapter


def _mock_queue_client(adapter: AzureQueueAdapter, queue_name: str = "default") -> MagicMock:
    client = MagicMock()
    adapter._queues[adapter._normalize_queue_name(queue_name)] = client
    return client


class TestReceiveReturnsReceiptHandle:
    def test_receive_includes_receipt_handle_key(self):
        adapter = _make_adapter()
        client = _mock_queue_client(adapter)

        raw_msg = MagicMock()
        raw_msg.id = "msg-123"
        raw_msg.pop_receipt = "pop-abc"
        raw_msg.content = json.dumps({"hello": "world"})
        client.receive_messages.return_value = [raw_msg]

        [received] = adapter.receive(queue_name="default", max_messages=1)

        assert received["id"] == "msg-123"
        assert received["pop_receipt"] == "pop-abc"
        assert "receipt_handle" in received
        assert received["receipt_handle"]  # truthy -- WorkerPool checks `if receipt_handle`

    def test_receipt_handle_round_trips_through_decode(self):
        message_id, pop_receipt = AzureQueueAdapter._decode_receipt(
            AzureQueueAdapter._encode_receipt("msg-123", "pop-abc")
        )
        assert message_id == "msg-123"
        assert pop_receipt == "pop-abc"


class TestAckAcceptsSingleReceiptHandle:
    def test_ack_with_generic_single_arg_deletes_using_decoded_parts(self):
        """Mirrors how WorkerPool/storage_router actually call ack():
        queue.ack(receipt_handle, queue_name) -- a single positional value,
        never a separate message_id.
        """
        adapter = _make_adapter()
        client = _mock_queue_client(adapter)

        receipt_handle = AzureQueueAdapter._encode_receipt("msg-123", "pop-abc")
        result = adapter.ack(receipt_handle, "default")

        assert result is True
        client.delete_message.assert_called_once_with("msg-123", "pop-abc")

    def test_ack_still_supports_explicit_message_id(self):
        adapter = _make_adapter()
        client = _mock_queue_client(adapter)

        result = adapter.ack("pop-abc", "default", message_id="msg-123")

        assert result is True
        client.delete_message.assert_called_once_with("msg-123", "pop-abc")

    def test_ack_raises_if_receipt_handle_has_no_message_id(self):
        adapter = _make_adapter()
        _mock_queue_client(adapter)

        with pytest.raises(Exception):
            adapter.ack("", "default")


class TestNack:
    def test_nack_updates_visibility_without_deleting(self):
        adapter = _make_adapter()
        client = _mock_queue_client(adapter)

        receipt_handle = AzureQueueAdapter._encode_receipt("msg-123", "pop-abc")
        result = adapter.nack("default", receipt_handle, delay=30)

        assert result is True
        client.update_message.assert_called_once_with(
            "msg-123", pop_receipt="pop-abc", visibility_timeout=30
        )
        client.delete_message.assert_not_called()

    def test_nack_defaults_delay_to_zero(self):
        adapter = _make_adapter()
        client = _mock_queue_client(adapter)

        receipt_handle = AzureQueueAdapter._encode_receipt("msg-123", "pop-abc")
        adapter.nack("default", receipt_handle)

        client.update_message.assert_called_once_with(
            "msg-123", pop_receipt="pop-abc", visibility_timeout=0
        )
