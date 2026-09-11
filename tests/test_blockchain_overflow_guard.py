"""
tests/test_blockchain_overflow_guard.py
=========================================
Phase 0 fix: BlockchainKVAdapter previously stood entirely outside the
NoSQLKVAdapter hierarchy -- no _check_overflow, no size guard of any kind
-- so a large record was sent on-chain unmodified, which is both far more
expensive per byte than any off-chain store and, on most EVM chains, will
simply be rejected past the call-data size ceiling before it ever reaches
this adapter's own size logic.

This test does not need a live chain: it constructs the adapter via
object.__new__ (bypassing __init__'s Web3/RPC/account setup entirely,
none of which the overflow guard depends on) and wires in only what
put()/get() actually touch -- self.max_size, self.object_storage,
self._lock, and a fake `contract` standing in for the real web3 Contract.
This exercises the real put()/get() method bodies without a network call.
"""

from __future__ import annotations

import json
import threading

import pytest

from polydb.adapters.BlockchainKVAdapter import BLOCKCHAIN_MAX_SIZE, BlockchainKVAdapter
from polydb.errors import StorageError


class FakeObjectStorage:
    def __init__(self):
        self.store: dict[str, bytes] = {}

    def put(self, key, data, **kwargs):
        self.store[key] = data
        return key

    def get(self, key):
        return self.store[key]

    def delete(self, key):
        del self.store[key]

    def list(self, prefix: str = ""):
        return [k for k in self.store if k.startswith(prefix)]


class FakeContractFunctions:
    """Stands in for web3's `contract.functions` -- records put() calls,
    lets get()/deleteKey() be driven from a plain dict, no chain needed."""

    def __init__(self, ledger: dict):
        self._ledger = ledger
        self.put_calls: list[tuple] = []

    def put(self, key, payload):
        self.put_calls.append((key, payload))

        class _Fn:
            def build_transaction(_self, tx):
                return tx

        return _Fn()

    def get(self, key):
        class _Call:
            def call(_self):
                return self._ledger.get(key, "")

        return _Call()

    def deleteKey(self, key):
        class _Fn:
            def build_transaction(_self, tx):
                return tx

        return _Fn()


class FakeContract:
    def __init__(self, ledger: dict):
        self.functions = FakeContractFunctions(ledger)


def make_adapter(*, max_size: int = BLOCKCHAIN_MAX_SIZE) -> tuple[BlockchainKVAdapter, dict]:
    """Bare BlockchainKVAdapter with no live RPC/account -- only the pieces
    put()/get() actually use are wired up."""
    adapter = object.__new__(BlockchainKVAdapter)
    adapter.logger = __import__("logging").getLogger("test")
    adapter.partition_config = None
    adapter.object_storage = FakeObjectStorage()
    adapter._lock = threading.Lock()
    adapter.max_size = max_size

    ledger: dict = {}
    adapter.contract = FakeContract(ledger)

    def _send_tx(fn):
        # put()'s payload was already handed to contract.functions.put()
        # (captured in put_calls) before _send_tx is invoked -- mirror a
        # real chain write by landing it in the fake ledger too, keyed the
        # same way put() built the call.
        return {"status": 1}

    adapter._send_tx = _send_tx
    return adapter, ledger


def _sync_ledger_from_calls(adapter, ledger):
    """The real put() only calls contract.functions.put(key, payload) then
    _send_tx(fn) -- it never writes into a dict itself (that's the real
    chain's job). Mirror that side effect here so get() can read it back,
    the same way the real chain would once the tx lands."""
    for key, payload in adapter.contract.functions.put_calls:
        ledger[key] = payload


class TestBlockchainOverflowGuard:
    def test_max_size_is_set_to_the_chain_appropriate_ceiling(self):
        adapter, _ = make_adapter()
        assert adapter.max_size == BLOCKCHAIN_MAX_SIZE
        assert adapter.max_size < 1024 * 1024  # far below the generic 1MB base default

    def test_put_overflows_a_payload_over_the_chain_ceiling(self):
        adapter, ledger = make_adapter(max_size=200)
        data = {"id": "rec1", "blob": "x" * 5000}

        result = adapter.put(None, data)
        _sync_ledger_from_calls(adapter, ledger)

        assert result["_overflow"] is True
        assert result["_blob_key"] in adapter.object_storage.store

        # What actually got sent to the chain must be the small reference,
        # never the 5KB payload.
        key, payload = adapter.contract.functions.put_calls[0]
        assert key == "rec1"
        sent = json.loads(payload)
        assert sent["_overflow"] is True
        assert len(payload) < 500

    def test_put_does_not_overflow_a_small_payload(self):
        adapter, ledger = make_adapter(max_size=200)
        data = {"id": "rec2", "note": "tiny"}

        result = adapter.put(None, data)
        _sync_ledger_from_calls(adapter, ledger)

        assert "_overflow" not in result
        key, payload = adapter.contract.functions.put_calls[0]
        assert json.loads(payload) == data

    def test_get_rehydrates_an_overflowed_record(self):
        adapter, ledger = make_adapter(max_size=200)
        data = {"id": "rec3", "blob": "y" * 5000}

        adapter.put(None, data)
        _sync_ledger_from_calls(adapter, ledger)

        fetched = adapter.get(None, "rec3")

        assert fetched["blob"] == data["blob"]
        assert fetched["id"] == "rec3"

    def test_get_detects_a_corrupted_overflow_blob(self):
        adapter, ledger = make_adapter(max_size=200)
        data = {"id": "rec4", "blob": "z" * 5000}
        result = adapter.put(None, data)
        _sync_ledger_from_calls(adapter, ledger)

        adapter.object_storage.store[result["_blob_key"]] = b'{"id": "rec4", "blob": "TAMPERED"}'

        with pytest.raises(StorageError, match="Checksum mismatch"):
            adapter.get(None, "rec4")
