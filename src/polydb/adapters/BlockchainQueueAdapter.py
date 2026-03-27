from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from dotenv import load_dotenv

from ..errors import QueueError

logger = logging.getLogger(__name__)


class BlockchainQueueAdapter:
    """
    Queue implementation using blockchain events.

    Messages are emitted via smart contract events and read via event filters.
    """

    def __init__(
        self,
        rpc_url: Optional[str] = None,
        private_key: Optional[str] = None,
        contract_address: Optional[str] = None,
        contract_abi: Optional[list] = None,
    ):
        load_dotenv()

        self.rpc_url = rpc_url or os.getenv("BLOCKCHAIN_RPC_URL")
        self.private_key = private_key or os.getenv("BLOCKCHAIN_PRIVATE_KEY")
        self.contract_address = contract_address or os.getenv("BLOCKCHAIN_QUEUE_CONTRACT")

        if not self.rpc_url:
            raise RuntimeError("BLOCKCHAIN_RPC_URL not configured")

        self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        self.account = self.w3.eth.account.from_key(self.private_key)

        if contract_abi is None:
            contract_abi = self._default_abi()
        if self.contract_address is not None:
            self.contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.contract_address),
                abi=contract_abi,
            )

    def _default_abi(self):
        return [
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": False, "name": "queue", "type": "string"},
                    {"indexed": False, "name": "data", "type": "string"},
                ],
                "name": "MessagePublished",
                "type": "event",
            },
            {
                "inputs": [
                    {"internalType": "string", "name": "queue", "type": "string"},
                    {"internalType": "string", "name": "data", "type": "string"},
                ],
                "name": "publish",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function",
            },
        ]

    def _send_tx(self, fn):
        nonce = self.w3.eth.get_transaction_count(self.account.address)

        tx = fn.build_transaction(
            {
                "from": self.account.address,
                "nonce": nonce,
                "gas": 300000,
                "gasPrice": self.w3.eth.gas_price,
            }
        )

        signed = self.account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)

        return tx_hash.hex()

    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        payload = json.dumps(message)
        fn = self.contract.functions.publish(queue_name, payload)
        tx_hash = self._send_tx(fn)

        return tx_hash

    def receive(
        self, queue_name: str = "default", max_messages: int = 1, from_block="latest"
    ) -> List[Dict]:
        event_filter = self.contract.events.MessagePublished.create_filter(fromBlock=from_block)

        events = event_filter.get_all_entries()

        messages = []

        for event in events:
            if event.args.queue == queue_name:
                messages.append(json.loads(event.args.data))

        return messages

    def delete(self, message_id: str, queue_name: str = "default", pop_receipt: str = "") -> bool:
        """
        Blockchain queues cannot delete events.
        This method exists for interface compatibility.
        """
        return False

    def ack(self, ack_id: str, queue_name: str = "default") -> bool:
        """
        ACK for Vercel Queue.

        Since Redis Streams via Vercel KV REST API does not support
        explicit ACK without consumer groups, this is treated as no-op.

        Exists for interface consistency.
        """
        if not ack_id:
            raise QueueError("ack_id is required")

        return True
