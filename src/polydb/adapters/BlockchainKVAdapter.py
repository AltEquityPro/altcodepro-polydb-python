from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

SUPPORTED_CHAINS = {
    "ethereum",
    "polygon",
    "avalanche",
    "bnb",
    "arbitrum",
}


class BlockchainKVAdapter:
    """
    Generic blockchain key-value adapter.

    Uses a smart contract to store JSON documents keyed by ID.

    Supports EVM-compatible chains:
        - Ethereum
        - Polygon
        - Avalanche
        - BNB Chain
        - Arbitrum
    """

    def __init__(
        self,
        chain: Optional[str] = None,
        rpc_url: Optional[str] = None,
        private_key: Optional[str] = None,
        contract_address: Optional[str] = None,
        contract_abi: Optional[list] = None,
    ):
        load_dotenv()

        self.chain = (chain or os.getenv("BLOCKCHAIN_CHAIN", "ethereum")).lower()

        if self.chain not in SUPPORTED_CHAINS:
            raise ValueError(f"Unsupported blockchain: {self.chain}")

        self.rpc_url = rpc_url or os.getenv("BLOCKCHAIN_RPC_URL")
        self.private_key = private_key or os.getenv("BLOCKCHAIN_PRIVATE_KEY")
        self.contract_address = contract_address or os.getenv("BLOCKCHAIN_CONTRACT")

        if not self.rpc_url:
            raise RuntimeError("BLOCKCHAIN_RPC_URL not configured")

        if not self.private_key:
            raise RuntimeError("BLOCKCHAIN_PRIVATE_KEY not configured")

        if not self.contract_address:
            raise RuntimeError("BLOCKCHAIN_CONTRACT not configured")

        self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))

        if self.chain in {"polygon", "avalanche", "bnb"}:
            self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        self.account = self.w3.eth.account.from_key(self.private_key)

        if contract_abi is None:
            contract_abi = self._default_abi()

        self.contract = self.w3.eth.contract(
            address=Web3.to_checksum_address(self.contract_address),
            abi=contract_abi,
        )

    def _default_abi(self):
        return [
            {
                "inputs": [
                    {"internalType": "string", "name": "key", "type": "string"},
                    {"internalType": "string", "name": "value", "type": "string"},
                ],
                "name": "put",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function",
            },
            {
                "inputs": [{"internalType": "string", "name": "key", "type": "string"}],
                "name": "get",
                "outputs": [{"internalType": "string", "name": "", "type": "string"}],
                "stateMutability": "view",
                "type": "function",
            },
            {
                "inputs": [{"internalType": "string", "name": "key", "type": "string"}],
                "name": "deleteKey",
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
                "gas": 500000,
                "gasPrice": self.w3.eth.gas_price,
            }
        )

        signed = self.account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)

        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    def put(self, model, data: Dict[str, Any]) -> Dict[str, Any]:
        key = str(data["id"])
        payload = json.dumps(data)

        fn = self.contract.functions.put(key, payload)
        self._send_tx(fn)

        return data

    def get(self, model, key: str) -> Optional[Dict[str, Any]]:
        result = self.contract.functions.get(str(key)).call()

        if not result:
            return None

        return json.loads(result)

    def delete(self, model, key: str):
        fn = self.contract.functions.deleteKey(str(key))
        self._send_tx(fn)

        return {"id": key}

    def query(self, model, query: Dict[str, Any], limit: Optional[int] = None):
        raise NotImplementedError(
            "Blockchain query requires an off-chain indexer (TheGraph / Elastic)"
        )