# src/polydb/adapters/BlockchainFileAdapter.py

from __future__ import annotations

import os
from typing import List, Optional

from dotenv import load_dotenv

from ..base.SharedFilesAdapter import SharedFilesAdapter
from ..errors import StorageError, ConnectionError

SUPPORTED_CHAINS = {"ethereum", "polygon", "avalanche", "bnb", "arbitrum"}


class BlockchainFileAdapter(SharedFilesAdapter):
    """
    Blockchain-backed shared file storage.

    Strategy:
      - file BYTES are stored on IPFS (content-addressed -> CID)
      - the PATH -> CID mapping is kept in an on-chain registry contract
      - per-call `share_name` namespaces the path on-chain

    Fully supported: write / read / delete.
    Partially supported: list (on-chain listFiles(prefix), with an event-replay
    fallback — true cheap enumeration needs an off-chain indexer like TheGraph).

    EVM chains: ethereum, polygon, avalanche, bnb, arbitrum.
    """

    def __init__(
        self,
        chain: Optional[str] = None,
        rpc_url: Optional[str] = None,
        private_key: Optional[str] = None,
        contract_address: Optional[str] = None,
        contract_abi: Optional[list] = None,
        ipfs_url: Optional[str] = None,
    ):
        super().__init__()
        load_dotenv()

        self.chain = (chain or os.getenv("BLOCKCHAIN_CHAIN", "ethereum")).lower()
        if self.chain not in SUPPORTED_CHAINS:
            raise ValueError(f"Unsupported blockchain: {self.chain}")

        self.rpc_url = rpc_url or os.getenv("BLOCKCHAIN_RPC_URL")
        self.private_key = private_key or os.getenv("BLOCKCHAIN_PRIVATE_KEY")
        self.contract_address = (
            contract_address
            or os.getenv("BLOCKCHAIN_FILE_CONTRACT")
            or os.getenv("BLOCKCHAIN_CONTRACT")
        )
        self.ipfs_url = (ipfs_url or os.getenv("IPFS_API_URL", "http://localhost:5001")).rstrip("/")

        if not self.rpc_url:
            raise ConnectionError("BLOCKCHAIN_RPC_URL not configured")
        if not self.private_key:
            raise ConnectionError("BLOCKCHAIN_PRIVATE_KEY not configured")
        if not self.contract_address:
            raise ConnectionError("BLOCKCHAIN_FILE_CONTRACT not configured")

        from web3 import Web3
        from web3.middleware import ExtraDataToPOAMiddleware

        self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))
        if self.chain in {"polygon", "avalanche", "bnb"}:
            self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        self.account = self.w3.eth.account.from_key(self.private_key)
        self.contract = self.w3.eth.contract(
            address=Web3.to_checksum_address(self.contract_address),
            abi=contract_abi or self._default_abi(),
        )
        self.logger.info(
            f"Blockchain file adapter ready (chain={self.chain}, ipfs={self.ipfs_url})"
        )

    # ------------------------------------------------------------------
    # Contract ABI + tx helpers
    # ------------------------------------------------------------------
    def _default_abi(self):
        return [
            {
                "inputs": [{"name": "path", "type": "string"}, {"name": "cid", "type": "string"}],
                "name": "putFile",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function",
            },
            {
                "inputs": [{"name": "path", "type": "string"}],
                "name": "getFile",
                "outputs": [{"name": "", "type": "string"}],
                "stateMutability": "view",
                "type": "function",
            },
            {
                "inputs": [{"name": "path", "type": "string"}],
                "name": "deleteFile",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function",
            },
            {
                "inputs": [{"name": "prefix", "type": "string"}],
                "name": "listFiles",
                "outputs": [{"name": "", "type": "string[]"}],
                "stateMutability": "view",
                "type": "function",
            },
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": False, "name": "path", "type": "string"},
                    {"indexed": False, "name": "cid", "type": "string"},
                ],
                "name": "FileStored",
                "type": "event",
            },
        ]

    def _send_tx(self, fn):
        nonce = self.w3.eth.get_transaction_count(self.account.address)
        try:
            gas = int(fn.estimate_gas({"from": self.account.address}) * 1.2)
        except Exception:
            gas = 500000
        tx = fn.build_transaction(
            {
                "from": self.account.address,
                "nonce": nonce,
                "gas": gas,
                "gasPrice": self.w3.eth.gas_price,
            }
        )
        signed = self.account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
        return self.w3.eth.wait_for_transaction_receipt(tx_hash)

    @staticmethod
    def _key(path: str, share_name: Optional[str] = None) -> str:
        path = (path or "").lstrip("/")
        if share_name:
            return f"{share_name.strip('/')}/{path}".rstrip("/")
        return path

    # ------------------------------------------------------------------
    # IPFS bytes layer
    # ------------------------------------------------------------------
    def _ipfs_add(self, data: bytes) -> str:
        import requests

        resp = requests.post(f"{self.ipfs_url}/api/v0/add", files={"file": data}, timeout=60)
        resp.raise_for_status()
        return resp.json()["Hash"]

    def _ipfs_cat(self, cid: str) -> bytes:
        import requests

        resp = requests.post(f"{self.ipfs_url}/api/v0/cat", params={"arg": cid}, timeout=60)
        resp.raise_for_status()
        return resp.content

    # ------------------------------------------------------------------
    # SharedFilesAdapter interface
    # ------------------------------------------------------------------
    def write(self, path: str, data: bytes, share_name: Optional[str] = None) -> bool:
        try:
            cid = self._ipfs_add(data)
            self._send_tx(self.contract.functions.putFile(self._key(path, share_name), cid))
            self.logger.debug(f"Blockchain file stored path={path} cid={cid}")
            return True
        except Exception as e:
            raise StorageError(f"Blockchain file write failed: {str(e)}")

    def read(self, path: str, share_name: Optional[str] = None) -> bytes | None:
        try:
            cid = self.contract.functions.getFile(self._key(path, share_name)).call()
            if not cid:
                return None
            return self._ipfs_cat(cid)
        except Exception as e:
            raise StorageError(f"Blockchain file read failed: {str(e)}")

    def delete(self, path: str, share_name: Optional[str] = None) -> bool:
        try:
            key = self._key(path, share_name)
            if not self.contract.functions.getFile(key).call():
                return False  # nothing mapped
            self._send_tx(self.contract.functions.deleteFile(key))
            return True
        except Exception as e:
            raise StorageError(f"Blockchain file delete failed: {str(e)}")

    def list(self, directory: str = "", share_name: Optional[str] = None) -> List[str]:
        prefix = self._key(directory, share_name)

        # 1) native on-chain enumeration if the contract supports it
        try:
            keys = self.contract.functions.listFiles(prefix).call()
            return list(keys)
        except Exception:
            pass

        # 2) fallback: replay FileStored events and reconstruct live keys
        try:
            evt = self.contract.events.FileStored.create_filter(fromBlock=0)
            keys = {}
            for e in evt.get_all_entries():
                p = e["args"]["path"]
                if p.startswith(prefix):
                    keys[p] = e["args"]["cid"]
            return list(keys.keys())
        except Exception as e:
            raise StorageError(f"Blockchain file list failed: {str(e)}")
