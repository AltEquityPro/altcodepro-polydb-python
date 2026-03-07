from __future__ import annotations

import logging
import os
from typing import Optional

import ipfshttpclient
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class BlockchainBlobAdapter:
    """
    Blob storage backed by IPFS.

    Returns CID hashes which can optionally be stored on blockchain.
    """

    def __init__(self, ipfs_url: Optional[str] = None):
        load_dotenv()

        self.ipfs_url = ipfs_url or os.getenv("IPFS_API_URL", "/dns/localhost/tcp/5001/http")

        try:
            import ipfshttpclient.client
            ipfshttpclient.client.assert_version = lambda *args, **kwargs: None
            self.client = ipfshttpclient.connect(self.ipfs_url, session=True)
        except Exception as e:
            raise RuntimeError(f"Failed to connect to IPFS: {e}")

    def put(self, key: str, data: bytes) -> str:
        cid = self.client.add_bytes(data)
        return cid

    def get(self, cid: str) -> bytes:
        return self.client.cat(cid)

    def delete(self, cid: str):
        """
        IPFS is immutable.
        Deletion is logical only.
        """
        return {"cid": cid, "deleted": True}
