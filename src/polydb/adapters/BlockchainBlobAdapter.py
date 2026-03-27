from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import ipfshttpclient
from dotenv import load_dotenv

from ..base.ObjectStorageAdapter import ObjectStorageAdapter
from ..errors import StorageError


class BlockchainBlobAdapter(ObjectStorageAdapter):
    """
    Blob storage backed by IPFS.

    - key is ignored for storage (IPFS is content-addressed)
    - returns CID (acts as key)
    - metadata is not natively supported → ignored or embedded externally
    """

    def __init__(self, ipfs_url: Optional[str] = None):
        super().__init__()

        load_dotenv()

        self.ipfs_url = ipfs_url or os.getenv("IPFS_API_URL", "/dns/localhost/tcp/5001/http")

        try:
            import ipfshttpclient.client

            ipfshttpclient.client.assert_version = lambda *args, **kwargs: None

            self.client = ipfshttpclient.connect(self.ipfs_url, session=True)
            self.logger.info(f"Connected to IPFS: {self.ipfs_url}")

        except Exception as e:
            raise StorageError(f"Failed to connect to IPFS: {e}")

    # --------------------------------------------------
    # PUT
    # --------------------------------------------------
    def _put_raw(
        self,
        key: str,
        data: bytes,
        fileName: str = "",
        media_type: Optional[str] = None,
        metadata: Dict[str, Any] | None = None,
    ) -> str:
        """Upload data to IPFS and return CID"""
        try:
            res = self.client.add_bytes(data)  # returns CID
            cid = res

            self.logger.debug(f"IPFS upload success cid={cid}")

            return cid

        except Exception as e:
            raise StorageError(f"IPFS put failed: {e}")

    # --------------------------------------------------
    # GET
    # --------------------------------------------------
    def get(self, key: str) -> bytes:
        """Fetch blob from IPFS using CID"""
        try:
            data = self.client.cat(key)
            self.logger.debug(f"IPFS get success cid={key}")
            return data

        except Exception as e:
            raise StorageError(f"IPFS get failed: {e}")

    # --------------------------------------------------
    # DELETE
    # --------------------------------------------------
    def delete(self, key: str) -> bool:
        """
        IPFS does not support true delete.
        We can unpin to allow GC.
        """
        try:
            self.client.pin.rm(key)
            self.logger.debug(f"IPFS unpinned cid={key}")
            return True

        except Exception:
            return False

    # --------------------------------------------------
    # LIST
    # --------------------------------------------------
    def list(self, prefix: str = "") -> List[str]:
        """
        IPFS has no native listing by prefix.
        Return pinned CIDs as approximation.
        """
        try:
            pins = self.client.pin.ls(type="all")
            cids = list(pins.get("Keys", {}).keys())

            if prefix:
                cids = [cid for cid in cids if cid.startswith(prefix)]

            self.logger.debug(f"IPFS list returned {len(cids)} items")
            return cids

        except Exception as e:
            raise StorageError(f"IPFS list failed: {e}")
