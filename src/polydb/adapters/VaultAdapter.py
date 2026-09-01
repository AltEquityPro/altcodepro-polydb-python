# src/polydb/adapters/VaultAdapter.py
#
# HashiCorp Vault (OSS, self-hosted) -- the cloud-agnostic default secrets
# backend, alongside the native per-cloud adapters (Azure Key Vault, AWS
# Secrets Manager, GCP Secret Manager).
import os
from typing import Optional

from ..base.SecretsAdapter import SecretsAdapter
from ..errors import ConnectionError
from ..retry import retry


class VaultAdapter(SecretsAdapter):
    def __init__(
        self,
        url: str = "",
        token: str = "",
        mount_point: str = "secret",
    ):
        super().__init__()

        self.url = url or os.getenv("VAULT_ADDR")
        self.token = token or os.getenv("VAULT_TOKEN")
        self.mount_point = mount_point

        if not self.url or not self.token:
            raise ConnectionError("VAULT_ADDR / VAULT_TOKEN are not configured")

        self._client = None
        self._initialize_client()

    def _initialize_client(self) -> None:
        import hvac

        self._client = hvac.Client(url=self.url, token=self.token)
        if not self._client.is_authenticated():
            raise ConnectionError("Vault authentication failed")

    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, Exception))
    def get_secret(self, key: str) -> Optional[str]:
        try:
            resp = self._client.secrets.kv.v2.read_secret_version(
                path=key, mount_point=self.mount_point, raise_on_deleted_version=True
            )
        except Exception as exc:
            if "InvalidPath" in type(exc).__name__ or "not found" in str(exc).lower():
                return None
            raise
        return resp["data"]["data"].get("value")

    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, Exception))
    def set_secret(self, key: str, value: str) -> None:
        self._client.secrets.kv.v2.create_or_update_secret(
            path=key, secret={"value": value}, mount_point=self.mount_point
        )

    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, Exception))
    def delete_secret(self, key: str) -> bool:
        self._client.secrets.kv.v2.delete_metadata_and_all_versions(
            path=key, mount_point=self.mount_point
        )
        return True

    @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError, Exception))
    def list_secrets(self, prefix: str = "") -> list[str]:
        try:
            resp = self._client.secrets.kv.v2.list_secrets(
                path=prefix, mount_point=self.mount_point
            )
        except Exception as exc:
            if "InvalidPath" in type(exc).__name__ or "not found" in str(exc).lower():
                return []
            raise
        return resp["data"]["keys"]
