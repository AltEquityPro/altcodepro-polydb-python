# src/polydb/adapters/AzureKeyVaultAdapter.py
import os
from typing import Optional

from ..base.SecretsAdapter import SecretsAdapter
from ..errors import ConnectionError
from ..retry import retry


class AzureKeyVaultAdapter(SecretsAdapter):
    def __init__(self, vault_url: str = ""):
        super().__init__()

        self.vault_url = vault_url or os.getenv("AZURE_KEY_VAULT_URL")
        if not self.vault_url:
            raise ConnectionError("AZURE_KEY_VAULT_URL is not configured")

        self._client = None
        self._initialize_client()

    def _initialize_client(self) -> None:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient

        self._client = SecretClient(vault_url=self.vault_url, credential=DefaultAzureCredential())

    # Key Vault secret names may only contain alphanumerics and dashes --
    # our secret keys (e.g. "tenant-42/stripe/api_key") use slashes, so
    # translate to a Key-Vault-safe name deterministically both ways.
    @staticmethod
    def _kv_name(key: str) -> str:
        return key.replace("/", "--")

    @retry(max_attempts=3, delay=1.0, exceptions=(Exception,))
    def get_secret(self, key: str) -> Optional[str]:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            return self._client.get_secret(self._kv_name(key)).value
        except ResourceNotFoundError:
            return None

    @retry(max_attempts=3, delay=1.0, exceptions=(Exception,))
    def set_secret(self, key: str, value: str) -> None:
        self._client.set_secret(self._kv_name(key), value)

    @retry(max_attempts=3, delay=1.0, exceptions=(Exception,))
    def delete_secret(self, key: str) -> bool:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            poller = self._client.begin_delete_secret(self._kv_name(key))
            poller.wait()
            return True
        except ResourceNotFoundError:
            return False

    @retry(max_attempts=3, delay=1.0, exceptions=(Exception,))
    def list_secrets(self, prefix: str = "") -> list[str]:
        kv_prefix = self._kv_name(prefix)
        return [
            p.name.replace("--", "/")
            for p in self._client.list_properties_of_secrets()
            if p.name.startswith(kv_prefix)
        ]
