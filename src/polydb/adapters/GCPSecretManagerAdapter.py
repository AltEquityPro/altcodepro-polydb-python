# src/polydb/adapters/GCPSecretManagerAdapter.py
import os
from typing import Optional

from ..base.SecretsAdapter import SecretsAdapter
from ..errors import ConnectionError
from ..retry import retry


class GCPSecretManagerAdapter(SecretsAdapter):
    def __init__(self, project_id: str = ""):
        super().__init__()

        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        if not self.project_id:
            raise ConnectionError("GOOGLE_CLOUD_PROJECT is not configured")

        self._client = None
        self._initialize_client()

    def _initialize_client(self) -> None:
        from google.cloud import secretmanager

        self._client = secretmanager.SecretManagerServiceClient()

    def _secret_path(self, key: str) -> str:
        return f"projects/{self.project_id}/secrets/{key}"

    @retry(max_attempts=3, delay=1.0, exceptions=(Exception,))
    def get_secret(self, key: str) -> Optional[str]:
        from google.api_core.exceptions import NotFound

        try:
            resp = self._client.access_secret_version(
                name=f"{self._secret_path(key)}/versions/latest"
            )
        except NotFound:
            return None
        return resp.payload.data.decode("utf-8")

    @retry(max_attempts=3, delay=1.0, exceptions=(Exception,))
    def set_secret(self, key: str, value: str) -> None:
        from google.api_core.exceptions import AlreadyExists

        try:
            self._client.create_secret(
                request={
                    "parent": f"projects/{self.project_id}",
                    "secret_id": key,
                    "secret": {"replication": {"automatic": {}}},
                }
            )
        except AlreadyExists:
            pass
        self._client.add_secret_version(
            request={
                "parent": self._secret_path(key),
                "payload": {"data": value.encode("utf-8")},
            }
        )

    @retry(max_attempts=3, delay=1.0, exceptions=(Exception,))
    def delete_secret(self, key: str) -> bool:
        from google.api_core.exceptions import NotFound

        try:
            self._client.delete_secret(request={"name": self._secret_path(key)})
            return True
        except NotFound:
            return False

    @retry(max_attempts=3, delay=1.0, exceptions=(Exception,))
    def list_secrets(self, prefix: str = "") -> list[str]:
        secrets = self._client.list_secrets(request={"parent": f"projects/{self.project_id}"})
        return [
            s.name.rsplit("/", 1)[-1]
            for s in secrets
            if s.name.rsplit("/", 1)[-1].startswith(prefix)
        ]
