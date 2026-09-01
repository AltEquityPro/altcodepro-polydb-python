# src/polydb/adapters/AWSSecretsManagerAdapter.py
import os
from typing import Optional

from ..base.SecretsAdapter import SecretsAdapter
from ..retry import retry


class AWSSecretsManagerAdapter(SecretsAdapter):
    def __init__(self, region: str = ""):
        super().__init__()

        self.region = region or os.getenv("AWS_REGION", "us-east-1")
        self._client = None
        self._initialize_client()

    def _initialize_client(self) -> None:
        import boto3

        self._client = boto3.client("secretsmanager", region_name=self.region)

    @retry(max_attempts=3, delay=1.0, exceptions=(Exception,))
    def get_secret(self, key: str) -> Optional[str]:
        from botocore.exceptions import ClientError

        try:
            resp = self._client.get_secret_value(SecretId=key)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
            raise
        return resp.get("SecretString")

    @retry(max_attempts=3, delay=1.0, exceptions=(Exception,))
    def set_secret(self, key: str, value: str) -> None:
        from botocore.exceptions import ClientError

        try:
            self._client.put_secret_value(SecretId=key, SecretString=value)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ResourceNotFoundException":
                self._client.create_secret(Name=key, SecretString=value)
            else:
                raise

    @retry(max_attempts=3, delay=1.0, exceptions=(Exception,))
    def delete_secret(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self._client.delete_secret(SecretId=key, ForceDeleteWithoutRecovery=True)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ResourceNotFoundException":
                return False
            raise

    @retry(max_attempts=3, delay=1.0, exceptions=(Exception,))
    def list_secrets(self, prefix: str = "") -> list[str]:
        names: list[str] = []
        paginator = self._client.get_paginator("list_secrets")
        for page in paginator.paginate():
            names.extend(s["Name"] for s in page["SecretList"] if s["Name"].startswith(prefix))
        return names
