from polydb.base.QueueAdapter import QueueAdapter
from polydb.errors import ConnectionError, QueueError
from polydb.retry import retry


import boto3
from botocore.client import BaseClient


import json
import os
import threading
from typing import Any, Dict, List

from ..json_safe import json_safe


class SQSAdapter(QueueAdapter):
    """AWS SQS with client reuse"""

    def __init__(self):
        super().__init__()
        self.queue_url = os.getenv("SQS_QUEUE_URL")
        self._client: BaseClient | None = None
        self._lock = threading.Lock()
        self._initialize_client()

    def _initialize_client(self):
        """Initialize SQS client once"""
        try:
            import boto3

            with self._lock:
                if not self._client:
                    self._client = boto3.client("sqs")
                    self.logger.info("Initialized SQS client")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize SQS client: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        """Send message to queue"""
        try:
            import json

            if not self._client:
                self._initialize_client()
            if self._client:
                response = self._client.send_message(
                    QueueUrl=self.queue_url, MessageBody=json.dumps(message,default=json_safe)
                )
                return response["MessageId"]
            return ""
        except Exception as e:
            raise QueueError(f"SQS send failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict[str, Any]]:
        """Receive messages from queue"""
        try:
            import json

            if not self._client:
                self._initialize_client()
            if self._client:
                response = self._client.receive_message(
                    QueueUrl=self.queue_url, MaxNumberOfMessages=max_messages, WaitTimeSeconds=5
                )
                messages = response.get("Messages", [])
                return [json.loads(msg["Body"]) for msg in messages]
            return []
        except Exception as e:
            raise QueueError(f"SQS receive failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def delete(self, message_id: str, queue_name: str = "default") -> bool:
        """Delete message from queue"""
        try:
            if not self._client:
                self._initialize_client()
            if self._client:
                self._client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=message_id)
                return True
            return False
        except Exception as e:
            raise QueueError(f"SQS delete failed: {str(e)}")