from __future__ import annotations

import json
import os
import threading
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError

from ..base.QueueAdapter import QueueAdapter
from ..errors import ConnectionError, QueueError
from ..retry import retry
from ..json_safe import json_safe


class SQSAdapter(QueueAdapter):
    """AWS SQS adapter with automatic queue creation (AWS + LocalStack compatible)"""

    def __init__(self, queue_name: str = "", region: str = "", endpoint_url: str = ""):
        super().__init__()

        self.queue_name = queue_name or os.getenv("SQS_QUEUE_NAME", "polydb-queue")

        self.region = (
            region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
        )

        self.endpoint_url = endpoint_url or os.getenv("AWS_ENDPOINT_URL")

        self._client: Any = None
        self._queue_url = None
        self._lock = threading.Lock()

        self._initialize_client()

    # ---------------------------------------------------------
    # Client initialization
    # ---------------------------------------------------------

    def _initialize_client(self):
        try:
            with self._lock:
                if self._client:
                    return

                self._client = boto3.client(
                    "sqs",
                    region_name=self.region,
                    endpoint_url=self.endpoint_url,
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
                )

                self._queue_url = self._ensure_queue_exists(self.queue_name)

                self.logger.info(
                    f"Initialized SQS client (queue={self.queue_name}, endpoint={self.endpoint_url or 'aws'})"
                )

        except Exception as e:
            raise ConnectionError(f"SQS init failed: {e}")

    # ---------------------------------------------------------
    # Queue management
    # ---------------------------------------------------------

    def _ensure_queue_exists(self, queue_name: str) -> str:
        """Create queue if it does not exist"""
        if not self._client:
            raise ConnectionError("SQS client not initialized")

        try:
            response = self._client.get_queue_url(QueueName=queue_name)
            return response["QueueUrl"]

        except self._client.exceptions.QueueDoesNotExist:  # type: ignore

            response = self._client.create_queue(QueueName=queue_name)
            return response["QueueUrl"]

        except ClientError as e:
            raise QueueError(f"SQS queue creation failed: {e}")

    # ---------------------------------------------------------
    # Queue operations
    # ---------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        """Send message to queue"""
        try:
            if not self._client:
                self._initialize_client()

            body = (
                json.dumps(message, default=json_safe) if not isinstance(message, str) else message
            )

            resp = self._client.send_message(
                QueueUrl=self._queue_url,
                MessageBody=body,
            )

            return resp["MessageId"]

        except Exception as e:
            raise QueueError(f"SQS send failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict[str, Any]]:
        """Receive messages from queue"""
        try:
            if not self._client:
                self._initialize_client()

            resp = self._client.receive_message(
                QueueUrl=self._queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=1,
            )

            messages = resp.get("Messages", [])

            out: List[Dict[str, Any]] = []

            for m in messages:
                body = m.get("Body")

                try:
                    body = json.loads(body)
                except Exception:
                    pass

                out.append(
                    {
                        "body": body,
                        "receipt_handle": m["ReceiptHandle"],
                        "message_id": m["MessageId"],
                    }
                )

            return out

        except Exception as e:
            raise QueueError(f"SQS receive failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def delete(self, receipt_handle: str, queue_name: str = "default") -> bool:
        """Delete message from queue"""
        try:
            if not self._client:
                self._initialize_client()

            self._client.delete_message(
                QueueUrl=self._queue_url,
                ReceiptHandle=receipt_handle,
            )

            return True

        except Exception as e:
            raise QueueError(f"SQS delete failed: {e}")

    # ---------------------------------------------------------
    # ACK (SQS = delete using receipt_handle)
    # ---------------------------------------------------------
    def ack(self, ack_id: str, queue_name: str = "default") -> bool:
        """
        ACK message in SQS.

        ack_id = receipt_handle (NOT message_id)

        This maps to delete operation.
        """
        if not ack_id:
            raise QueueError("ack_id (receipt_handle) is required for SQS ack")

        return self.delete(ack_id, queue_name=queue_name)
