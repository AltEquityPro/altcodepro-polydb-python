# src/polydb/adapters/AzureQueueAdapter.py

import os
import threading
import json
import re

from typing import Any, Dict, List, Optional

from azure.storage.queue import QueueServiceClient, QueueClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from ..base.QueueAdapter import QueueAdapter
from ..errors import ConnectionError, QueueError
from ..retry import retry
from ..json_safe import json_safe


class AzureQueueAdapter(QueueAdapter):
    """
    Azure Queue Storage adapter.

    Features
    - Thread-safe initialization
    - Automatic queue creation
    - Client reuse
    - Retry support
    """

    def __init__(self, connection_string: str = ""):
        super().__init__()

        self.connection_string = connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING")

        if not self.connection_string:
            raise ConnectionError("AZURE_STORAGE_CONNECTION_STRING is not configured")

        self._client: Optional[QueueServiceClient] = None
        self._queues: Dict[str, QueueClient] = {}

        self._lock = threading.Lock()

        self._initialize_client()

    def _normalize_queue_name(self, name: str) -> str:
        name = name.lower()
        name = re.sub(r"[^a-z0-9-]", "-", name)  # replace invalid chars
        name = re.sub(r"-+", "-", name)  # collapse multiple dashes
        return name.strip("-")

    def _initialize_client(self) -> None:
        """Initialize Azure Queue client"""
        try:
            with self._lock:
                if self._client is not None:
                    return
                if not self.connection_string:
                    raise ConnectionError("AZURE_STORAGE_CONNECTION_STRING is not configured")
                self._client = QueueServiceClient.from_connection_string(self.connection_string)

                self.logger.info("Initialized Azure Queue Storage client")

        except Exception as e:
            raise ConnectionError(f"Failed to initialize Azure Queue Storage: {e}")

    def _get_queue(self, queue_name: str) -> QueueClient:
        """Get or create queue client"""
        if self._client is None:
            raise ConnectionError("Azure Queue client not initialized")
        queue_name = self._normalize_queue_name(queue_name)
        if queue_name not in self._queues:
            queue_client = self._client.get_queue_client(queue_name)

            try:
                queue_client.create_queue()
            except ResourceExistsError:
                pass

            self._queues[queue_name] = queue_client

        return self._queues[queue_name]

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        """Send message to queue"""
        try:
            queue_name = self._normalize_queue_name(queue_name)
            queue_client = self._get_queue(queue_name)

            response = queue_client.send_message(json.dumps(message, default=json_safe))

            return response.id

        except Exception as e:
            raise QueueError(f"Azure Queue send failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict[str, Any]]:
        """Receive messages"""
        try:

            queue_name = self._normalize_queue_name(queue_name)
            queue_client = self._get_queue(queue_name)

            messages = queue_client.receive_messages(max_messages=max_messages)

            results = []

            for msg in messages:
                payload = json.loads(msg.content)
                results.append(
                    {
                        "id": msg.id,
                        "pop_receipt": msg.pop_receipt,
                        "body": payload,
                    }
                )

            return results

        except Exception as e:
            raise QueueError(f"Azure Queue receive failed: {e}")

    def delete(self, message_id: str, queue_name: str = "default", pop_receipt: str = "") -> bool:
        """Delete message from queue"""
        try:
            queue_name = self._normalize_queue_name(queue_name)
            queue_client = self._get_queue(queue_name)

            queue_client.delete_message(message_id, pop_receipt)

            return True

        except ResourceNotFoundError:
            return False
        except Exception as e:
            raise QueueError(f"Azure Queue delete failed: {e}")

    def ack(
        self,
        pop_receipt: str,
        queue_name: str = "default",
        message_id: Optional[str] = None,
    ) -> bool:
        """
        Acknowledge (delete) a message.

        Azure Queue requires BOTH:
        - message_id
        - pop_receipt

        Preferred usage:
            ack(pop_receipt=..., message_id=...)

        If message_id is not provided, this will fail safely.
        """
        if not message_id:
            raise QueueError("AzureQueueAdapter.ack requires message_id")
        queue_name = self._normalize_queue_name(queue_name)
        return self.delete(
            message_id=message_id,
            queue_name=queue_name,
            pop_receipt=pop_receipt,
        )
