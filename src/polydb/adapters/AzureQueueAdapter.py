# src/polydb/adapters/AzureQueueAdapter.py
from polydb.base.QueueAdapter import QueueAdapter
from polydb.errors import ConnectionError, QueueError
from polydb.retry import retry
import os
import threading
from typing import Any, Dict, List

class AzureQueueAdapter(QueueAdapter):
    """Azure Queue Storage with client reuse"""

    def __init__(self):
        super().__init__()
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING") or ""
        self._client = None
        self._lock = threading.Lock()
        self._initialize_client()

    def _initialize_client(self):
        """Initialize Azure Queue Storage client once"""
        try:
            from azure.storage.queue import QueueServiceClient

            with self._lock:
                if not self._client:
                    self._client = QueueServiceClient.from_connection_string(self.connection_string)
                    self.logger.info("Initialized Azure Queue Storage client")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize Azure Queue Storage: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        """Send message to queue"""
        try:
            import json

            if self._client:
                queue_client = self._client.get_queue_client(queue_name)
                response = queue_client.send_message(json.dumps(message))
                return response.id
            return ""
        except Exception as e:
            raise QueueError(f"Azure Queue send failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict[str, Any]]:
        """Receive messages from queue"""
        try:
            import json

            if self._client:
                queue_client = self._client.get_queue_client(queue_name)
                messages = queue_client.receive_messages(max_messages=max_messages)
                return [json.loads(msg.content) for msg in messages]
            return []
        except Exception as e:
            raise QueueError(f"Azure Queue receive failed: {str(e)}")

    def delete(self, message_id: str, queue_name: str = "default") -> bool:
        """Delete message from queue (requires receipt handle)"""
        return True