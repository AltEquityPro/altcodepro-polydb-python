# src/polydb/adapters/PubSubAdapter.py

from polydb.base.QueueAdapter import QueueAdapter
from polydb.errors import ConnectionError, QueueError
from polydb.retry import retry
import os
import threading
from typing import Any, Dict, List

class PubSubAdapter(QueueAdapter):
    """GCP Pub/Sub with client reuse"""

    def __init__(self):
        super().__init__()
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or ""
        self.topic_name = os.getenv("PUBSUB_TOPIC", "default")
        self._publisher = None
        self._subscriber = None
        self._lock = threading.Lock()
        self._initialize_clients()

    def _initialize_clients(self):
        """Initialize Pub/Sub clients once"""
        try:
            from google.cloud import pubsub_v1

            with self._lock:
                if not self._publisher:
                    self._publisher = pubsub_v1.PublisherClient()
                    self._subscriber = pubsub_v1.SubscriberClient()
                    self.logger.info("Initialized Pub/Sub clients")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize Pub/Sub: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        """Send message to topic"""
        try:
            import json

            if self._publisher:
                topic_path = self._publisher.topic_path(
                    self.project_id, queue_name or self.topic_name
                )
                data = json.dumps(message).encode("utf-8")
                future = self._publisher.publish(topic_path, data)
                return future.result()
            return ""
        except Exception as e:
            raise QueueError(f"Pub/Sub send failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict[str, Any]]:
        """Receive messages from subscription"""
        try:
            import json

            if self._subscriber:
                subscription_path = self._subscriber.subscription_path(
                    self.project_id, queue_name or self.topic_name
                )

                response = self._subscriber.pull(
                    subscription=subscription_path, max_messages=max_messages, timeout=5.0
                )

                messages = [
                    json.loads(msg.message.data.decode()) for msg in response.received_messages
                ]

                # Acknowledge messages
                if response.received_messages:
                    ack_ids = [msg.ack_id for msg in response.received_messages]
                    self._subscriber.acknowledge(subscription=subscription_path, ack_ids=ack_ids)

                return messages
            return []
        except Exception as e:
            raise QueueError(f"Pub/Sub receive failed: {str(e)}")

    def delete(self, message_id: str, queue_name: str = "default") -> bool:
        """Messages are acknowledged upon receipt"""
        return True