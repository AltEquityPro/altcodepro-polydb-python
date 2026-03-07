import os
import json
import threading
from typing import Any, Dict, List, Optional, Tuple, Union

from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists, NotFound

from ..base.QueueAdapter import QueueAdapter
from ..errors import ConnectionError, QueueError
from ..retry import retry
from ..json_safe import json_safe


JsonLike = Union[Dict[str, Any], List[Any], str, int, float, bool, None]


class GCPPubSubAdapter(QueueAdapter):
    """
    Production-grade GCP Pub/Sub adapter.

    - Emulator support via PUBSUB_EMULATOR_HOST (google client honors it)
    - Auto topic + subscription creation
    - send accepts Any (tests send str)
    - receive returns [{"id","ack_id","body"}...]
    - ack() method for tests
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        topic: Optional[str] = None,
        subscription: Optional[str] = None,
    ):
        super().__init__()

        self.project_id: str = project_id or os.getenv("GOOGLE_CLOUD_PROJECT", "polydb-test")
        self.default_topic: str = topic or os.getenv("PUBSUB_TOPIC", "polydb-topic")
        self.default_subscription: str = subscription or os.getenv(
            "PUBSUB_SUBSCRIPTION", "polydb-sub"
        )

        self._publisher: Optional[pubsub_v1.PublisherClient] = None
        self._subscriber: Optional[pubsub_v1.SubscriberClient] = None
        self._lock = threading.Lock()

        self._initialize_clients()

    def _initialize_clients(self) -> None:
        try:
            with self._lock:
                if self._publisher and self._subscriber:
                    return
                # google client auto-detects PUBSUB_EMULATOR_HOST if set
                self._publisher = pubsub_v1.PublisherClient()
                self._subscriber = pubsub_v1.SubscriberClient()
                self.logger.info("Initialized Pub/Sub clients")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize Pub/Sub: {e}")

    def _resolve_names(self, queue_name: str) -> Tuple[str, str]:
        """
        Your tests call send() / receive() without queue_name,
        so they pass default="default". That should map to adapter defaults.
        """
        if not queue_name or queue_name == "default":
            return self.default_topic, self.default_subscription
        # If user passes a custom name, use it for both (simple convention)
        return queue_name, queue_name

    def _topic_path(self, topic: str) -> str:
        if not self._publisher:
            raise ConnectionError("Pub/Sub publisher not initialized")
        return self._publisher.topic_path(self.project_id, topic)

    def _subscription_path(self, subscription: str) -> str:
        if not self._subscriber:
            raise ConnectionError("Pub/Sub subscriber not initialized")
        return self._subscriber.subscription_path(self.project_id, subscription)

    def _ensure_topic(self, topic: str) -> str:
        if not self._publisher:
            raise ConnectionError("Pub/Sub publisher not initialized")
        path = self._topic_path(topic)
        try:
            self._publisher.get_topic(request={"topic": path})
        except NotFound:
            try:
                self._publisher.create_topic(request={"name": path})
                self.logger.info(f"Created Pub/Sub topic: {topic}")
            except AlreadyExists:
                pass
        return path

    def _ensure_subscription(self, topic: str, subscription: str) -> str:
        if not self._subscriber:
            raise ConnectionError("Pub/Sub subscriber not initialized")
        if not self._publisher:
            raise ConnectionError("Pub/Sub publisher not initialized")

        topic_path = self._ensure_topic(topic)
        sub_path = self._subscription_path(subscription)

        try:
            self._subscriber.get_subscription(request={"subscription": sub_path})
        except NotFound:
            try:
                self._subscriber.create_subscription(
                    request={"name": sub_path, "topic": topic_path}
                )
                self.logger.info(f"Created Pub/Sub subscription: {subscription}")
            except AlreadyExists:
                pass

        return sub_path

    # -------------------------
    # API
    # -------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def send(self, message: JsonLike, queue_name: str = "default") -> str:
        """
        Publish message to Pub/Sub.
        Tests send a string; production may send dict.
        We envelope it as {"body": <message>}.
        """
        try:
            if not self._publisher:
                raise ConnectionError("Pub/Sub publisher not initialized")

            topic, subscription = self._resolve_names(queue_name)
            topic_path = self._ensure_topic(topic)
            # Ensure subscription exists so receive works immediately in emulator
            self._ensure_subscription(topic, subscription)

            payload = {"body": message}
            data = json.dumps(payload, default=json_safe).encode("utf-8")

            future = self._publisher.publish(topic_path, data=data)
            msg_id = future.result(timeout=10)

            self.logger.debug(f"Published Pub/Sub message {msg_id} (topic={topic})")
            return msg_id

        except Exception as e:
            raise QueueError(f"Pub/Sub send failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict[str, Any]]:
        """
        Pull messages.
        IMPORTANT: Do NOT auto-ack here (so test_ack_message can ack explicitly).
        """
        try:
            if not self._subscriber:
                raise ConnectionError("Pub/Sub subscriber not initialized")

            topic, subscription = self._resolve_names(queue_name)
            sub_path = self._ensure_subscription(topic, subscription)

            resp = self._subscriber.pull(
                request={"subscription": sub_path, "max_messages": max_messages},
                timeout=5,
            )

            out: List[Dict[str, Any]] = []
            for r in resp.received_messages:
                body: Any = None
                try:
                    decoded = json.loads(r.message.data.decode("utf-8"))
                    if isinstance(decoded, dict) and "body" in decoded:
                        body = decoded["body"]
                    else:
                        body = decoded
                except Exception:
                    body = r.message.data.decode("utf-8", errors="replace")

                out.append(
                    {
                        "id": r.message.message_id,
                        "ack_id": r.ack_id,
                        "body": body,
                    }
                )

            return out

        except Exception as e:
            raise QueueError(f"Pub/Sub receive failed: {e}")

    def delete(self, message_id: str, queue_name: str = "default", pop_receipt: str = "") -> bool:
        """
        Pub/Sub delete == ack.
        - pop_receipt maps to ack_id (preferred).
        - message_id fallback kept for backward compatibility.
        """
        try:
            if not self._subscriber:
                raise ConnectionError("Pub/Sub subscriber not initialized")

            _, subscription = self._resolve_names(queue_name)
            sub_path = self._subscription_path(subscription)

            ack_id = pop_receipt or message_id
            if not ack_id:
                raise QueueError("Pub/Sub delete requires ack_id (use pop_receipt from receive())")

            self._subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": [ack_id]})
            return True

        except Exception as e:
            raise QueueError(f"Pub/Sub delete failed: {e}")

    # Alias that your tests want
    def ack(self, ack_id: str, queue_name: str = "default") -> bool:
        return self.delete(message_id="", queue_name=queue_name, pop_receipt=ack_id)