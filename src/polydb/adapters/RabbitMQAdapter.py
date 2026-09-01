# src/polydb/adapters/RabbitMQAdapter.py
import os
import json
import threading
import uuid
from typing import Any, Dict, List, Optional

from ..base.QueueAdapter import QueueAdapter
from ..errors import ConnectionError, QueueError
from ..retry import retry
from ..json_safe import json_safe


class RabbitMQAdapter(QueueAdapter):
    """
    RabbitMQ adapter using the synchronous `pika` client, not `aio-pika` --
    same rationale as KafkaQueueAdapter: every adapter in this codebase is
    synchronous end to end.

    queue_name IS the AMQP queue name. Queues are declared durable and
    auto-created on first use, matching SQSAdapter's/AzureQueueAdapter's
    "create the underlying resource if it doesn't already exist" behavior.
    """

    def __init__(
        self,
        url: str = "",
        host: str = "",
        port: int = 0,
        username: str = "",
        password: str = "",
        virtual_host: str = "",
    ):
        super().__init__()

        # An explicit amqp:// URL (or RABBITMQ_URL/AMQP_URL env var) takes
        # priority; otherwise build one from discrete fields/env so callers
        # don't have to hand-assemble a URL just to point at localhost.
        self.url = url or os.getenv("RABBITMQ_URL") or os.getenv("AMQP_URL")

        if not self.url:
            host = host or os.getenv("RABBITMQ_HOST", "localhost")
            port = port or int(os.getenv("RABBITMQ_PORT", "5672"))
            username = username or os.getenv("RABBITMQ_USER", "guest")
            password = password or os.getenv("RABBITMQ_PASSWORD", "guest")
            virtual_host = virtual_host or os.getenv("RABBITMQ_VHOST", "/")
            vhost_path = virtual_host if virtual_host.startswith("/") else f"/{virtual_host}"
            self.url = f"amqp://{username}:{password}@{host}:{port}{vhost_path}"

        self._connection: Any = None
        self._channel: Any = None
        self._declared_queues: set = set()
        self._lock = threading.Lock()

        # Our own message_id (embedded in AMQP message properties on
        # publish, since basic_publish has no return value to hand one
        # back) -> the AMQP delivery_tag basic_get gave us for it. ack()/
        # delete() look the delivery tag up here so callers never need to
        # know pika's delivery-tag concept exists.
        self._pending: Dict[str, int] = {}

        self._initialize_connection()

    # ---------------------------------------------------------
    # Connection management
    # ---------------------------------------------------------

    def _initialize_connection(self) -> None:
        import pika

        try:
            with self._lock:
                if self._connection is not None and self._connection.is_open:
                    return
                self._connection = pika.BlockingConnection(pika.URLParameters(self.url))
                self._channel = self._connection.channel()
                self.logger.info("Initialized RabbitMQ connection")
        except Exception as e:
            raise ConnectionError(f"RabbitMQ connection failed: {e}")

    def _ensure_open(self) -> None:
        needs_reconnect = (
            self._connection is None
            or self._connection.is_closed
            or self._channel is None
            or self._channel.is_closed
        )
        if needs_reconnect:
            # A dropped connection also invalidates any queues we thought
            # we'd already declared on the old channel.
            self._declared_queues.clear()
            self._initialize_connection()

    def _ensure_queue(self, queue_name: str) -> None:
        if queue_name in self._declared_queues:
            return
        self._channel.queue_declare(queue=queue_name, durable=True)
        self._declared_queues.add(queue_name)

    # ---------------------------------------------------------
    # Queue operations
    # ---------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        """Publish to `queue_name`, returning a message id we generate and
        embed ourselves -- AMQP's basic_publish is fire-and-forget and
        hands back nothing to identify the message with."""
        try:
            import pika

            self._ensure_open()
            self._ensure_queue(queue_name)

            message_id = uuid.uuid4().hex
            body = json.dumps(message, default=json_safe).encode("utf-8")

            self._channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=body,
                properties=pika.BasicProperties(
                    message_id=message_id,
                    delivery_mode=2,  # persistent: survives a broker restart
                ),
            )

            return message_id

        except Exception as e:
            raise QueueError(f"RabbitMQ send failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict[str, Any]]:
        """
        basic_get in a loop, not a long-lived basic_consume callback. The
        QueueAdapter contract needs receive() to return promptly with
        whatever's immediately available -- the same shape SQS/Azure/
        Pub/Sub/Kafka already have. A consumer callback would instead
        block indefinitely inside pika's IO loop waiting for messages,
        which doesn't fit a synchronous request/response method at all.
        """
        try:
            self._ensure_open()
            self._ensure_queue(queue_name)

            out: List[Dict[str, Any]] = []

            for _ in range(max_messages):
                method, properties, body = self._channel.basic_get(
                    queue=queue_name, auto_ack=False
                )
                if method is None:
                    break  # queue is (currently) empty

                message_id = (properties.message_id if properties else None) or str(
                    method.delivery_tag
                )

                try:
                    payload = json.loads(body.decode("utf-8"))
                except Exception:
                    payload = body.decode("utf-8", errors="replace")

                self._pending[message_id] = method.delivery_tag

                out.append(
                    {
                        "id": message_id,
                        "receipt_handle": message_id,
                        "body": payload,
                        "redelivered": method.redelivered,
                    }
                )

            return out

        except Exception as e:
            raise QueueError(f"RabbitMQ receive failed: {e}")

    def _ack_delivery(self, message_id: str) -> bool:
        delivery_tag = self._pending.pop(message_id, None)
        if delivery_tag is None:
            # Unknown/already-acked id -- treat as a no-op success, matching
            # VercelQueueAdapter's/BlockchainQueueAdapter's existing
            # convention for a redundant ack rather than raising.
            return False
        try:
            self._ensure_open()
            self._channel.basic_ack(delivery_tag=delivery_tag)
            return True
        except Exception as e:
            raise QueueError(f"RabbitMQ ack failed: {e}")

    def ack(self, ack_id: str, queue_name: str = "default") -> bool:
        if not ack_id:
            raise QueueError("ack_id is required for RabbitMQ ack")
        return self._ack_delivery(ack_id)

    def delete(self, message_id: str, queue_name: str = "default", pop_receipt: str = "") -> bool:
        """
        Delete == ack. AMQP has no "delete without processing" primitive
        beyond ack/nack/reject -- basic_ack is the only way to permanently
        remove a message from the queue -- so this collapses to the same
        call as ack(), matching the majority convention among this
        codebase's other adapters (SQS, Pub/Sub: ack is delete).
        """
        return self._ack_delivery(message_id)

    def close(self) -> None:
        """Close the channel/connection. Not part of QueueAdapter's
        abstract contract, but good hygiene for a long-lived instance."""
        try:
            if self._channel is not None and self._channel.is_open:
                self._channel.close()
        except Exception:
            pass
        try:
            if self._connection is not None and self._connection.is_open:
                self._connection.close()
        except Exception:
            pass
