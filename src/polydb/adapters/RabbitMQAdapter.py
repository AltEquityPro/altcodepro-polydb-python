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
        """Auto-create-on-first-use path for send()/receive(). Plain
        `durable=True`, no arguments -- if this ran again on a queue
        `declare()` already provisioned WITH dead-letter arguments,
        RabbitMQ would reject it as a channel-level PRECONDITION_FAILED
        (re-declaring an existing queue with different `arguments` is an
        error, not a silent merge). That doesn't happen here because
        `declare()` adds `queue_name` to `_declared_queues` itself after
        declaring it -- the `if queue_name in self._declared_queues:
        return` guard below short-circuits before this method's own
        plain queue_declare ever runs, as long as `declare()` was called
        before the first send()/receive() on that queue. Calling
        `declare()` *after* an implicit auto-create already happened on
        the same name is the one ordering this doesn't protect against --
        the second declare (with arguments) would itself hit
        PRECONDITION_FAILED against the already-durable/no-arguments
        queue this method created first.
        """
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

    def nack(self, ack_id: str, queue_name: str = "default") -> bool:
        """basic_nack(requeue=True) -- puts the message straight back on
        the queue for immediate redelivery, same `_pending` delivery-tag
        lookup ack()/delete() already use, same "unknown id is a
        harmless no-op" convention `_ack_delivery` establishes. This is
        deliberately NOT how dead-lettering gets triggered (that's
        `basic_nack(requeue=False)`/`basic_reject(requeue=False)` or a
        TTL/max-length policy on the queue itself, not a per-call
        requeue=False here) -- this method's contract is "redeliver now",
        not "give up on it", so it always requeues."""
        delivery_tag = self._pending.pop(ack_id, None)
        if delivery_tag is None:
            return False
        try:
            self._ensure_open()
            self._channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            return True
        except Exception as e:
            raise QueueError(f"RabbitMQ nack failed: {e}")

    def purge(self, queue_name: str = "default") -> int:
        """channel.queue_purge()'s Queue.PurgeOk response frame carries
        the actual count RabbitMQ purged (`.method.message_count`) --
        read from pika's own BlockingChannel.queue_purge return type
        rather than assumed, verified against a real local broker."""
        try:
            self._ensure_open()
            self._ensure_queue(queue_name)
            result = self._channel.queue_purge(queue=queue_name)
            return result.method.message_count
        except Exception as e:
            raise QueueError(f"RabbitMQ purge failed: {e}")

    def declare(
        self,
        queue_name: str = "default",
        *,
        durable: bool = True,
        dead_letter_queue: Optional[str] = None,
    ) -> bool:
        """Real AMQP dead-lettering via `x-dead-letter-exchange`/
        `x-dead-letter-routing-key` queue arguments -- not a hand-rolled
        shadow implementation that polls and re-routes messages itself.
        The default exchange ("") + the DLQ's own name as routing key is
        how AMQP delivers a dead-lettered message straight to a queue by
        that name with no extra exchange/binding setup required.

        The DLQ target is declared first, as an ordinary durable queue,
        so the dead-letter-routing-key always has somewhere real to land
        before the source queue (which references it) is declared.
        """
        try:
            self._ensure_open()

            arguments: Dict[str, Any] = {}
            if dead_letter_queue:
                self._channel.queue_declare(queue=dead_letter_queue, durable=True)
                self._declared_queues.add(dead_letter_queue)
                arguments["x-dead-letter-exchange"] = ""
                arguments["x-dead-letter-routing-key"] = dead_letter_queue

            self._channel.queue_declare(
                queue=queue_name, durable=durable, arguments=arguments or None
            )
            # Marking it declared here is what keeps _ensure_queue()'s own
            # implicit auto-create path (plain durable=True, no
            # arguments) from ever re-declaring this queue with different
            # arguments on the first send()/receive() -- see that
            # method's docstring for the PRECONDITION_FAILED this avoids.
            self._declared_queues.add(queue_name)
            return True
        except Exception as e:
            raise QueueError(f"RabbitMQ declare failed: {e}")

    def status(self, queue_name: str = "default") -> Dict[str, Any]:
        """queue_declare(passive=True) -- "don't create it, just inspect
        it" (raises if the queue doesn't already exist, unlike a plain
        queue_declare) -- confirmed against pika's actual
        BlockingChannel.queue_declare behavior, not assumed. Its
        Queue.DeclareOk response frame carries both message_count and
        consumer_count for a classic queue."""
        try:
            self._ensure_open()
            result = self._channel.queue_declare(queue=queue_name, passive=True)
            return {
                "message_count": result.method.message_count,
                "consumer_count": result.method.consumer_count,
            }
        except Exception as e:
            raise QueueError(f"RabbitMQ status failed: {e}")

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
