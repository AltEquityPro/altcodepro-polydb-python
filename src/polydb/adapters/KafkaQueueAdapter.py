# src/polydb/adapters/KafkaQueueAdapter.py
import os
import json
import threading
import uuid
from typing import Any, Dict, List, Optional

from ..base.QueueAdapter import QueueAdapter
from ..errors import ConnectionError, QueueError
from ..retry import retry
from ..json_safe import json_safe


class KafkaQueueAdapter(QueueAdapter):
    """
    Apache Kafka adapter using the synchronous `kafka-python` client, not
    `aiokafka`. Every other adapter in this codebase (SQS, Azure Queue,
    Pub/Sub, RabbitMQ, ...) is synchronous end to end -- pulling in an
    asyncio-native client here would mean either spinning an event loop
    per call or wrapping every call in asyncio.run(), both strictly worse
    than using the client kafka-python already provides for exactly this
    call shape.

    queue_name IS the Kafka topic.
    """

    def __init__(
        self,
        bootstrap_servers: str = "",
        group_id: str = "",
        client_id: str = "",
        security_protocol: str = "",
        sasl_mechanism: str = "",
        sasl_plain_username: str = "",
        sasl_plain_password: str = "",
        ssl_cafile: str = "",
        auto_offset_reset: str = "earliest",
    ):
        super().__init__()

        servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.bootstrap_servers = [s.strip() for s in servers.split(",") if s.strip()]

        # A random per-instance consumer group by default so that two
        # independently-constructed adapters (e.g. two test runs, or two
        # unrelated callers in the same process) don't silently share
        # partition assignment / offsets with each other. Callers that
        # actually want shared work-queue semantics across processes pass
        # an explicit group_id (or set KAFKA_GROUP_ID).
        self.group_id = (
            group_id or os.getenv("KAFKA_GROUP_ID") or f"polydb-{uuid.uuid4().hex[:12]}"
        )
        self.client_id = client_id or os.getenv("KAFKA_CLIENT_ID", "polydb")
        self.auto_offset_reset = auto_offset_reset

        # Auth/TLS are all optional -- PLAINTEXT (no auth) is the default
        # so local/dev brokers work with zero extra config, matching how
        # e.g. SQSAdapter defaults endpoint_url to "" (real AWS) rather
        # than requiring LocalStack settings to be supplied.
        self.security_protocol = security_protocol or os.getenv(
            "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"
        )
        self.sasl_mechanism = sasl_mechanism or os.getenv("KAFKA_SASL_MECHANISM") or None
        self.sasl_plain_username = (
            sasl_plain_username or os.getenv("KAFKA_SASL_USERNAME") or None
        )
        self.sasl_plain_password = (
            sasl_plain_password or os.getenv("KAFKA_SASL_PASSWORD") or None
        )
        self.ssl_cafile = ssl_cafile or os.getenv("KAFKA_SSL_CAFILE") or None

        self._producer: Any = None
        self._consumers: Dict[str, Any] = {}  # topic -> KafkaConsumer

        # message_id -> (TopicPartition, offset_to_commit). Populated by
        # receive(), consumed (and popped) by ack()/delete(). See the long
        # comment on receive() for why committing only happens here.
        self._pending: Dict[str, Any] = {}

        self._lock = threading.Lock()

    # ---------------------------------------------------------
    # Client initialization
    # ---------------------------------------------------------

    def _client_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {
            "bootstrap_servers": self.bootstrap_servers,
            "security_protocol": self.security_protocol,
        }
        if self.sasl_mechanism:
            kwargs["sasl_mechanism"] = self.sasl_mechanism
            kwargs["sasl_plain_username"] = self.sasl_plain_username
            kwargs["sasl_plain_password"] = self.sasl_plain_password
        if self.ssl_cafile:
            kwargs["ssl_cafile"] = self.ssl_cafile
        return kwargs

    def _get_producer(self):
        from kafka import KafkaProducer

        if self._producer is not None:
            return self._producer

        with self._lock:
            if self._producer is not None:
                return self._producer
            try:
                self._producer = KafkaProducer(
                    client_id=self.client_id,
                    # We JSON-encode to bytes ourselves (matches json_safe
                    # usage elsewhere in this codebase), so the serializer
                    # is a passthrough rather than kafka-python's own.
                    value_serializer=lambda v: v,
                    **self._client_kwargs(),
                )
                self.logger.info(
                    f"Initialized Kafka producer (bootstrap={self.bootstrap_servers})"
                )
            except Exception as e:
                raise ConnectionError(f"Kafka producer init failed: {e}")
            return self._producer

    def _get_consumer(self, topic: str):
        if topic in self._consumers:
            return self._consumers[topic]

        from kafka import KafkaConsumer

        with self._lock:
            if topic in self._consumers:
                return self._consumers[topic]
            try:
                consumer = KafkaConsumer(
                    topic,
                    group_id=self.group_id,
                    client_id=self.client_id,
                    # Manual commits only -- see receive()'s docstring for
                    # why offsets are committed exclusively from ack()/
                    # delete(), never automatically here.
                    enable_auto_commit=False,
                    auto_offset_reset=self.auto_offset_reset,
                    **self._client_kwargs(),
                )
                self.logger.info(f"Initialized Kafka consumer (topic={topic}, group={self.group_id})")
            except Exception as e:
                raise ConnectionError(f"Kafka consumer init failed: {e}")
            self._consumers[topic] = consumer
            return consumer

    # ---------------------------------------------------------
    # Queue operations
    # ---------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        """Produce a message to `queue_name` (the Kafka topic)."""
        try:
            producer = self._get_producer()
            body = (
                json.dumps(message, default=json_safe).encode("utf-8")
                if not isinstance(message, (bytes, bytearray))
                else message
            )

            future = producer.send(queue_name, value=body)
            # kafka-python's send() is async by default (it returns a
            # FutureRecordMetadata immediately, before the broker has
            # necessarily accepted the record). .get() blocks for that ack
            # so send() returns only once the message is durably produced
            # -- matching every other adapter's synchronous
            # send()->message_id contract instead of firing-and-forgetting.
            record = future.get(timeout=10)
            return f"{record.partition}-{record.offset}"

        except Exception as e:
            raise QueueError(f"Kafka send failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict[str, Any]]:
        """
        Poll up to `max_messages` from `queue_name`'s consumer group.

        Deliberately does NOT commit offsets here (the consumer is created
        with enable_auto_commit=False too). A message only becomes "done"
        from Kafka's point of view once ack()/delete() commits its offset.
        If the caller crashes after receive() but before ack(), nothing
        was ever committed, so the next poll -- this process restarted, or
        any other consumer sharing this group_id -- redelivers the message
        from the same offset. That's the same at-least-once shape SQS's
        visibility timeout, Azure's visibility timeout, and Pub/Sub's
        unacked-redelivery already give this codebase's other queue
        adapters. Auto-committing inside receive() would instead mark a
        message "consumed" the instant it's handed out, which loses
        redelivery on a crash mid-processing -- effectively at-most-once,
        not the at-least-once contract the rest of this codebase relies on
        (WorkerPool retries on a failed/never-acked message).
        """
        try:
            from kafka import TopicPartition

            consumer = self._get_consumer(queue_name)

            out: List[Dict[str, Any]] = []
            # poll() returns whatever's ready in a single batch, which may
            # be less than max_messages even when more exists -- loop
            # (bounded) rather than assuming one poll() satisfies the ask.
            attempts = 0
            while len(out) < max_messages and attempts < 5:
                attempts += 1
                remaining = max_messages - len(out)
                batches = consumer.poll(timeout_ms=1000, max_records=remaining)
                if not batches:
                    break

                for tp, records in batches.items():
                    for record in records:
                        message_id = f"{record.partition}-{record.offset}"

                        try:
                            body = json.loads(record.value.decode("utf-8"))
                        except Exception:
                            body = record.value.decode("utf-8", errors="replace")

                        out.append(
                            {
                                "id": message_id,
                                "receipt_handle": message_id,
                                "body": body,
                                "topic": tp.topic,
                                "partition": tp.partition,
                                "offset": record.offset,
                            }
                        )

                        # Kafka commit semantics: the committed offset is
                        # "the next record to read", so we store
                        # offset + 1, not offset itself.
                        self._pending[message_id] = (
                            TopicPartition(tp.topic, tp.partition),
                            record.offset + 1,
                        )

                        if len(out) >= max_messages:
                            break
                    if len(out) >= max_messages:
                        break

            return out

        except Exception as e:
            raise QueueError(f"Kafka receive failed: {e}")

    def _commit(self, message_id: str, queue_name: str) -> bool:
        from kafka import OffsetAndMetadata

        pending = self._pending.pop(message_id, None)
        if pending is None:
            # Unknown or already-committed id -- treat as a no-op success.
            # Matches VercelQueueAdapter/BlockchainQueueAdapter's existing
            # convention of a redundant ack being harmless rather than an
            # error.
            return False

        tp, next_offset = pending
        consumer = self._consumers.get(queue_name)
        if consumer is None:
            raise QueueError(
                f"No active Kafka consumer for topic '{queue_name}' to commit offset against"
            )

        try:
            consumer.commit({tp: OffsetAndMetadata(next_offset, None)})
            return True
        except Exception as e:
            raise QueueError(f"Kafka offset commit failed: {e}")

    def delete(self, message_id: str, queue_name: str = "default", pop_receipt: str = "") -> bool:
        """
        Delete == commit the offset. Kafka has no notion of deleting a
        single record independent of consumer offsets, so -- matching the
        majority convention among this codebase's other adapters (SQS,
        Pub/Sub: ack is delete) -- delete() and ack() do the same thing.
        """
        return self._commit(message_id, queue_name)

    def ack(self, ack_id: str, queue_name: str = "default") -> bool:
        """Explicit ACK: commits the consumed offset for `ack_id`."""
        if not ack_id:
            raise QueueError("ack_id is required for Kafka ack")
        return self._commit(ack_id, queue_name)

    def close(self) -> None:
        """
        Flush the producer and close every consumer. Not part of
        QueueAdapter's abstract contract (none of the other adapters need
        it -- boto3/Azure SDK/Pub/Sub clients don't hold a persistent
        local socket the way a Kafka producer/consumer does), but good
        hygiene for a long-lived adapter instance to call explicitly.
        """
        if self._producer is not None:
            try:
                self._producer.close()
            except Exception:
                pass
        for consumer in self._consumers.values():
            try:
                consumer.close()
            except Exception:
                pass
