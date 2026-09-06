from polydb.utils import setup_logger


from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class QueueAdapter(ABC):
    """Base class for Queue/Message services"""

    def __init__(self, connection_string: str = ""):
        self.logger = setup_logger(self.__class__.__name__)

    @abstractmethod
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        """Send message to queue"""
        pass

    @abstractmethod
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict[str, Any]]:
        """Receive messages from queue"""
        pass

    @abstractmethod
    def delete(self, message_id: str, queue_name: str = "default", pop_receipt: str = "") -> bool:
        """Delete message from queue"""
        pass

    # ------------------------------------------------------------------
    # Extended queue management -- regular (non-abstract) methods, not
    # `@abstractmethod`, on purpose. `send`/`receive`/`delete` above are
    # the original contract every adapter (SQS/Azure/GCP/Vercel/
    # Blockchain/Kafka/RabbitMQ) already implements; making the methods
    # below abstract too would force every one of those subclasses to
    # grow a stub the instant this file changes, even the ones nobody has
    # actually implemented these for yet. Matches the same pattern
    # `NoSQLKVAdapter`/`SecretsAdapter` already use for their own
    # not-yet-universal capabilities (a bare `raise NotImplementedError`
    # in the base, overridden only where a subclass genuinely supports
    # it) -- a subclass that doesn't override one of these raises here,
    # loudly and by name, instead of silently doing nothing.
    # ------------------------------------------------------------------

    def nack(self, ack_id: str, queue_name: str = "default") -> bool:
        """Negative-acknowledge: put the message back for redelivery
        immediately, without waiting out any natural visibility/
        redelivery timeout the backend might otherwise enforce."""
        raise NotImplementedError(f"{self.__class__.__name__} does not implement nack()")

    def purge(self, queue_name: str = "default") -> int:
        """Delete every message currently in the queue. Returns the
        number purged where the backend reports one; adapters that
        implement this and genuinely cannot get a count from the backend
        document their own sentinel return value in their override --
        this base default never guesses one for an adapter nobody has
        implemented purge() for yet."""
        raise NotImplementedError(f"{self.__class__.__name__} does not implement purge()")

    def declare(
        self,
        queue_name: str = "default",
        *,
        durable: bool = True,
        dead_letter_queue: Optional[str] = None,
    ) -> bool:
        """Explicit queue provisioning ahead of first use -- distinct
        from the auto-create-on-first-send/receive some adapters already
        do implicitly. When `dead_letter_queue` is given, wires up
        dead-lettering to a second queue by that name (adapter-specific
        mechanism; see each override for what "dead-lettering" actually
        means on that backend)."""
        raise NotImplementedError(f"{self.__class__.__name__} does not implement declare()")

    def status(self, queue_name: str = "default") -> Dict[str, Any]:
        """At minimum `{"message_count": int}`; an override may add other
        fields the backend genuinely exposes."""
        raise NotImplementedError(f"{self.__class__.__name__} does not implement status()")

    # ------------------------------------------------------------------
    # extend / delay / cancel -- the same "regular method, not abstract,
    # base raises loudly by name" shape as nack/purge/declare/status
    # above, for the identical reason: adding these to every existing
    # subclass at once the moment this file changes would be a false
    # claim of support nobody actually verified. Each is real ONLY where
    # a genuine backend primitive exists for it -- checked per adapter,
    # per method, against the real installed SDK, not assumed uniform:
    #   - extend(): SQS (ChangeMessageVisibility), Azure Queue
    #     (UpdateMessage with no content= change), GCP Pub/Sub
    #     (ModifyAckDeadline) all have a REAL "keep this ALREADY-RECEIVED
    #     message invisible a while longer" primitive. RabbitMQ/Kafka do
    #     not -- AMQP's consumer model has no per-message renewable
    #     visibility timer at all (ack/nack/reject only), and Kafka's
    #     offset-based model has no per-message delivery state to extend
    #     either -- so neither adapter overrides this.
    #   - delay(): SQS (SendMessage's own DelaySeconds, real, capped at
    #     900s by SQS itself), Azure Queue (send_message's own
    #     visibility_timeout param), and RabbitMQ (a real, plugin-free
    #     AMQP pattern: publish to a per-duration queue declared with
    #     x-message-ttl + a dead-letter-exchange pointing at the real
    #     queue_name, so the message lands there automatically once the
    #     TTL elapses) all have a real "don't make this NEW message
    #     visible until N seconds from now" primitive. GCP Pub/Sub has no
    #     such primitive (a published message is deliverable immediately;
    #     Cloud Tasks/Scheduler is a genuinely different GCP service, out
    #     of scope for a Pub/Sub adapter) and Kafka doesn't either.
    #   - cancel(): only meaningful for a message a delay() call has not
    #     yet delivered. Azure Queue is real (the id+pop_receipt
    #     send_message's own response returns are already enough to
    #     delete that exact message before its visibility_timeout
    #     elapses). RabbitMQ is real too, via a bounded scan-and-requeue
    #     of the one specific per-duration delay queue this message was
    #     published to (see RabbitMQAdapter.cancel's own docstring for
    #     why this is the honest, bounded shape rather than an unbounded
    #     one). SQS has none: a delayed SQS message is not receivable
    #     (no ReceiptHandle exists) until its own DelaySeconds elapses,
    #     so there is no real API call that can remove it earlier.
    # ------------------------------------------------------------------

    def extend(
        self, ack_id: str, queue_name: str = "default", *, visibility_timeout: int = 30
    ) -> bool:
        """Extend how long an already-received, not-yet-acked message
        stays invisible to other consumers, without acknowledging or
        requeuing it -- the redelivery-window equivalent of "I'm still
        working on this, don't hand it to anyone else yet." `ack_id` is
        the same receipt/handle `receive()` returned for this message."""
        raise NotImplementedError(f"{self.__class__.__name__} does not implement extend()")

    def delay(
        self, message: Dict[str, Any], queue_name: str = "default", *, delay_seconds: int = 0
    ) -> str:
        """Send a NEW message that will not become visible/deliverable
        until `delay_seconds` from now. Returns an id `cancel()` can use
        to remove it again before that happens -- not necessarily the
        same string shape `send()` returns, since some backends (Azure)
        need more than a bare message id to cancel a not-yet-visible
        message."""
        raise NotImplementedError(f"{self.__class__.__name__} does not implement delay()")

    def cancel(self, message_id: str, queue_name: str = "default") -> bool:
        """Cancel a still-delayed message (one a prior `delay()` call has
        not yet made visible) before it is ever delivered. `message_id`
        is whatever `delay()` returned for it. Returns whether a
        matching still-pending message was actually found and removed --
        `False`, not an exception, when it already fired or never
        existed, the same "unknown id is a no-op, not an error"
        convention `RabbitMQAdapter._ack_delivery` already establishes
        for a different method."""
        raise NotImplementedError(f"{self.__class__.__name__} does not implement cancel()")
