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
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement nack()"
        )

    def purge(self, queue_name: str = "default") -> int:
        """Delete every message currently in the queue. Returns the
        number purged where the backend reports one; adapters that
        implement this and genuinely cannot get a count from the backend
        document their own sentinel return value in their override --
        this base default never guesses one for an adapter nobody has
        implemented purge() for yet."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement purge()"
        )

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
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement declare()"
        )

    def status(self, queue_name: str = "default") -> Dict[str, Any]:
        """At minimum `{"message_count": int}`; an override may add other
        fields the backend genuinely exposes."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement status()"
        )
