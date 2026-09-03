# src/polydb/adapters/AzureQueueAdapter.py

import os
import threading
import json
import re

from typing import Any, Dict, List, Optional


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

        self._client = None
        self._queues = {}

        self._lock = threading.Lock()

        self._initialize_client()

    def _normalize_queue_name(self, name: str) -> str:
        name = name.lower()
        name = re.sub(r"[^a-z0-9-]", "-", name)  # replace invalid chars
        name = re.sub(r"-+", "-", name)  # collapse multiple dashes
        return name.strip("-")

    @staticmethod
    def _encode_receipt(message_id: str, pop_receipt: str) -> str:
        # Azure needs BOTH message_id and pop_receipt to delete/update a
        # message, but the generic queue contract (WorkerPool, SQSAdapter)
        # only ever carries a single opaque `receipt_handle` string end to
        # end. Pack both into one string here so ack()/nack() can recover
        # them without any caller needing to know Azure's two-part scheme.
        return f"{message_id}|{pop_receipt}"

    @staticmethod
    def _decode_receipt(receipt_handle: str) -> "tuple[str, str]":
        message_id, _, pop_receipt = (receipt_handle or "").partition("|")
        return message_id, pop_receipt

    def _initialize_client(self) -> None:
        """Initialize Azure Queue client"""
        from azure.storage.queue import QueueServiceClient

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

    def _get_queue(self, queue_name: str):
        """Get or create queue client"""
        from azure.core.exceptions import ResourceExistsError

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

    # How long a delivered message stays invisible to other consumers before
    # Azure hands it out again. The SDK's own default is 30s — far shorter
    # than a real task (an LLM generation call alone can run 30-120s+), so
    # a still-in-flight message was becoming visible again and getting
    # picked up by a second worker mid-processing: two concurrent
    # executions of the same task, one of which stomps/duplicates the
    # other's result. ack() deletes the message immediately on completion
    # regardless; a genuinely crashed worker recovers it after this timeout.
    #
    # This MUST be >= the longest a consumer may legitimately hold a message.
    # It was 300s while the platform's durable tasks are allowed 3600s
    # (execution/durable_job.py: timeout_seconds=3600.0), so every task that
    # ran longer than five minutes was redelivered WHILE STILL RUNNING -- and
    # again at ten, at fifteen, each redelivery starting another concurrent
    # execution of the same job. Observed on an artifact generation run: the
    # second invocation found the first's per-artifact lock held, recorded the
    # artifact as `already_running`, and carried on to the next one, so the
    # first artifact sat at `running` forever while its dependents blocked on
    # it. CPU pegged, RSS climbed with every duplicate, and the process was
    # eventually OOM-killed. The de-duplication guard upstream could not save
    # it: a redelivery carries the SAME run id as the original, so nothing in
    # the payload distinguishes them.
    #
    # Matched to the durable task timeout deliberately: the queue must not
    # reclaim a message the executor is still allowed to be working on. The
    # cost is that a hard worker crash now leaves the message invisible for up
    # to an hour before recovery, which is the right trade -- a delayed retry
    # is recoverable, two concurrent runs mutating the same rows are not.
    DEFAULT_VISIBILITY_TIMEOUT = int(
        os.environ.get("POLYDB_QUEUE_VISIBILITY_TIMEOUT") or 3600
    )

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def receive(
        self,
        queue_name: str = "default",
        max_messages: int = 1,
        visibility_timeout: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Receive messages"""
        try:

            queue_name = self._normalize_queue_name(queue_name)
            queue_client = self._get_queue(queue_name)

            messages = queue_client.receive_messages(
                max_messages=max_messages,
                visibility_timeout=(
                    visibility_timeout
                    if visibility_timeout is not None
                    else self.DEFAULT_VISIBILITY_TIMEOUT
                ),
            )

            results = []

            for msg in messages:
                payload = json.loads(msg.content)
                results.append(
                    {
                        "id": msg.id,
                        "pop_receipt": msg.pop_receipt,
                        # WorkerPool (and the generic UDL queue contract)
                        # reads this key to ack/nack. Without it, ack() is
                        # always skipped (falsy receipt_handle), the message
                        # is never deleted, and it silently reappears once
                        # Azure's visibility timeout elapses — replaying the
                        # same task forever even after it already ran.
                        "receipt_handle": self._encode_receipt(msg.id, msg.pop_receipt),
                        # How many times Azure has handed this same message
                        # out (1 on first delivery). Lets a consumer (e.g.
                        # WorkerPool) detect a poison message that keeps
                        # failing to ack/process and dead-letter it instead
                        # of retrying forever.
                        "dequeue_count": msg.dequeue_count,
                        "body": payload,
                    }
                )

            return results

        except Exception as e:
            raise QueueError(f"Azure Queue receive failed: {e}")

    def delete(self, message_id: str, queue_name: str = "default", pop_receipt: str = "") -> bool:
        """Delete message from queue"""
        from azure.core.exceptions import ResourceNotFoundError

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

        Generic callers (WorkerPool, storage_router.queue_ack) only pass a
        single `receipt_handle` positionally and never supply message_id —
        in that case `pop_receipt` here is actually the combined value
        produced by `receive()` (`"<message_id>|<pop_receipt>"`), so decode
        it rather than failing.
        """
        if not message_id:
            message_id, pop_receipt = self._decode_receipt(pop_receipt)
        if not message_id:
            raise QueueError("AzureQueueAdapter.ack requires message_id")
        queue_name = self._normalize_queue_name(queue_name)
        return self.delete(
            message_id=message_id,
            queue_name=queue_name,
            pop_receipt=pop_receipt,
        )

    def nack(
        self,
        queue_name: str,
        ack_id: str,
        *,
        delay: Optional[int] = None,
    ) -> bool:
        """
        Make a message visible again (optionally after `delay` seconds)
        without deleting it, instead of leaving it to reappear only once
        Azure's default visibility timeout elapses.

        `ack_id` is the combined `"<message_id>|<pop_receipt>"` receipt
        handle produced by `receive()`. Matches the positional order used
        by storage_router.queue_nack: nack(queue_name, ack_id, delay=...).
        """
        message_id, pop_receipt = self._decode_receipt(ack_id)
        if not message_id:
            raise QueueError("AzureQueueAdapter.nack requires a receipt_handle from receive()")
        try:
            queue_name = self._normalize_queue_name(queue_name)
            queue_client = self._get_queue(queue_name)
            queue_client.update_message(
                message_id,
                pop_receipt=pop_receipt,
                visibility_timeout=delay or 0,
            )
            return True
        except Exception as e:
            raise QueueError(f"Azure Queue nack failed: {e}")
