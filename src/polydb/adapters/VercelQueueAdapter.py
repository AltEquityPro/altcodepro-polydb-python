import os
import json
import requests
import redis
from typing import Dict, Any, List, Optional

from ..base.QueueAdapter import QueueAdapter
from ..errors import QueueError
from ..retry import retry


class VercelQueueAdapter:

    def __init__(self, url: str = "", token: str = ""):
        self.url = url or os.getenv("KV_REST_API_URL")
        self.token = token or os.getenv("KV_REST_API_TOKEN")

        # Same local-testability convention VercelKVAdapter already
        # established: a plain redis:// URL means "use local Redis
        # directly," no cloud account or REST endpoint needed. Redis
        # Streams (XADD/XRANGE/XDEL) are the same primitive the REST
        # mode already speaks (see send/receive below), so this isn't a
        # different queue model locally vs. in production, just a
        # different transport to the same Redis Streams operations.
        self._redis: Optional["redis.Redis"] = None
        if self.url and self.url.startswith("redis://"):
            self._redis = redis.from_url(self.url, decode_responses=True)

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        try:
            payload = json.dumps(message)

            if self._redis:
                return self._redis.xadd(queue_name, {"payload": payload})

            r = requests.post(
                f"{self.url}/xadd/{queue_name}",
                headers={"Authorization": f"Bearer {self.token}"},
                json={"*": payload},
            )

            r.raise_for_status()

            return r.json()["result"]

        except Exception as e:
            raise QueueError(f"Vercel queue send failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict]:

        try:
            if self._redis:
                entries = self._redis.xrange(queue_name, count=max_messages)
                messages = []
                for entry_id, fields in entries:
                    data = json.loads(fields["payload"])
                    data["_id"] = entry_id
                    messages.append(data)
                return messages

            r = requests.get(
                f"{self.url}/xrange/{queue_name}/-/{max_messages}",
                headers={"Authorization": f"Bearer {self.token}"},
            )

            r.raise_for_status()

            result = r.json()["result"]

            messages = []

            for msg in result:
                data = json.loads(msg[1][0][1])
                data["_id"] = msg[0]
                messages.append(data)

            return messages

        except Exception as e:
            raise QueueError(f"Vercel queue receive failed: {e}")

    def delete(self, message_id: str, queue_name: str = "default", pop_receipt: str = "") -> bool:
        if self._redis:
            # Real deletion locally -- unlike the REST-mode no-op below
            # (Redis Streams via the Vercel KV REST API has no XDEL
            # endpoint exposed, same limitation ack() below documents for
            # explicit ACK), local Redis has the primitive natively.
            return bool(self._redis.xdel(queue_name, message_id))
        return True

    # ---------------------------------------------------------
    # ACK (same as delete for compatibility)
    # ---------------------------------------------------------
    def ack(self, ack_id: str, queue_name: str = "default") -> bool:
        """
        ACK for Vercel Queue.

        Since Redis Streams via Vercel KV REST API does not support
        explicit ACK without consumer groups, this is treated as no-op.

        Exists for interface consistency.
        """
        if not ack_id:
            raise QueueError("ack_id is required")

        return True
