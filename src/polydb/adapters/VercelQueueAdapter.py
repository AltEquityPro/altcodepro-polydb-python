import os
import json
import requests
from typing import Dict, Any, List
from ..errors import QueueError
from ..retry import retry


class VercelQueueAdapter:

    def __init__(self, url: str = "", token: str = ""):
        self.url = url or os.getenv("KV_REST_API_URL")
        self.token = token or os.getenv("KV_REST_API_TOKEN")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        try:

            payload = json.dumps(message)

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
    def get_queue(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict]:

        try:

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

    def delete(self, message_id: str, queue_name: str = "default") -> bool:
        return True
