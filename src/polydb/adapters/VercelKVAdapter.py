# src/polydb/adapters/VercelKVAdapter.py

import os
import json
import redis
from typing import Any, Dict, List, Optional, Tuple

from ..json_safe import json_safe
from ..errors import NoSQLError, DatabaseError
from ..retry import retry
from ..types import JsonDict
from ..models import PartitionConfig
from ..base.NoSQLKVAdapter import NoSQLKVAdapter


class VercelKVAdapter(NoSQLKVAdapter):
    """
    Vercel KV adapter.

    Supports:
    • Local Redis (used in tests)
    • Vercel KV REST API (production)

    Tests run against redis://localhost:6380
    """

    def __init__(
        self,
        partition_config: Optional[PartitionConfig] = None,
        kv_url: str = "",
        kv_token: str = "",
        timeout: int = 10,
    ):
        super().__init__(partition_config)

        self.kv_url = kv_url or os.getenv("KV_REST_API_URL", "")
        self.kv_token = kv_token or os.getenv("KV_REST_API_TOKEN", "")
        self.timeout = timeout

        self._redis: Optional[redis.Redis] = None

        # detect local redis
        if self.kv_url.startswith("redis://"):
            self._redis = redis.from_url(self.kv_url, decode_responses=True)

    # ------------------------------------------------------------------
    # PUT
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        try:

            key = f"{pk}:{rk}"

            payload = dict(data)
            payload["_pk"] = pk
            payload["_rk"] = rk
            payload["id"] = pk

            value = json.dumps(payload, default=json_safe)

            # LOCAL REDIS
            if self._redis:
                self._redis.set(key, value)
                return payload

            # REST API (vercel production)
            import requests

            requests.post(
                f"{self.kv_url}/set/{key}",
                headers={"Authorization": f"Bearer {self.kv_token}"},
                json={"value": value},
                timeout=self.timeout,
            ).raise_for_status()

            return payload

        except Exception as e:
            raise NoSQLError(f"Vercel KV put failed: {e}")

    # ------------------------------------------------------------------
    # GET
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:

        try:

            key = f"{pk}:{rk}"

            # LOCAL REDIS
            if self._redis:

                value: Any = self._redis.get(key)

                if not value:
                    return None

                obj = json.loads(value)
                obj.setdefault("id", obj.get("_pk"))
                return obj

            # REST API
            import requests

            resp = requests.get(
                f"{self.kv_url}/get/{key}",
                headers={"Authorization": f"Bearer {self.kv_token}"},
                timeout=self.timeout,
            )

            if resp.status_code != 200:
                return None

            result = resp.json().get("result")

            if not result:
                return None

            obj = json.loads(result)
            obj.setdefault("id", obj.get("_pk"))

            return obj

        except Exception as e:
            raise NoSQLError(f"Vercel KV get failed: {e}")

    # ------------------------------------------------------------------
    # QUERY
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _query_raw(
        self,
        model: type,
        filters: Dict[str, Any],
        limit: Optional[int],
    ) -> List[JsonDict]:

        try:

            results: List[JsonDict] = []

            # LOCAL REDIS
            if self._redis:

                for key in self._redis.scan_iter("*"):

                    value: Any = self._redis.get(key)

                    if not value:
                        continue

                    obj = json.loads(value)

                    match = True

                    for k, v in filters.items():

                        if k == "id":
                            if obj.get("_pk") != v:
                                match = False
                                break

                        elif obj.get(k) != v:
                            match = False
                            break

                    if match:
                        obj.setdefault("id", obj.get("_pk"))
                        results.append(obj)

                    if limit and len(results) >= limit:
                        break

                return results

            # REST API fallback
            import requests

            resp = requests.get(
                f"{self.kv_url}/keys/*",
                headers={"Authorization": f"Bearer {self.kv_token}"},
                timeout=self.timeout,
            )

            if resp.status_code != 200:
                return []

            keys = resp.json().get("result", [])

            for key in keys:

                if limit and len(results) >= limit:
                    break

                get_resp = requests.get(
                    f"{self.kv_url}/get/{key}",
                    headers={"Authorization": f"Bearer {self.kv_token}"},
                    timeout=self.timeout,
                )

                if get_resp.status_code != 200:
                    continue

                result = get_resp.json().get("result")

                if not result:
                    continue

                obj = json.loads(result)

                match = True

                for k, v in filters.items():

                    if k == "id":
                        if obj.get("_pk") != v:
                            match = False
                            break

                    elif obj.get(k) != v:
                        match = False
                        break

                if match:
                    obj.setdefault("id", obj.get("_pk"))
                    results.append(obj)

            return results

        except Exception as e:
            raise NoSQLError(f"Vercel KV query failed: {e}")

    # ------------------------------------------------------------------
    # DELETE
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _delete_raw(
        self,
        model: type,
        pk: str,
        rk: str,
        etag: Optional[str],
    ) -> JsonDict:

        try:

            key = f"{pk}:{rk}"

            # LOCAL REDIS
            if self._redis:

                if not self._redis.exists(key):
                    raise DatabaseError(f"Item {pk}/{rk} does not exist")

                self._redis.delete(key)

                return {"id": pk}

            # REST API
            import requests

            resp = requests.get(
                f"{self.kv_url}/get/{key}",
                headers={"Authorization": f"Bearer {self.kv_token}"},
                timeout=self.timeout,
            )

            if resp.status_code != 200:
                raise DatabaseError(f"Item {pk}/{rk} does not exist")

            requests.delete(
                f"{self.kv_url}/del/{key}",
                headers={"Authorization": f"Bearer {self.kv_token}"},
                timeout=self.timeout,
            ).raise_for_status()

            return {"id": pk}

        except DatabaseError:
            raise

        except Exception as e:
            raise NoSQLError(f"Vercel KV delete failed: {e}")

    # ------------------------------------------------------------------
    # PAGINATION
    # ------------------------------------------------------------------

    def query_page(
        self,
        model: type,
        query: Dict[str, Any],
        page_size: int,
        continuation_token: Optional[str] = None,
    ) -> Tuple[List[JsonDict], Optional[str]]:

        rows = self._query_raw(model, query, None)

        start = 0

        if continuation_token:
            for i, r in enumerate(rows):
                if r["id"] == continuation_token:
                    start = i + 1
                    break

        page = rows[start : start + page_size]

        next_token = None

        if start + page_size < len(rows):
            next_token = page[-1]["id"]

        return page, next_token
