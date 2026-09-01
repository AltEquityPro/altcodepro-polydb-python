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

    def _table_name(self, model: type) -> str:
        """Same convention DynamoDBAdapter/_table_name and
        FirestoreAdapter/_collection_name already use -- but here it's not
        optional. Unlike DynamoDB (a real per-model table) or Firestore (a
        real per-model collection), Vercel KV/Redis is one flat keyspace: a
        key that was just "{pk}:{rk}" (tenant_id:id) let two different
        models sharing a tenant_id and, coincidentally, the same id
        collide on the exact same physical key -- one model's create
        silently overwriting an unrelated model's row. Reproduced directly:
        a "notes" row and a "widgets" row both keyed "tenant-a:x1" and the
        second create clobbered the first. Folding the table name into the
        key is the fix; every _*_raw method below must use this, not the
        bare "{pk}:{rk}" shape.
        """
        meta = getattr(model, "__polydb__", {})
        return meta.get("table") or meta.get("collection") or model.__name__.lower()

    # ------------------------------------------------------------------
    # PUT
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        try:

            key = f"{self._table_name(model)}:{pk}:{rk}"

            payload = dict(data)
            payload["_pk"] = pk
            payload["_rk"] = rk
            # NOTE: "id" is intentionally left as whatever the caller supplied
            # (already present via `dict(data)` above). NoSQLKVAdapter._get_pk_rk
            # derives `rk` from data.get(rk_field, ...) with rk_field defaulting
            # to "id" -- i.e. the write-side convention is that a model's "id"
            # field means "row key", not partition key. Overwriting it with
            # `pk` here corrupted the caller's own id field and made it
            # inconsistent with the read-side lookups below, which key on the
            # record's real "id" value rather than the partition key.
            payload.setdefault("id", rk)

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

            key = f"{self._table_name(model)}:{pk}:{rk}"

            # LOCAL REDIS
            if self._redis:

                value: Any = self._redis.get(key)

                if not value:
                    return None

                obj = json.loads(value)
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
            table = self._table_name(model)

            # LOCAL REDIS
            if self._redis:

                for key in self._redis.scan_iter(f"{table}:*"):

                    value: Any = self._redis.get(key)

                    if not value:
                        continue

                    obj = json.loads(value)

                    match = True

                    for k, v in filters.items():

                        if obj.get(k) != v:
                            match = False
                            break

                    if match:
                        results.append(obj)

                    if limit and len(results) >= limit:
                        break

                return results

            # REST API fallback
            import requests

            resp = requests.get(
                f"{self.kv_url}/keys/{table}:*",
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

                    if obj.get(k) != v:
                        match = False
                        break

                if match:
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

            key = f"{self._table_name(model)}:{pk}:{rk}"

            # LOCAL REDIS
            if self._redis:

                if not self._redis.exists(key):
                    raise DatabaseError(f"Item {pk}/{rk} does not exist")

                self._redis.delete(key)

                # "id" means row key throughout this adapter (see _put_raw) --
                # return rk, not pk, for consistency.
                return {"id": rk}

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

            return {"id": rk}

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
