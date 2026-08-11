# src/polydb/adapters/mongodb.py

from __future__ import annotations

import os
import re
import threading
from typing import Any, Dict, List, Optional

from ..base.NoSQLKVAdapter import NoSQLKVAdapter
from ..errors import NoSQLError, ConnectionError, DatabaseError
from ..retry import retry
from ..types import JsonDict
from ..models import PartitionConfig


def _as_literal(value: Any) -> Any:
    """Wrap a dict value so Mongo compares it instead of executing it.

    A bare dict on the right-hand side of a field is an *operator expression*
    to Mongo, so a filter value that came from request input can turn
    ``{"password": <value>}`` into ``{"password": {"$gt": ""}}`` - a match on
    every document, i.e. filter/auth bypass and full-collection extraction.
    ``{"$eq": {...}}`` is exactly the same comparison Mongo performs for a
    literal subdocument, so legitimate exact-match-on-subdocument queries keep
    working while ``$``-operators can no longer be smuggled in through a value.
    """
    return {"$eq": value} if isinstance(value, dict) else value


def _sanitize_query(query: Dict[str, Any]) -> Dict[str, Any]:
    """Apply _as_literal to every value of a caller-supplied Mongo query."""
    return {k: _as_literal(v) for k, v in (query or {}).items()}


class MongoDBAdapter(NoSQLKVAdapter):
    """MongoDB adapter compatible with PolyDB contract"""

    def __init__(
        self,
        partition_config: Optional[PartitionConfig] = None,
        mongo_uri: str = "",
        db_name: str = "",
    ):
        super().__init__(partition_config)

        self.mongo_uri = mongo_uri or os.getenv("MONGODB_URI", "mongodb://localhost:27017")

        self.db_name = db_name or os.getenv("MONGODB_DATABASE", "polydb")

        self._client = None
        self._lock = threading.Lock()
        self._indexed_collections: set = set()

        self._initialize_client()

    # -----------------------------------------------------
    # Client init
    # -----------------------------------------------------

    def _initialize_client(self):
        try:
            from pymongo import MongoClient

            with self._lock:
                if self._client:
                    return

                self._client = MongoClient(
                    self.mongo_uri,
                    maxPoolSize=int(os.getenv("MONGODB_MAX_POOL_SIZE", "10")),
                    minPoolSize=int(os.getenv("MONGODB_MIN_POOL_SIZE", "1")),
                    serverSelectionTimeoutMS=int(
                        os.getenv("MONGODB_SERVER_SELECTION_TIMEOUT_MS", "5000")
                    ),
                )

                self._client.server_info()

                self.logger.info("MongoDB initialized")

        except Exception as e:
            raise ConnectionError(f"MongoDB init failed: {e}")

    # -----------------------------------------------------
    # Collection helper
    # -----------------------------------------------------

    def _get_collection(self, model: type):
        if not self._client:
            self._initialize_client()

        meta = getattr(model, "__polydb__", {}) or {}

        collection_name = meta.get("collection") or meta.get("table") or model.__name__.lower()

        collection = self._client[self.db_name][collection_name]  # type: ignore

        self._ensure_pk_rk_index(collection, collection_name)

        return collection

    def _ensure_pk_rk_index(self, collection, collection_name: str) -> None:
        """
        Create the compound (_pk, _rk) index once per collection.

        Without it, every _get_raw/_put_raw lookup and query_page's
        `.sort("_pk")` scan the whole collection -- on Cosmos DB's Mongo
        API that's a cross-partition fan-out, billed and latent
        accordingly. Best-effort: a provider that rejects index creation
        (e.g. restricted tier) shouldn't take the adapter down, so a
        failure here is logged once and not retried on every call.
        Note: this is a logical uniqueness/query index only -- it does
        NOT retroactively set Cosmos's physical shard key, which can only
        be chosen at container-creation time.
        """
        if collection_name in self._indexed_collections:
            return

        with self._lock:
            if collection_name in self._indexed_collections:
                return

            try:
                collection.create_index(
                    [("_pk", 1), ("_rk", 1)], unique=True, name="pk_rk_unique"
                )
            except Exception:
                self.logger.warning(
                    "Could not ensure (_pk, _rk) index on %s",
                    collection_name,
                    exc_info=True,
                )
            finally:
                self._indexed_collections.add(collection_name)

    # -----------------------------------------------------
    # PUT
    # -----------------------------------------------------
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        try:
            collection = self._get_collection(model)

            payload = dict(data or {})
            payload["_pk"] = pk
            payload["_rk"] = rk
            # _rk is the record's natural identifier (the base class
            # already falls back to `id` when a model has no explicit
            # sort_key); _pk is just the tenant/partition grouping, so it
            # must never overwrite a caller-supplied `id`.
            payload.setdefault("id", rk)

            collection.update_one(
                {"_pk": pk, "_rk": rk},
                {"$set": payload},
                upsert=True,
            )

            # return full stored row (tests expect this)
            result = dict(payload)
            result.pop("_pk", None)
            result.pop("_rk", None)

            return result

        except Exception as e:
            raise NoSQLError(f"MongoDB put failed: {e}")

    # -----------------------------------------------------
    # GET
    # -----------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:
        try:
            collection = self._get_collection(model)

            doc = collection.find_one({"_pk": pk, "_rk": rk})

            if not doc:
                return None

            doc.pop("_id", None)
            doc.setdefault("id", rk)

            return doc

        except Exception as e:
            raise NoSQLError(f"MongoDB get failed: {e}")

    # -----------------------------------------------------
    # QUERY
    # -----------------------------------------------------
    def query_page(
        self,
        model: type,
        query: Dict[str, Any],
        page_size: int,
        continuation_token: Optional[str] = None,
    ):
        try:
            collection = self._get_collection(model)

            # Sanitize before adding our own operator clause below, so the
            # pagination cursor isn't stripped along with injected operators.
            query = _sanitize_query(query)

            if continuation_token:
                query["_pk"] = {"$gt": continuation_token}

            cursor = collection.find(query).sort("_pk", 1).limit(page_size)

            rows = []

            for doc in cursor:
                doc.pop("_id", None)
                doc.setdefault("id", doc.get("_rk"))
                rows.append(doc)

            if not rows:
                return [], None

            next_token = None

            if len(rows) == page_size:
                next_token = rows[-1]["id"]

            return rows, next_token

        except Exception as e:
            raise NoSQLError(f"MongoDB query_page failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _query_raw(
        self, model: type, filters: Dict[str, Any], limit: Optional[int]
    ) -> List[JsonDict]:
        try:
            collection = self._get_collection(model)

            query: Dict[str, Any] = {}

            for k, v in (filters or {}).items():

                if k == "id":
                    query["_rk"] = _as_literal(v)
                    continue

                if k.endswith("__gt"):
                    query[k[:-4]] = {"$gt": v}

                elif k.endswith("__gte"):
                    query[k[:-5]] = {"$gte": v}

                elif k.endswith("__lt"):
                    query[k[:-4]] = {"$lt": v}

                elif k.endswith("__lte"):
                    query[k[:-5]] = {"$lte": v}

                elif k.endswith("__in"):
                    # $in operands are matched literally by Mongo, but the list
                    # itself has to be a list - a dict here would be read as an
                    # operator document.
                    query[k[:-4]] = {"$in": list(v) if isinstance(v, (list, tuple, set)) else [v]}

                elif k.endswith("__contains"):
                    safe_pattern = re.escape(str(v))
                    query[k[:-10]] = {
                        "$regex": safe_pattern,
                        "$options": "i",
                    }

                else:
                    query[k] = _as_literal(v)

            cursor = collection.find(query)

            if limit:
                cursor = cursor.limit(limit)

            results: List[JsonDict] = []

            for doc in cursor:
                doc.pop("_id", None)
                doc.setdefault("id", doc.get("_rk"))
                results.append(doc)

            return results

        except Exception as e:
            raise NoSQLError(f"MongoDB query failed: {e}")

    # -----------------------------------------------------
    # DELETE
    # -----------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _delete_raw(self, model: type, pk: str, rk: str, etag: Optional[str]) -> JsonDict:
        try:
            collection = self._get_collection(model)

            result = collection.delete_one({"_pk": pk, "_rk": rk})

            if result.deleted_count == 0:
                raise DatabaseError(f"Item {pk}/{rk} does not exist")

            return {"id": rk}

        except DatabaseError:
            raise

        except Exception as e:
            raise NoSQLError(f"MongoDB delete failed: {e}")

    # -----------------------------------------------------
    # Close connection
    # -----------------------------------------------------

    def __del__(self):
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
