# src/polydb/adapters/mongodb.py
import os
import re
import threading
from typing import Any, Dict, List, Optional
from polydb.base.NoSQLKVAdapter import NoSQLKVAdapter

from ..errors import NoSQLError, ConnectionError
from ..retry import retry
from ..types import JsonDict
from ..models import PartitionConfig


class MongoDBAdapter(NoSQLKVAdapter):
    """MongoDB with overflow and LINQ"""

    def __init__(self, partition_config: Optional[PartitionConfig] = None):
        super().__init__(partition_config)
        self.mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        self.db_name = os.getenv("MONGODB_DATABASE", "default")
        self._client = None
        self._client_lock = threading.Lock()
        self._initialize_client()

    def _initialize_client(self):
        try:
            from pymongo import MongoClient

            with self._client_lock:
                if not self._client:
                    self._client = MongoClient(
                        self.mongo_uri,
                        maxPoolSize=int(os.getenv("MONGODB_MAX_POOL_SIZE", "10")),
                        minPoolSize=int(os.getenv("MONGODB_MIN_POOL_SIZE", "1")),
                        serverSelectionTimeoutMS=5000,
                    )
                    self._client.server_info()
                    self.logger.info("MongoDB initialized")
        except Exception as e:
            raise ConnectionError(f"MongoDB init failed: {str(e)}")

    def _get_collection(self, model: type):
        if not self._client:
            self._initialize_client()

        meta = getattr(model, "__polydb__", {})
        collection_name = meta.get("collection") or meta.get("table") or model.__name__.lower()
        return self._client[self.db_name][collection_name]  # type: ignore

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        try:
            data_copy = dict(data)
            data_copy["_pk"] = pk
            data_copy["_rk"] = rk

            collection = self._get_collection(model)
            result = collection.update_one(
                {"_pk": pk, "_rk": rk},
                {"$set": data_copy},
                upsert=True,
            )

            return {"_pk": pk, "_rk": rk}
        except Exception as e:
            raise NoSQLError(f"MongoDB put failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:
        try:
            collection = self._get_collection(model)
            doc = collection.find_one({"_pk": pk, "_rk": rk})

            if doc:
                doc.pop("_id", None)
                return doc

            return None
        except Exception as e:
            raise NoSQLError(f"MongoDB get failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _query_raw(
        self, model: type, filters: Dict[str, Any], limit: Optional[int]
    ) -> List[JsonDict]:
        try:
            collection = self._get_collection(model)

            query = {}
            for k, v in filters.items():
                if k.endswith("__gt"):
                    query[k[:-4]] = {"$gt": v}
                elif k.endswith("__gte"):
                    query[k[:-5]] = {"$gte": v}
                elif k.endswith("__lt"):
                    query[k[:-4]] = {"$lt": v}
                elif k.endswith("__lte"):
                    query[k[:-5]] = {"$lte": v}
                elif k.endswith("__in"):
                    query[k[:-4]] = {"$in": v}
                elif k.endswith("__contains"):
                    safe_pattern = re.escape(str(v))
                    query[k[:-10]] = {"$regex": safe_pattern, "$options": "i"}  # case-insensitive
                else:
                    query[k] = v

            cursor = collection.find(query)

            if limit:
                cursor = cursor.limit(limit)

            results = []
            for doc in cursor:
                doc.pop("_id", None)
                results.append(doc)

            return results
        except Exception as e:
            raise NoSQLError(f"MongoDB query failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _delete_raw(self, model: type, pk: str, rk: str, etag: Optional[str]) -> JsonDict:
        try:
            collection = self._get_collection(model)
            result = collection.delete_one({"_pk": pk, "_rk": rk})

            return {"deleted": result.deleted_count > 0, "_pk": pk, "_rk": rk}
        except Exception as e:
            raise NoSQLError(f"MongoDB delete failed: {str(e)}")

    def __del__(self):
        if self._client:
            try:
                self._client.close()
            except:
                pass
