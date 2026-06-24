# src/polydb/adapters/AzureTableStorageAdapter.py

from __future__ import annotations

import os
import re
import json
import base64
import hashlib
import threading
import logging

from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID

from ..base.NoSQLKVAdapter import NoSQLKVAdapter
from ..json_safe import json_safe
from ..errors import NoSQLError, ConnectionError
from ..retry import retry
from ..types import JsonDict
from ..models import PartitionConfig

logger = logging.getLogger(__name__)

_BYTES_PREFIX = "@@polydb_bytes@@:"
_JSON_PREFIX = "@@polydb_json@@:"
_BASE64_RE = re.compile(r"^[A-Za-z0-9+/]*={0,2}$")

# ensures model isolation across the same table
_MODEL_FIELD = "__polydb_model__"

_CONNECT_TIMEOUT = float(os.getenv("AZURE_TABLE_CONNECT_TIMEOUT", "10"))
_READ_TIMEOUT = float(os.getenv("AZURE_TABLE_READ_TIMEOUT", "30"))
_RETRY_TOTAL = int(os.getenv("AZURE_TABLE_RETRY_TOTAL", "3"))

logging.getLogger("azure").setLevel(logging.ERROR)


class AzureTableStorageAdapter(NoSQLKVAdapter):
    """
    Azure Table Storage adapter with:
    - Any-type payload support (dict/list/custom objects -> JSON)
    - Key sanitization for PartitionKey/RowKey and property names
    - Query support for scalar fields
    - Blob overflow for entities > 1MB
    - Model isolation using __polydb_model__
    - Always returns id (derived from RowKey if missing)
    """

    AZURE_TABLE_MAX_SIZE = 60 * 1024  # 1MB
    _RESERVED = {"PartitionKey", "RowKey", "Timestamp", "etag", "ETag"}

    def __init__(
        self,
        partition_config: Optional[PartitionConfig] = None,
        connection_string: str = "",
        container_name="",
    ):
        super().__init__(partition_config)
        self.max_size = self.AZURE_TABLE_MAX_SIZE
        self.connection_string = (
            connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING") or ""
        )
        self.container_name = container_name or os.getenv("AZURE_CONTAINER_NAME", "overflow") or ""

        if not self.connection_string:
            raise ConnectionError("AZURE_STORAGE_CONNECTION_STRING must be set")

        self._client: Any = None
        self._blob_service = None
        self._client_lock = threading.Lock()

        self._ensured_tables: set = set()
        self._table_clients_cache: Dict[str, Any] = {}
        self._cache_lock = threading.Lock()

        self._initialize_client()

    def _initialize_client(self):
        try:
            from azure.data.tables import TableServiceClient
            from azure.storage.blob import BlobServiceClient

            with self._client_lock:
                if not self._client:
                    self._client = TableServiceClient.from_connection_string(
                        self.connection_string,
                        connection_timeout=_CONNECT_TIMEOUT,
                        read_timeout=_READ_TIMEOUT,
                        retry_total=_RETRY_TOTAL,
                    )
                    self._blob_service = BlobServiceClient.from_connection_string(
                        self.connection_string,
                        connection_timeout=_CONNECT_TIMEOUT,
                        read_timeout=_READ_TIMEOUT,
                        retry_total=_RETRY_TOTAL,
                    )

                    try:
                        self._blob_service.create_container(self.container_name)
                    except Exception:
                        pass

                    self.logger.info("Azure Table Storage initialized with Blob overflow")
        except Exception as e:
            raise ConnectionError(f"Azure Table init failed: {str(e)}")

    # -----------------------------
    # Key / property sanitization
    # -----------------------------

    def _sanitize_pk_rk(self, value: Any) -> str:
        s = str(value)
        s = re.sub(r"[\\/#\?\x00-\x1f\x7f:+ ]", "_", s)
        return s[:1024] if len(s) > 1024 else s

    def _sanitize_prop_name(self, name: Any) -> str:
        s = str(name)
        s = re.sub(r"[^A-Za-z0-9_]", "_", s)
        if not re.match(r"^[A-Za-z_]", s):
            s = f"f_{s}"
        if s in self._RESERVED:
            s = f"f_{s}"
        return s[:255] if len(s) > 255 else s

    # -----------------------------
    # Value encoding / decoding
    # -----------------------------

    def _encode_value(self, v: Any) -> Any:
        if v is None:
            return None

        if isinstance(v, (list, tuple, dict)) and len(v) == 0:
            return None

        if isinstance(v, bytes):
            return _BYTES_PREFIX + base64.b64encode(v).decode("ascii")

        if isinstance(v, (dict, list, tuple)):
            return _JSON_PREFIX + json.dumps(
                list(v) if isinstance(v, tuple) else v, default=json_safe
            )

        if isinstance(v, UUID):
            return str(v)

        if isinstance(v, Decimal):
            return float(v)

        if isinstance(v, date) and not isinstance(v, datetime):
            return v.isoformat()

        if isinstance(v, datetime):
            return v

        if isinstance(v, (str, bool, int, float)):
            return v

        return _JSON_PREFIX + json.dumps(v, default=json_safe)

    def _decode_value(self, v: Any) -> Any:
        if isinstance(v, str):
            if v.startswith(_JSON_PREFIX):
                payload = v[len(_JSON_PREFIX):]
                try:
                    return json.loads(payload)
                except Exception:
                    return v

            if v.startswith(_BYTES_PREFIX):
                payload = v[len(_BYTES_PREFIX):].strip()
                if (len(payload) % 4) == 1:
                    return v
                if not _BASE64_RE.match(payload):
                    return v
                pad = (-len(payload)) % 4
                if pad:
                    payload = payload + ("=" * pad)
                try:
                    return base64.b64decode(payload, validate=True)
                except TypeError:
                    try:
                        return base64.b64decode(payload)
                    except Exception:
                        return v
                except Exception:
                    return v
            return v

        if isinstance(v, dict) and "__type__" in v:
            t = v.get("__type__")
            if t == "json":
                raw = v.get("value")
                try:
                    return json.loads(raw or "null")
                except Exception:
                    return raw
            if t == "bytes":
                payload = (v.get("b64") or "").strip()
                if (len(payload) % 4) == 1:
                    return b""
                if payload and not _BASE64_RE.match(payload):
                    return b""
                pad = (-len(payload)) % 4
                if pad:
                    payload = payload + ("=" * pad)
                try:
                    return base64.b64decode(payload, validate=True)
                except TypeError:
                    try:
                        return base64.b64decode(payload)
                    except Exception:
                        return b""
                except Exception:
                    return b""
            if t == "str":
                return v.get("value")

        return v

    # -----------------------------
    # Entity pack/unpack
    # -----------------------------
    def _pack_entity(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        entity: JsonDict = {
            "PartitionKey": pk,
            "RowKey": rk,
        }
        entity[_MODEL_FIELD] = model.__qualname__
        keymap: Dict[str, str] = {}

        for orig_key, orig_val in (data or {}).items():
            orig_key_str = str(orig_key)

            if orig_key_str in self._RESERVED or orig_key_str in ("PartitionKey", "RowKey"):
                continue

            skey = self._sanitize_prop_name(orig_key)

            base = skey
            counter = 1
            while skey in entity:
                skey = f"{base}_{counter}"
                counter += 1

            keymap[skey] = orig_key_str

            entity[skey] = self._encode_value(orig_val)

        if keymap:
            entity["__keymap__"] = json.dumps(keymap, default=json_safe)

        return entity

    def _unpack_entity(self, entity: JsonDict) -> JsonDict:
        if not entity:
            return {}

        raw = dict(entity)
        raw.pop("etag", None)
        raw.pop("ETag", None)
        raw.pop("Timestamp", None)

        keymap_str = raw.pop("__keymap__", None)
        keymap: Dict[str, str] = {}
        if keymap_str:
            try:
                keymap = json.loads(keymap_str)
            except Exception:
                keymap = {}

        out: JsonDict = {}

        pk = raw.get("PartitionKey")
        rk = raw.get("RowKey")
        if pk is not None:
            out["PartitionKey"] = pk
        if rk is not None:
            out["RowKey"] = rk

        for k, v in raw.items():
            if k in ("PartitionKey", "RowKey"):
                continue

            if k.startswith("_") or k in (_MODEL_FIELD,):
                out[k] = v
                continue

            orig_key = keymap.get(k, k)
            out[orig_key] = self._decode_value(v)

        if "id" not in out and rk is not None:
            out["id"] = rk

        return out

    def _sanitize_blob_part(self, value: str) -> str:
        s = str(value).lower()
        s = re.sub(r"[^a-z0-9\-]", "-", s)
        s = re.sub(r"-+", "-", s)
        return s.strip("-")[:100]

    def _entity_size_bytes(self, entity: JsonDict) -> int:
        return len(json.dumps(entity, default=json_safe).encode("utf-8"))

    def _blob_key(self, pk: str, rk: str, checksum: str) -> str:
        safe_pk = self._sanitize_blob_part(pk)
        safe_rk = self._sanitize_blob_part(rk)
        return f"{safe_pk}-{safe_rk}-{checksum}.json"

    def _blob_upload(self, blob_key: str, data_bytes: bytes):
        if not self._blob_service:
            return
        blob_client = self._blob_service.get_blob_client(self.container_name, blob_key)
        blob_client.upload_blob(data_bytes, overwrite=True)

    def _blob_download(self, blob_key: str) -> bytes:
        if not self._blob_service:
            raise NoSQLError("Blob service not initialized")
        blob_client = self._blob_service.get_blob_client(self.container_name, blob_key)
        return blob_client.download_blob().readall()

    def _blob_delete(self, blob_key: str):
        if not self._blob_service:
            return
        blob_client = self._blob_service.get_blob_client(self.container_name, blob_key)
        try:
            blob_client.delete_blob()
        except Exception:
            pass

    # -----------------------------
    # Required NoSQLKVAdapter hooks
    # -----------------------------
    def _sanitize_table_name(self, name: str) -> str:
        if not name:
            return "defaulttable"

        sanitized = re.sub(r"[^a-zA-Z0-9]", "", name)

        if sanitized and sanitized[0].isdigit():
            sanitized = "t" + sanitized

        if len(sanitized) < 3:
            sanitized = sanitized + "table"
        if len(sanitized) > 63:
            sanitized = sanitized[:63]

        return sanitized.lower()

    def _get_table_name(self, model: type) -> str:
        definition = getattr(model, "__udl_definition__", None)
        if definition:
            metadata = getattr(definition, "x_metadata", {}) or {}
            collection_name = metadata.get("collection_name")
            if collection_name:
                return self._sanitize_table_name(collection_name)

        polydb_meta = getattr(model, "__polydb__", None)
        if isinstance(polydb_meta, dict):
            collection = polydb_meta.get("collection") or polydb_meta.get("collection_name")
            if collection:
                return self._sanitize_table_name(str(collection))

        return os.getenv("AZURE_TABLE_NAME", "defaulttable") or "defaulttable"

    def _get_table_client(self, model: type):
        table_name = self._get_table_name(model)

        cached = self._table_clients_cache.get(table_name)
        if cached is not None and table_name in self._ensured_tables:
            return cached

        with self._cache_lock:
            cached = self._table_clients_cache.get(table_name)
            already_ensured = table_name in self._ensured_tables

            if not already_ensured:
                try:
                    self._client.create_table_if_not_exists(table_name)
                    logger.info(f"✅ Azure Table ensured/created: {table_name}")
                except Exception as e:
                    msg = str(e)
                    if "TableAlreadyExists" not in msg and "already exists" not in msg.lower():
                        logger.warning(f"Could not create table {table_name}: {e}")
                self._ensured_tables.add(table_name)

            if cached is None:
                cached = self._client.get_table_client(table_name=table_name)
                self._table_clients_cache[table_name] = cached

            return cached

    def _restore_overflow_properties(self, entity_dict: JsonDict) -> JsonDict:
        restored = {}

        for k, v in entity_dict.items():
            if k.startswith("_"):
                restored[k] = v
                continue

            if isinstance(v, str) and v.startswith("{") and '"_overflow":' in v:
                try:
                    metadata = json.loads(v)
                    if not metadata.get("_overflow"):
                        restored[k] = v
                        continue

                    blob_key = metadata.get("_blob_key")
                    checksum = metadata.get("_checksum")

                    if not blob_key:
                        restored[k] = v
                        continue

                    blob_data = self._blob_download(blob_key)

                    if checksum:
                        actual_checksum = hashlib.md5(blob_data).hexdigest()
                        if actual_checksum != checksum:
                            logger.warning(
                                f"Checksum mismatch for blob {blob_key} (property '{k}')"
                            )

                    actual_value = json.loads(blob_data.decode("utf-8"))
                    restored[k] = actual_value
                    logger.debug(f"Restored large property '{k}' from blob: {blob_key}")

                    continue

                except Exception as e:
                    logger.error(f"Failed to restore overflow property '{k}': {e}")
                    restored[k] = v
                    continue

            restored[k] = v

        return restored

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        try:
            table_client = self._get_table_client(model)
            safe_pk = self._sanitize_pk_rk(pk)
            safe_rk = self._sanitize_pk_rk(rk)
            entity = self._pack_entity(model, safe_pk, safe_rk, data)
            MAX_PROPERTY_CHARS = 30 * 1024

            def _is_large_string(val: Any) -> bool:
                return isinstance(val, str) and len(val) > MAX_PROPERTY_CHARS

            large_val_dict = {}
            for key, value in entity.items():
                if _is_large_string(value):
                    logger.warning(
                        f"LARGE PROPERTY DETECTED: {key} length={len(value)} chars → forcing blob overflow"
                    )
                    payload_json = json.dumps(value, default=json_safe)
                    payload_bytes = payload_json.encode("utf-8")
                    payload_size = len(payload_bytes)
                    checksum = hashlib.md5(payload_bytes).hexdigest()
                    val_key = self._sanitize_blob_part(key)
                    blob_key = self._blob_key(f"{safe_pk}_{safe_rk}", val_key, checksum)
                    self._blob_upload(blob_key, payload_bytes)
                    logger.info(f"Stored in blob: {blob_key} ({payload_size // 1024} KB)")
                    large_val_dict[key] = {
                        "PartitionKey": safe_pk,
                        "RowKey": safe_rk,
                        _MODEL_FIELD: model.__qualname__,
                        "_overflow": True,
                        "_blob_key": blob_key,
                        "_size": payload_size,
                        "_checksum": checksum,
                    }

            reference_entity = {
                _MODEL_FIELD: model.__qualname__,
            }
            for k, v in entity.items():
                if k.startswith("_"):
                    continue
                if k in large_val_dict:
                    metadata = large_val_dict[k]
                    reference_entity[k] = json.dumps(metadata, default=json_safe)
                    logger.info(f"Overflow reference stored for {k} → {metadata['_blob_key']}")
                else:
                    reference_entity[k] = v

            table_client.upsert_entity(reference_entity)

            restored = self._unpack_entity(entity)
            restored["id"] = safe_rk

            return restored

        except Exception as e:
            if "PropertyValueTooLarge" in str(e):
                raise NoSQLError("Azure Table limit exceeded → must use blob overflow") from e

            raise NoSQLError(f"Azure Table put failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:
        try:
            safe_pk = self._sanitize_pk_rk(pk)
            safe_rk = self._sanitize_pk_rk(rk)
            table_client = self._get_table_client(model)

            entity = table_client.get_entity(safe_pk, safe_rk)
            entity_dict = dict(entity)

            if entity_dict.get(_MODEL_FIELD) != model.__qualname__:
                return None

            restored_entity = self._restore_overflow_properties(entity_dict)

            out = self._unpack_entity(restored_entity)
            if "id" not in out:
                out["id"] = safe_rk

            return out

        except Exception as e:
            if "ResourceNotFound" in str(e):
                return None
            raise NoSQLError(f"Azure Table get failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _query_raw(
        self, model: type, filters: Dict[str, Any], limit: Optional[int]
    ) -> List[JsonDict]:
        try:
            table_client = self._get_table_client(model)

            eff_filters = dict(filters or {})
            parts: List[str] = []
            for orig_k, orig_v in eff_filters.items():
                if orig_v is None:
                    continue

                if orig_k in ("partition_key", "PartitionKey"):
                    sk = "PartitionKey"
                elif orig_k in ("row_key", "RowKey"):
                    sk = "RowKey"
                else:
                    sk = (
                        self._sanitize_prop_name(orig_k) if orig_k != _MODEL_FIELD else _MODEL_FIELD
                    )

                ev = self._encode_value(orig_v)

                if ev is None:
                    parts.append(f"{sk} eq null")
                elif isinstance(ev, bool):
                    parts.append(f"{sk} eq {str(ev).lower()}")
                elif isinstance(ev, (int, float)):
                    parts.append(f"{sk} eq {ev}")
                elif isinstance(ev, datetime):
                    iso = ev.isoformat()
                    if not iso.endswith("Z"):
                        iso = iso + "Z"
                    parts.append(f"{sk} eq datetime'{iso}'")
                else:
                    sval = str(ev).replace("'", "''")
                    parts.append(f"{sk} eq '{sval}'")

            query_filter = " and ".join(parts) if parts else None

            entities = table_client.query_entities(query_filter=query_filter)

            results: List[JsonDict] = []
            count = 0
            for ent in entities:
                ent_dict = dict(ent)

                restored_entity = self._restore_overflow_properties(ent_dict)

                out = self._unpack_entity(restored_entity)

                if "id" not in out and "RowKey" in ent_dict:
                    out["id"] = ent_dict["RowKey"]

                results.append(out)

                count += 1
                if limit and count >= limit:
                    break

            return results

        except Exception as e:
            raise NoSQLError(f"Azure Table query failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _delete_raw(self, model: type, pk: str, rk: str, etag: Optional[str]) -> JsonDict:
        try:
            table_client = self._get_table_client(model)
            safe_pk = self._sanitize_pk_rk(pk)
            safe_rk = self._sanitize_pk_rk(rk)

            try:
                entity = table_client.get_entity(safe_pk, safe_rk)
                entity_dict = dict(entity)

                if entity_dict.get(_MODEL_FIELD) != model.__qualname__:
                    return {"deleted": False}

                if entity_dict.get("_overflow"):
                    blob_key = entity_dict.get("_blob_key")
                    if blob_key:
                        self._blob_delete(blob_key)
            except Exception:
                pass

            table_client.delete_entity(safe_pk, safe_rk, etag=etag)
            return {"deleted": True, "PartitionKey": safe_pk, "RowKey": safe_rk, "id": safe_rk}

        except Exception as e:
            raise NoSQLError(f"Azure Table delete failed: {str(e)}")

    # -----------------------------
    # Pagination overrides
    # -----------------------------

    @property
    def capabilities(self):
        from ..models import BackendCapabilities
        return BackendCapabilities(
            server_order=False,
            server_filter=True,
            native_cursor=True,
            supports_count=False,
        )

    def query_paged(self, model: type, request) -> "PageResult":
        """Use Azure native continuation tokens when no ORDER BY; fall back to in-memory sort."""
        from ..models import PageResult

        if request.order_by:
            # Azure Table has no ORDER BY — fall back to in-memory sort via base class
            return super().query_paged(model, request)

        table_client = self._get_table_client(model)
        eff_filters = dict(request.filters or {})
        parts: List[str] = []
        for orig_k, orig_v in eff_filters.items():
            if orig_v is None:
                continue
            if orig_k in ("partition_key", "PartitionKey"):
                sk = "PartitionKey"
            elif orig_k in ("row_key", "RowKey"):
                sk = "RowKey"
            else:
                sk = self._sanitize_prop_name(orig_k)
            ev = self._encode_value(orig_v)
            if ev is None:
                parts.append(f"{sk} eq null")
            elif isinstance(ev, bool):
                parts.append(f"{sk} eq {str(ev).lower()}")
            elif isinstance(ev, (int, float)):
                parts.append(f"{sk} eq {ev}")
            elif isinstance(ev, datetime):
                iso = ev.isoformat()
                if not iso.endswith("Z"):
                    iso += "Z"
                parts.append(f"{sk} eq datetime'{iso}'")
            else:
                sval = str(ev).replace("'", "''")
                parts.append(f"{sk} eq '{sval}'")
        query_filter = " and ".join(parts) if parts else None

        azure_ct = None
        if request.cursor:
            cd = self._decode_cursor(request.cursor)
            if cd.get("type") == "azure":
                azure_ct = cd.get("token")

        try:
            paged = table_client.query_entities(
                query_filter=query_filter,
                results_per_page=request.limit,
            )
            page_iter = paged.by_page(continuation_token=azure_ct)
            try:
                page = next(iter(page_iter))
                items = []
                for ent in page:
                    ent_dict = dict(ent)
                    restored = self._restore_overflow_properties(ent_dict)
                    out = self._unpack_entity(restored)
                    if "id" not in out and "RowKey" in ent_dict:
                        out["id"] = ent_dict["RowKey"]
                    items.append(out)
                next_azure_ct = page_iter.continuation_token
            except StopIteration:
                items = []
                next_azure_ct = None
        except Exception as e:
            raise NoSQLError(f"Azure Table paged query failed: {str(e)}")

        if request.fields:
            field_set = set(request.fields)
            items = [{k: v for k, v in item.items() if k in field_set} for item in items]

        next_cursor = None
        if next_azure_ct:
            next_cursor = self._encode_cursor({"type": "azure", "token": next_azure_ct})

        return PageResult(items=items, next_cursor=next_cursor, has_more=bool(next_azure_ct))

    # -----------------------------
    # Lifecycle
    # -----------------------------
    def close(self) -> None:
        with self._cache_lock:
            for tc in self._table_clients_cache.values():
                try:
                    tc.close()
                except Exception:
                    pass
            self._table_clients_cache.clear()
            self._ensured_tables.clear()

        with self._client_lock:
            if self._client is not None:
                try:
                    self._client.close()
                except Exception:
                    pass
            if self._blob_service is not None:
                try:
                    self._blob_service.close()
                except Exception:
                    pass
