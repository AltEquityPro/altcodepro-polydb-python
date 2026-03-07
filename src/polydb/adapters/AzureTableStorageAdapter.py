# src/polydb/adapters/AzureTableStorageAdapter.py

from __future__ import annotations

import os
import re
import json
import base64
import hashlib
import threading
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


_BYTES_PREFIX = "@@polydb_bytes@@:"
_JSON_PREFIX = "@@polydb_json@@:"
_BASE64_RE = re.compile(r"^[A-Za-z0-9+/]*={0,2}$")

# ensures model isolation across the same table
_MODEL_FIELD = "__polydb_model__"


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

    AZURE_TABLE_MAX_SIZE = 1024 * 1024  # 1MB
    _RESERVED = {"PartitionKey", "RowKey", "Timestamp", "etag", "ETag"}

    def __init__(
        self,
        partition_config: Optional[PartitionConfig] = None,
        connection_string: str = "",
        table_name="",
        container_name="",
    ):
        super().__init__(partition_config)
        self.max_size = self.AZURE_TABLE_MAX_SIZE
        self.connection_string = (
            connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING") or ""
        )
        self.table_name = table_name or os.getenv("AZURE_TABLE_NAME", "defaulttable") or ""
        self.container_name = container_name or os.getenv("AZURE_CONTAINER_NAME", "overflow") or ""

        if not self.connection_string:
            raise ConnectionError("AZURE_STORAGE_CONNECTION_STRING must be set")

        self._client = None
        self._table_client = None
        self._blob_service = None
        self._client_lock = threading.Lock()
        self._initialize_client()

    def _initialize_client(self):
        try:
            from azure.data.tables import TableServiceClient
            from azure.storage.blob import BlobServiceClient

            with self._client_lock:
                if not self._client:
                    self._client = TableServiceClient.from_connection_string(self.connection_string)
                    self._table_client = self._client.get_table_client(self.table_name)

                    try:
                        self._client.create_table_if_not_exists(self.table_name)
                    except Exception:
                        pass

                    self._blob_service = BlobServiceClient.from_connection_string(
                        self.connection_string
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

        if isinstance(v, bytes):
            return _BYTES_PREFIX + base64.b64encode(v).decode("ascii")

        if isinstance(v, (dict, list)):
            return _JSON_PREFIX + json.dumps(v, default=json_safe)

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
                payload = v[len(_JSON_PREFIX) :]
                try:
                    return json.loads(payload)
                except Exception:
                    return v

            if v.startswith(_BYTES_PREFIX):
                payload = v[len(_BYTES_PREFIX) :].strip()
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
        entity: JsonDict = {"PartitionKey": pk, "RowKey": rk}

        # ✅ model isolation
        entity[_MODEL_FIELD] = model.__qualname__

        keymap: Dict[str, str] = {}
        revmap: Dict[str, str] = {}

        for orig_key, orig_val in (data or {}).items():
            if str(orig_key) in self._RESERVED or str(orig_key) in ("PartitionKey", "RowKey"):
                continue

            skey = revmap.get(str(orig_key))
            if not skey:
                skey = self._sanitize_prop_name(orig_key)
                base = skey
                i = 1
                while skey in entity:
                    skey = f"{base}_{i}"
                    i += 1
                revmap[str(orig_key)] = skey
                keymap[skey] = str(orig_key)

            entity[skey] = self._encode_value(orig_val)

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

            # keep internal metadata fields too
            if k.startswith("_") or k in (_MODEL_FIELD,):
                out[k] = v
                continue

            orig_key = keymap.get(k, k)
            out[orig_key] = self._decode_value(v)

        # ✅ guarantee id for tests & ergonomics
        if "id" not in out and rk is not None:
            out["id"] = rk

        return out

    def _entity_size_bytes(self, entity: JsonDict) -> int:
        return len(json.dumps(entity, default=json_safe).encode("utf-8"))

    def _blob_key(self, pk: str, rk: str, checksum: str) -> str:
        return f"{pk}/{rk}/{checksum}.json"

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

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        try:
            if not self._table_client:
                raise NoSQLError("Azure Table client not initialized")

            safe_pk = self._sanitize_pk_rk(pk)
            safe_rk = self._sanitize_pk_rk(rk)

            entity = self._pack_entity(model, safe_pk, safe_rk, data)

            size = self._entity_size_bytes(entity)
            if size > self.AZURE_TABLE_MAX_SIZE:
                full_payload_bytes = json.dumps(entity, default=json_safe).encode("utf-8")
                checksum = hashlib.md5(full_payload_bytes).hexdigest()
                blob_key = self._blob_key(safe_pk, safe_rk, checksum)
                self._blob_upload(blob_key, full_payload_bytes)

                reference_entity: JsonDict = {
                    "PartitionKey": safe_pk,
                    "RowKey": safe_rk,
                    _MODEL_FIELD: model.__qualname__,
                    "_overflow": True,
                    "_blob_key": blob_key,
                    "_size": len(full_payload_bytes),
                    "_checksum": checksum,
                    "__keymap__": entity.get("__keymap__", "{}"),
                }

                # keep a small index of scalars for basic filtering
                kept = 0
                for k, v in entity.items():
                    if k in ("PartitionKey", "RowKey", "__keymap__", _MODEL_FIELD):
                        continue
                    if k.startswith("_"):
                        continue
                    if v is None or isinstance(v, (str, bool, int, float, datetime)):
                        reference_entity[k] = v
                        kept += 1
                    if kept >= 50:
                        break

                self._table_client.upsert_entity(reference_entity)
                return {
                    "PartitionKey": safe_pk,
                    "RowKey": safe_rk,
                    "_overflow": True,
                    "id": safe_rk,
                }

            self._table_client.upsert_entity(entity)
            return {"PartitionKey": safe_pk, "RowKey": safe_rk, "id": safe_rk}

        except Exception as e:
            raise NoSQLError(f"Azure Table put failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:
        try:
            if not self._table_client:
                return None

            safe_pk = self._sanitize_pk_rk(pk)
            safe_rk = self._sanitize_pk_rk(rk)

            entity = self._table_client.get_entity(safe_pk, safe_rk)
            entity_dict = dict(entity)

            # model isolation
            if entity_dict.get(_MODEL_FIELD) != model.__qualname__:
                return None

            if entity_dict.get("_overflow"):
                blob_key = entity_dict.get("_blob_key")
                checksum = entity_dict.get("_checksum")
                if not blob_key:
                    raise NoSQLError("Overflow entity missing _blob_key")

                blob_data = self._blob_download(blob_key)
                actual_checksum = hashlib.md5(blob_data).hexdigest()
                if checksum and actual_checksum != checksum:
                    raise NoSQLError(
                        f"Checksum mismatch: expected {checksum}, got {actual_checksum}"
                    )

                restored = json.loads(blob_data.decode("utf-8"))
                out = self._unpack_entity(restored)
                if "id" not in out:
                    out["id"] = safe_rk
                return out

            out = self._unpack_entity(entity_dict)
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
            if not self._table_client:
                return []

            # always enforce model filter
            eff_filters = dict(filters or {})
            eff_filters[_MODEL_FIELD] = model.__qualname__

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

            entities = self._table_client.query_entities(query_filter=query_filter)  # type: ignore

            results: List[JsonDict] = []
            count = 0
            for ent in entities:
                ent_dict = dict(ent)

                # defensive: enforce model even if query_filter omitted
                if ent_dict.get(_MODEL_FIELD) != model.__qualname__:
                    continue

                if ent_dict.get("_overflow"):
                    blob_key = ent_dict.get("_blob_key")
                    if blob_key:
                        blob_data = self._blob_download(blob_key)
                        restored = json.loads(blob_data.decode("utf-8"))
                        out = self._unpack_entity(restored)
                    else:
                        out = self._unpack_entity(ent_dict)
                else:
                    out = self._unpack_entity(ent_dict)

                # guarantee id
                if "id" not in out and out.get("RowKey") is not None:
                    out["id"] = out["RowKey"]

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
            if not self._table_client:
                return {"deleted": False}

            safe_pk = self._sanitize_pk_rk(pk)
            safe_rk = self._sanitize_pk_rk(rk)

            # read to check model + overflow
            try:
                entity = self._table_client.get_entity(safe_pk, safe_rk)
                entity_dict = dict(entity)

                if entity_dict.get(_MODEL_FIELD) != model.__qualname__:
                    return {"deleted": False}

                if entity_dict.get("_overflow"):
                    blob_key = entity_dict.get("_blob_key")
                    if blob_key:
                        self._blob_delete(blob_key)
            except Exception:
                pass

            self._table_client.delete_entity(safe_pk, safe_rk, etag=etag)
            return {"deleted": True, "PartitionKey": safe_pk, "RowKey": safe_rk, "id": safe_rk}

        except Exception as e:
            raise NoSQLError(f"Azure Table delete failed: {str(e)}")
