# src/polydb/base/constraints.py
"""
Unique constraints and equality lookup indexes for key/value NoSQL stores.

Table Storage (and every other KV backend here) enforces uniqueness only for the (PartitionKey, RowKey) pair and can only be
queried efficiently by those two keys. A model that declares `unique_fields` / `index_fields` in `__polydb__` gets both from this
mixin, on every NoSQL adapter, with no per-provider code beyond one primitive (`_put_new_raw`, insert-if-absent).

How it works -- two hidden side tables per model (`<table>uq`, `<table>ix`):

* UNIQUE   one "claim" row per (field, value): PartitionKey = u<2 hex of the hash>, RowKey = sha256(field, value). The claim is
           written with an INSERT-IF-ABSENT before the entity, so two writers racing for one value cannot both succeed. A
           duplicate raises UniqueConstraintError. The claim names its owner entity; a claim whose owner no longer exists, or
           whose owner moved to another value (a crash between the claim and the entity write), is taken over.
* INDEX    one row per (field, value, entity): PartitionKey = i<hash of (field, value)>, RowKey = hash of the entity key. An
           equality query on an indexed field reads that one partition, then point-reads each owner and re-checks every filter,
           so a stale index row can never produce a wrong result (it is skipped).

Ordering: claims and index rows are written BEFORE the entity and removed AFTER a delete / value change, so a visible entity
always has its rows. The residue of a crash is a stale row, which is harmless (verified on read, taken over on the next claim).

Not covered: rows written before a field was declared have no claim/index row -- run `rebuild_constraints()` once. Backends
without a native conditional insert fall back to a non-atomic get-then-put (documented on `_put_new_raw`).
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List, Optional, Tuple

from ..errors import NoSQLError
from ..types import JsonDict

_UNSAFE_KEY = re.compile(r"[\\/#\?\x00-\x1f\x7f:+ ]")
_UQ, _IX = "uq", "ix"


class UniqueConstraintError(NoSQLError):
    """A write would create a second row with a value a `unique_fields` field already holds."""

    def __init__(self, field: str, value: Any):
        super().__init__(f"unique constraint violated on {field!r}")
        self.field = field
        self.value = value


def normalize_value(value: Any) -> Optional[str]:
    """The string two values are compared as. None/empty/containers carry no constraint."""
    if value is None or isinstance(value, (dict, list, tuple, set)):
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value)
    return text if text != "" else None


def _digest(*parts: str) -> str:
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


class NoSQLConstraintsMixin:
    """Mixed into NoSQLKVAdapter. Needs `_get_raw`, `_put_raw`, `_delete_raw`, `_query_raw`, `_get_pk_rk`."""

    # -- metadata ---------------------------------------------------------------------------------------------------------

    @staticmethod
    def _constraint_fields(model: type) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
        meta = getattr(model, "__polydb__", None) or {}
        return tuple(meta.get("unique_fields") or ()), tuple(meta.get("index_fields") or ())

    def _side_model(self, model: type, suffix: str) -> type:
        cache = self.__dict__.setdefault("_side_models", {})
        meta = getattr(model, "__polydb__", None) or {}
        base = meta.get("collection") or meta.get("collection_name") or meta.get("table") or getattr(model, "__name__", "model")
        base = re.sub(r"[^A-Za-z0-9]", "", str(base)).lower()[:58] or "model"
        key = (base, suffix)
        if key not in cache:
            name = f"{base}{suffix}"
            cache[key] = type(name, (), {"__polydb__": {"storage": "nosql", "table": name, "pk_field": "bucket", "rk_field": "id"}})
        return cache[key]

    # -- insert-if-absent primitive ----------------------------------------------------------------------------------------

    def _put_new_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> bool:
        """Insert only when (pk, rk) is absent. True = inserted, False = already there. This default is get-then-put and is NOT
        atomic; an adapter with a native conditional create (Azure `create_entity`, Dynamo `attribute_not_exists`, ...) overrides."""
        if self._get_raw(model, pk, rk) is not None:  # type: ignore[attr-defined]
            return False
        self._put_raw(model, pk, rk, data)  # type: ignore[attr-defined]
        return True

    def _query_partition_raw(self, model: type, partition: str, limit: Optional[int]) -> List[JsonDict]:
        """Every row of one partition. Portable default: a property equality on the mirrored `ix_bucket` field."""
        return self._query_raw(model, {"ix_bucket": partition}, limit)  # type: ignore[attr-defined]

    # -- side-row helpers --------------------------------------------------------------------------------------------------

    def _safe_delete(self, model: type, pk: str, rk: str) -> None:
        try:
            self._delete_raw(model, pk, rk, None)  # type: ignore[attr-defined]
        except Exception:  # already gone, or the row was never written
            pass

    def _claim(self, model: type, pk: str, rk: str, field: str, value: str) -> None:
        side = self._side_model(model, _UQ)
        h = _digest(field, value)
        spk = f"u{h[:2]}"
        row = {"bucket": spk, "ix_bucket": spk, "id": h, "field": field, "owner_pk": pk, "owner_rk": rk}
        for _ in range(4):
            if self._put_new_raw(side, spk, h, row):
                return
            cur = self._get_raw(side, spk, h)  # type: ignore[attr-defined]
            if cur is None:  # released between our insert and our read
                continue
            if (cur.get("owner_pk"), cur.get("owner_rk")) == (pk, rk):
                return  # our own claim (an idempotent retry)
            owner = self._get_raw(model, cur.get("owner_pk"), cur.get("owner_rk"))  # type: ignore[attr-defined]
            if owner is not None and normalize_value(owner.get(field)) == value:
                raise UniqueConstraintError(field, value)
            self._safe_delete(side, spk, h)  # orphaned or moved-on claim: take it over
        raise UniqueConstraintError(field, value)

    def _release(self, model: type, pk: str, rk: str, field: str, value: str) -> None:
        side = self._side_model(model, _UQ)
        h = _digest(field, value)
        spk = f"u{h[:2]}"
        cur = self._get_raw(side, spk, h)  # type: ignore[attr-defined]
        if cur is not None and (cur.get("owner_pk"), cur.get("owner_rk")) == (pk, rk):
            self._safe_delete(side, spk, h)

    def _index_key(self, field: str, value: str, pk: str, rk: str) -> Tuple[str, str]:
        return f"i{_digest(field, value)}", _digest(str(pk), str(rk))

    def _index_put(self, model: type, pk: str, rk: str, field: str, value: str) -> None:
        side = self._side_model(model, _IX)
        spk, srk = self._index_key(field, value, pk, rk)
        row = {"bucket": spk, "ix_bucket": spk, "id": srk, "field": field, "owner_pk": pk, "owner_rk": rk}
        self._put_raw(side, spk, srk, row)  # type: ignore[attr-defined]

    def _index_del(self, model: type, pk: str, rk: str, field: str, value: str) -> None:
        spk, srk = self._index_key(field, value, pk, rk)
        self._safe_delete(self._side_model(model, _IX), spk, srk)

    # -- write path --------------------------------------------------------------------------------------------------------

    def _constrained_write(self, model: type, pk: str, rk: str, previous: Optional[JsonDict], new: JsonDict, write):
        """Claim -> index -> entity write -> release/remove the old rows. `new` is the row as it will exist afterwards."""
        uq, ix = self._constraint_fields(model)
        prev = previous or {}
        claimed: List[Tuple[str, str]] = []
        try:
            for f in uq:
                nv, ov = normalize_value(new.get(f)), normalize_value(prev.get(f))
                if nv is not None and nv != ov:
                    self._claim(model, pk, rk, f, nv)
                    claimed.append((f, nv))
            for f in ix:
                nv, ov = normalize_value(new.get(f)), normalize_value(prev.get(f))
                if nv is not None and nv != ov:
                    self._index_put(model, pk, rk, f, nv)
            result = write()
        except Exception:
            for f, v in claimed:
                self._release(model, pk, rk, f, v)
            raise
        for f in uq:
            nv, ov = normalize_value(new.get(f)), normalize_value(prev.get(f))
            if ov is not None and ov != nv:
                self._release(model, pk, rk, f, ov)
        for f in ix:
            nv, ov = normalize_value(new.get(f)), normalize_value(prev.get(f))
            if ov is not None and ov != nv:
                self._index_del(model, pk, rk, f, ov)
        return result

    def _release_all(self, model: type, pk: str, rk: str, row: Optional[JsonDict]) -> None:
        if not row:
            return
        uq, ix = self._constraint_fields(model)
        for f in uq:
            v = normalize_value(row.get(f))
            if v is not None:
                self._release(model, pk, rk, f, v)
        for f in ix:
            v = normalize_value(row.get(f))
            if v is not None:
                self._index_del(model, pk, rk, f, v)

    # -- read path ---------------------------------------------------------------------------------------------------------

    def _lookup_owners(self, model: type, field: str, value: str, limit: Optional[int]) -> List[Tuple[str, str]]:
        uq, _ = self._constraint_fields(model)
        h = _digest(field, value)
        if field in uq:
            cur = self._get_raw(self._side_model(model, _UQ), f"u{h[:2]}", h)  # type: ignore[attr-defined]
            return [(cur["owner_pk"], cur["owner_rk"])] if cur else []
        rows = self._query_partition_raw(self._side_model(model, _IX), f"i{h}", limit)
        return [(r["owner_pk"], r["owner_rk"]) for r in rows if r.get("owner_pk") is not None]

    @staticmethod
    def _filter_matches(row: JsonDict, query: Dict[str, Any], pk: str, rk: str) -> bool:
        for k, want in query.items():
            if k in ("partition_key", "PartitionKey"):
                if _UNSAFE_KEY.sub("_", str(pk)) != str(want):
                    return False
            elif k in ("row_key", "RowKey"):
                if _UNSAFE_KEY.sub("_", str(rk)) != str(want):
                    return False
            elif want is None:
                if row.get(k) is not None:
                    return False
            elif normalize_value(row.get(k)) != normalize_value(want):
                return False
        return True

    def _indexed_query(self, model: type, query: Dict[str, Any], limit: Optional[int]) -> Optional[List[JsonDict]]:
        """Rows for a query that has an equality on a unique/indexed field, or None when the index cannot serve it."""
        uq, ix = self._constraint_fields(model)
        if not query or not (uq or ix):
            return None
        if any(isinstance(v, (dict, list, tuple, set)) for v in query.values()):
            return None
        for f in (*uq, *ix):
            nv = normalize_value(query.get(f)) if f in query else None
            if nv is None:
                continue
            out: List[JsonDict] = []
            for opk, ork in self._lookup_owners(model, f, nv, None):
                row = self._get_raw(model, opk, ork)  # type: ignore[attr-defined]
                if row is not None and self._filter_matches(row, query, opk, ork):
                    out.append(row)
                    if limit and len(out) >= limit:
                        break
            return out
        return None

    def rebuild_constraints(self, model: type) -> Dict[str, int]:
        """One-off backfill for rows written before a field was declared unique/indexed. Claims every existing value (the
        first holder keeps it; a duplicate is reported, not overwritten) and writes every index row."""
        uq, ix = self._constraint_fields(model)
        done = {"rows": 0, "claims": 0, "indexed": 0, "duplicates": 0}
        for row in self._query_raw(model, {}, None):  # type: ignore[attr-defined]
            pk, rk = self._get_pk_rk(model, row)  # type: ignore[attr-defined]
            done["rows"] += 1
            for f in uq:
                v = normalize_value(row.get(f))
                if v is None:
                    continue
                try:
                    self._claim(model, pk, rk, f, v)
                    done["claims"] += 1
                except UniqueConstraintError:
                    done["duplicates"] += 1
            for f in ix:
                v = normalize_value(row.get(f))
                if v is not None:
                    self._index_put(model, pk, rk, f, v)
                    done["indexed"] += 1
        return done
