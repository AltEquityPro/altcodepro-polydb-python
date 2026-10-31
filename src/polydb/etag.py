"""Optimistic concurrency: how a row's version tag (ETag) is exposed and enforced.

* **Exposure is opt-in.** A caller that passes `with_etag=True` to `read` / `read_one` / `create` / `update` / `upsert`
  gets each returned row with a reserved read-only key `_etag`. Nobody else sees any change: without the flag the rows are
  byte-for-byte what they were before this module existed (so API responses, audit logs and encrypted-field handling are
  untouched). Keys starting with `_` are never written, so a row can be round-tripped back into `update()` safely.
* **Enforcement.** `update(..., etag=X)` and `delete(..., etag=X)` succeed only if the stored row still has version X;
  otherwise `ConcurrencyError` (HTTP 412 on Azure). A backend that cannot do this raises `OperationNotSupportedError` --
  an etag is never silently ignored.

The flag travels in a `ContextVar` so the adapters (several layers below the facade) need no extra parameter threaded
through every read path; `expose_etag()` is entered by `DatabaseFactory` around exactly one operation.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

ETAG_KEY = "_etag"
_WITH_ETAG: ContextVar[bool] = ContextVar("polydb_with_etag", default=False)


def etag_requested() -> bool:
    return _WITH_ETAG.get()


@contextmanager
def expose_etag(on: bool = True) -> Iterator[None]:
    token = _WITH_ETAG.set(bool(on))
    try:
        yield
    finally:
        _WITH_ETAG.reset(token)
