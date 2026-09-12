# Async API for PolyDB -- closes CLAUDE.md's own known-gap #5 ("No async
# API. Everything is synchronous... A documented stance would at least
# set expectations").
#
# THE STANCE, stated plainly: PolyDB's entire adapter layer is built on
# genuinely synchronous SDKs -- psycopg2 (not asyncpg), boto3 (not
# aioboto3), the sync azure-* / google-cloud-* clients, pymongo (not
# motor), pika, kafka-python. A "real" native-async rewrite would mean
# swapping every one of those for an async-native equivalent across every
# adapter in `adapters/*.py` -- a from-scratch, parallel adapter layer
# roughly doubling this package's own maintenance surface (every fix,
# every new provider feature, landing twice), for a benefit (not blocking
# the event loop) that a MUCH smaller, purely additive wrapper already
# delivers for the overwhelming majority of real callers: an
# asyncio/FastAPI app that doesn't want a slow Postgres/S3/DynamoDB call
# to stall its event loop.
#
# So this module is that smaller thing, not a rewrite: `AsyncPolyDB`/
# `AsyncDatabaseFactory` wrap the real, unchanged, synchronous
# `PolyDB`/`DatabaseFactory` instance and run every one of its methods in
# a worker thread via `asyncio.to_thread` (or a dedicated
# `ThreadPoolExecutor` when `max_workers` is given), returning an
# awaitable. This is the same "wrap a blocking DB-API driver in a thread
# pool" pattern `encode/databases` and Starlette's own docs recommend for
# exactly this situation -- not a novel trick, the standard one.
#
# Genuinely additive: `PolyDB`/`DatabaseFactory` themselves are completely
# untouched by this module -- every existing synchronous caller keeps
# working byte-for-byte unchanged. A caller who needs real native async
# (avoiding thread-pool overhead entirely, or wanting async cancellation
# semantics psycopg2 can't give) still needs the from-scratch rewrite
# described above -- that is real, separate, much larger future work,
# not something this module pretends to already be.
from __future__ import annotations

import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Generic, Optional, TypeVar

_T = TypeVar("_T")


class _AsyncProxy(Generic[_T]):
    """Wraps any synchronous object `sync` and exposes every one of its
    public callables as an `async def`-equivalent (an async function
    returned from `__getattr__`, run via `asyncio.to_thread`/a dedicated
    executor); every non-callable attribute passes through unchanged,
    read synchronously (cheap, in-memory access -- never worth a thread
    hop). Not specific to PolyDB/DatabaseFactory -- either concrete
    subclass below just fixes the wrapped type for clearer type hints
    and a more specific `__repr__`.

    `max_workers=None` (the default) runs every call via
    `asyncio.to_thread`, i.e. the running loop's own default executor --
    shared with anything else in the same process already using
    `asyncio.to_thread`, the same default a plain asyncio app already
    has. Pass `max_workers=N` for a DEDICATED thread pool sized for this
    instance's own expected DB/cloud-call concurrency, decoupled from
    whatever else the host application runs on the default executor --
    worth doing once real concurrent load is a concern, since the
    default executor is sized `min(32, os.cpu_count() + 4)` by Python
    itself, which can bottleneck a database-heavy workload sharing it
    with other blocking work.
    """

    def __init__(self, sync: _T, *, max_workers: Optional[int] = None) -> None:
        self._sync = sync
        self._executor: Optional[ThreadPoolExecutor] = (
            ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix=f"{type(self).__name__.lower()}"
            )
            if max_workers is not None
            else None
        )
        self._wrapper_cache: dict[str, Callable[..., Any]] = {}

    @property
    def sync(self) -> _T:
        """Escape hatch to the real, wrapped, synchronous instance -- for
        anything this wrapper doesn't cover, or to call something
        deliberately on the current thread (already running inside a
        worker thread, a one-off script with no running event loop,
        etc.)."""
        return self._sync

    async def _run(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        bound = functools.partial(fn, *args, **kwargs)
        if self._executor is not None:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(self._executor, bound)
        return await asyncio.to_thread(bound)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        cached = self._wrapper_cache.get(name)
        if cached is not None:
            return cached
        attr = getattr(self._sync, name)
        if not callable(attr):
            return attr

        async def _async_call(*args: Any, __attr: Callable[..., Any] = attr, **kwargs: Any) -> Any:
            return await self._run(__attr, *args, **kwargs)

        _async_call.__name__ = name
        _async_call.__doc__ = getattr(attr, "__doc__", None)
        self._wrapper_cache[name] = _async_call
        return _async_call

    def __dir__(self) -> list[str]:
        return sorted(set(super().__dir__()) | set(dir(self._sync)))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._sync!r})"

    def close(self) -> None:
        """Shuts down the dedicated executor, if one was created
        (`max_workers` was given). A no-op when running off the shared
        default executor (`max_workers=None`) -- there is nothing this
        instance owns to shut down in that case."""
        if self._executor is not None:
            self._executor.shutdown(wait=True)

    async def __aenter__(self) -> "_AsyncProxy[_T]":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        self.close()


class AsyncDatabaseFactory(
    _AsyncProxy["DatabaseFactory"]
):  # noqa: F821 -- DatabaseFactory imported lazily below for typing only
    """Async wrapper over `DatabaseFactory` -- for callers using the
    lower-level factory directly rather than the `PolyDB` facade. See
    this module's own top comment for the full design rationale."""

    def __init__(
        self,
        *args: Any,
        max_workers: Optional[int] = None,
        sync: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        if sync is None:
            from .databaseFactory import DatabaseFactory

            sync = DatabaseFactory(*args, **kwargs)
        elif args or kwargs:
            raise TypeError(
                "AsyncDatabaseFactory: pass either `sync=<existing DatabaseFactory>` or constructor args, not both"
            )
        super().__init__(sync, max_workers=max_workers)


class AsyncPolyDB(
    _AsyncProxy["PolyDB"]
):  # noqa: F821 -- PolyDB imported lazily below for typing only
    """Async wrapper over the `PolyDB` facade -- the primary
    developer-facing async entrypoint. See this module's own top comment
    for the full design rationale (why this is a thread-pool wrapper, not
    a native-async rewrite, and what a real native-async story would
    require).

    Usage::

        db = AsyncPolyDB(provider=CloudProvider.POSTGRESQL)
        row = await db.create(MyModel, {"id": "1", "name": "hi"})
        rows = await db.read(MyModel, {"name": "hi"})

        # or, for a dedicated thread pool sized for this instance:
        db = AsyncPolyDB(provider=CloudProvider.POSTGRESQL, max_workers=20)

        # or, wrapping an already-constructed sync PolyDB:
        db = AsyncPolyDB(sync=existing_polydb_instance)

        async with db:
            ...  # db.close() runs automatically on exit
    """

    def __init__(
        self,
        *args: Any,
        max_workers: Optional[int] = None,
        sync: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        if sync is None:
            from .PolyDB import PolyDB

            sync = PolyDB(*args, **kwargs)
        elif args or kwargs:
            raise TypeError(
                "AsyncPolyDB: pass either `sync=<existing PolyDB>` or constructor args, not both"
            )
        super().__init__(sync, max_workers=max_workers)
