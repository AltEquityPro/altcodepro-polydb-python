# src/polydb/adapters/VercelFileAdapter.py

from typing import List, Optional

from ..base.SharedFilesAdapter import SharedFilesAdapter


class VercelFileAdapter(SharedFilesAdapter):
    """
    Vercel has no shared/persistent filesystem (only ephemeral per-invocation /tmp).
    Shared file storage is not supported — use object storage (Vercel Blob) instead.
    """

    _MSG = (
        "Vercel has no shared file storage (only ephemeral /tmp). "
        "Use object storage (get_object_storage) instead."
    )

    def write(self, path: str, data: bytes, share_name: Optional[str] = None) -> bool:
        raise NotImplementedError(self._MSG)

    def read(self, path: str, share_name: Optional[str] = None) -> bytes | None:
        raise NotImplementedError(self._MSG)

    def delete(self, path: str, share_name: Optional[str] = None) -> bool:
        raise NotImplementedError(self._MSG)

    def list(self, directory: str = "", share_name: Optional[str] = None) -> List[str]:
        raise NotImplementedError(self._MSG)
