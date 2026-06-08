"""
polydb.services.security_service
==================================
Field-level encryption for sensitive model fields.

Fix M15: Missing POLYDB_ENCRYPTION_KEY previously caused encrypted fields to
be stored in plaintext with only a warning. Now raises EncryptionConfigError
at startup if any registered model has encrypted=true fields and the key is
not set.
"""

from __future__ import annotations

import base64
import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger("polydb.services.security")


class EncryptionConfigError(RuntimeError):
    """Raised on startup when encryption is required but not configured."""


class SecurityService:
    """Field-level encryption.

    Raises EncryptionConfigError at startup if any registered model has
    encrypted=true fields and POLYDB_ENCRYPTION_KEY is not set.
    """

    ENV_KEY = "POLYDB_ENCRYPTION_KEY"

    def __init__(
        self,
        *,
        encryption_key: Optional[bytes] = None,
    ) -> None:
        self._key: Optional[bytes] = encryption_key or self._read_env_key()

    # ------------------------------------------------------------------
    # Startup validation
    # ------------------------------------------------------------------

    def validate_startup(self, registered_models: List[Dict[str, Any]]) -> None:
        """Call during application startup after models are registered.

        Raises EncryptionConfigError if any model declares encrypted fields
        and the encryption key is absent.  Fail-fast is intentional: running
        without encryption when it is configured is a silent data-security
        violation.
        """
        if self._key:
            return

        encrypted_models: List[str] = []
        for m in registered_models:
            fields = m.get("fields") or {}
            if isinstance(fields, dict):
                for _fname, fdef in fields.items():
                    if isinstance(fdef, dict) and fdef.get("encrypted"):
                        encrypted_models.append(m.get("name", "<unknown>"))
                        break

        if encrypted_models:
            raise EncryptionConfigError(
                f"{self.ENV_KEY} is not set but the following models have "
                f"encrypted=true fields: {', '.join(encrypted_models)}. "
                f"Set {self.ENV_KEY} to a base64url-encoded 32-byte Fernet key "
                f"or remove the encrypted=true flag from those model fields."
            )

    # ------------------------------------------------------------------
    # Encrypt / Decrypt
    # ------------------------------------------------------------------

    def encrypt(self, value: str) -> str:
        """Encrypt *value*. Returns Fernet token (URL-safe base64 string)."""
        if not self._key:
            raise EncryptionConfigError(
                f"Cannot encrypt: {self.ENV_KEY} is not set."
            )
        try:
            from cryptography.fernet import Fernet
            return Fernet(self._key).encrypt(value.encode("utf-8")).decode("ascii")
        except ImportError:
            raise EncryptionConfigError(
                "cryptography package is required for field encryption. "
                "Install it: pip install cryptography"
            )

    def decrypt(self, value: str) -> str:
        """Decrypt a Fernet token produced by :meth:`encrypt`."""
        if not self._key:
            raise EncryptionConfigError(
                f"Cannot decrypt: {self.ENV_KEY} is not set."
            )
        try:
            from cryptography.fernet import Fernet
            return Fernet(self._key).decrypt(value.encode("ascii")).decode("utf-8")
        except ImportError:
            raise EncryptionConfigError(
                "cryptography package is required for field encryption."
            )
        except Exception as exc:
            raise ValueError(f"Decryption failed: {exc}") from exc

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @classmethod
    def _read_env_key(cls) -> Optional[bytes]:
        raw = os.environ.get(cls.ENV_KEY, "").strip()
        if not raw:
            return None
        try:
            # Fernet requires a 32-byte URL-safe base64 key (44 chars with padding)
            key_bytes = raw.encode("ascii")
            decoded = base64.urlsafe_b64decode(key_bytes + b"=" * (-len(raw) % 4))
            if len(decoded) != 32:
                logger.error(
                    "%s decoded to %d bytes; Fernet requires exactly 32 bytes. "
                    "Generate a valid key with: python -c \""
                    "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\"",
                    cls.ENV_KEY, len(decoded),
                )
                return None
            return key_bytes
        except Exception as exc:
            logger.error("Failed to decode %s: %s", cls.ENV_KEY, exc)
            return None
