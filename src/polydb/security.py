# src/polydb/security.py
"""
Security features: encryption, masking, row-level security
"""

from typing import Dict, Any, List, Optional, Callable, Union
from dataclasses import dataclass
import hashlib
import base64
import os
import json
from functools import wraps
import logging

from .json_safe import json_safe
from .errors import EncryptionConfigError

logger = logging.getLogger(__name__)

DEFAULT_KEY_ID = "v1"


@dataclass
class EncryptionConfig:
    """Field-level encryption configuration"""

    encrypted_fields: List[str]
    key: bytes
    algorithm: str = "AES-256-GCM"


class FieldEncryption:
    """Field-level encryption for sensitive data (AES-256-GCM).

    Keys are loaded from the environment - never generated in memory - so a
    process restart can never silently orphan previously-encrypted data:

    - ``POLYDB_ENCRYPTION_KEY``: base64-encoded 32-byte key used to encrypt
      new values. Its key id is ``POLYDB_ENCRYPTION_KEY_ID`` (default "v1").
    - ``POLYDB_ENCRYPTION_KEYS``: optional JSON object ``{key_id: base64key}``
      of retired keys kept around only to decrypt values written before a
      rotation; not used for new encryption.

    Constructing this class raises :class:`EncryptionConfigError` if no
    active key is configured - callers only instantiate it when encryption
    is explicitly enabled, so failing fast here means a misconfigured
    deployment refuses to start rather than persisting plaintext.
    """

    def __init__(self, encryption_key: Optional[bytes] = None):
        if encryption_key is not None:
            self._keys: Dict[str, bytes] = {DEFAULT_KEY_ID: encryption_key}
            self._active_key_id = DEFAULT_KEY_ID
        else:
            self._keys = self._load_keys()
            self._active_key_id = os.getenv("POLYDB_ENCRYPTION_KEY_ID", DEFAULT_KEY_ID).strip()
            if self._active_key_id not in self._keys:
                raise EncryptionConfigError(
                    f"POLYDB_ENCRYPTION_KEY_ID={self._active_key_id!r} has no matching key. "
                    "Set POLYDB_ENCRYPTION_KEY (for the active key id) or include it in "
                    "POLYDB_ENCRYPTION_KEYS."
                )

    @property
    def encryption_key(self) -> bytes:
        """The active key's raw bytes (kept for backward-compatible access)."""
        return self._keys[self._active_key_id]

    @staticmethod
    def _decode_key(raw: str, source: str) -> bytes:
        try:
            key = base64.b64decode(raw, validate=True)
        except Exception as exc:
            raise EncryptionConfigError(f"{source} is not valid base64: {exc}") from exc
        if len(key) != 32:
            raise EncryptionConfigError(
                f"{source} decodes to {len(key)} bytes; AES-256-GCM requires exactly 32 bytes."
            )
        return key

    @classmethod
    def _load_keys(cls) -> Dict[str, bytes]:
        """Load the active key plus any retired keys from the environment."""
        keys: Dict[str, bytes] = {}

        retired_raw = os.getenv("POLYDB_ENCRYPTION_KEYS")
        if retired_raw:
            try:
                retired = json.loads(retired_raw)
            except json.JSONDecodeError as exc:
                raise EncryptionConfigError(
                    f"POLYDB_ENCRYPTION_KEYS is not valid JSON: {exc}"
                ) from exc
            for key_id, raw in retired.items():
                keys[key_id] = cls._decode_key(raw, f"POLYDB_ENCRYPTION_KEYS[{key_id!r}]")

        active_raw = os.getenv("POLYDB_ENCRYPTION_KEY")
        if not active_raw:
            raise EncryptionConfigError(
                "Field encryption is enabled but POLYDB_ENCRYPTION_KEY is not set. "
                "Generate one with: python -c \"import os,base64; "
                "print(base64.b64encode(os.urandom(32)).decode())\" and store it securely "
                "(e.g. Azure Key Vault / AWS Secrets Manager / GCP Secret Manager) - do not "
                "commit it or let it be generated on the fly."
            )
        active_key_id = os.getenv("POLYDB_ENCRYPTION_KEY_ID", DEFAULT_KEY_ID).strip()
        keys[active_key_id] = cls._decode_key(active_raw, "POLYDB_ENCRYPTION_KEY")
        return keys

    def _encrypt_value(self, value: Any) -> str:
        """Encrypt arbitrary value (serialize if non-str)"""
        if value is None:
            return ""
        data = json.dumps(value, default=json_safe) if not isinstance(value, str) else value

        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError as exc:
            raise ImportError(
                "cryptography not installed. Install with: pip install 'altcodepro-polydb-python[security]'"
            ) from exc

        aesgcm = AESGCM(self._keys[self._active_key_id])
        nonce = os.urandom(12)

        ciphertext = aesgcm.encrypt(nonce, data.encode("utf-8"), None)

        # Combine nonce and ciphertext; tag the payload with the key id used so a
        # future key rotation can still decrypt values written under an older key.
        encrypted = base64.b64encode(nonce + ciphertext).decode("utf-8")
        return f"encrypted:{self._active_key_id}:{encrypted}"

    def _decrypt_value(self, encrypted_data: Any) -> Any:
        """Decrypt field data (deserialize if needed)"""
        if not isinstance(encrypted_data, str) or not encrypted_data.startswith("encrypted:"):
            return encrypted_data

        payload = encrypted_data[len("encrypted:"):]
        # Legacy format (pre key-versioning) has no key id segment - decrypt
        # with the active key for backward compatibility with existing data.
        key_id, _, b64_blob = payload.partition(":")
        if not b64_blob:
            key_id, b64_blob = self._active_key_id, key_id

        key = self._keys.get(key_id)
        if key is None:
            raise EncryptionConfigError(
                f"No encryption key available for key id {key_id!r}. Add it to "
                "POLYDB_ENCRYPTION_KEYS to decrypt values written before a key rotation."
            )

        try:
            try:
                from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            except ImportError as exc:
                raise ImportError(
                    "cryptography not installed. Install with: pip install 'altcodepro-polydb-python[security]'"
                ) from exc

            combined = base64.b64decode(b64_blob)
            nonce = combined[:12]
            ciphertext = combined[12:]

            aesgcm = AESGCM(key)
            plaintext_bytes = aesgcm.decrypt(nonce, ciphertext, None)
            plaintext = plaintext_bytes.decode("utf-8")

            # Attempt to parse as JSON if it looks like JSON
            try:
                return json.loads(plaintext)
            except json.JSONDecodeError:
                return plaintext
        except Exception as e:
            # Fail loud. Returning ciphertext as if it were plaintext masks
            # key-rotation errors / corruption and leaks the 'encrypted:' blob
            # into application data.
            logger.error("Field decryption failed: %s", e)
            raise

    def encrypt_fields(self, data: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
        """Encrypt specified fields in data dict"""
        result = dict(data)

        for field in fields:
            if field in result and result[field] is not None:
                result[field] = self._encrypt_value(result[field])

        return result

    def decrypt_fields(self, data: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
        """Decrypt specified fields in data dict"""
        result = dict(data)

        for field in fields:
            if field in result and result[field] is not None:
                result[field] = self._decrypt_value(result[field])

        return result


class DataMasking:
    """Data masking for sensitive information"""

    def __init__(self):
        # Model-specific masking configs: {model: {field: mask_type}}
        self._configs: Dict[str, Dict[str, str]] = {}
        # Global field-based rules (fallback)
        self._global_rules = {
            "email": self._mask_email,
            "phone": self._mask_phone,
            "ssn": self._mask_ssn,
            "credit_card": self._mask_credit_card,
            "password": lambda x: "[REDACTED]",
            "ssn_*": self._mask_ssn,  # Pattern match
            "card_*": self._mask_credit_card,
        }

    def register_model_config(self, model: str, config: Dict[str, str]):
        """Register masking config for a model: {field: mask_type}"""
        self._configs[model] = config

    def _infer_mask_type(self, field: str) -> Optional[Callable]:
        """Infer masker based on field name (global fallback)"""
        for pattern, masker in self._global_rules.items():
            if pattern.endswith("_*") and field.startswith(pattern[:-2]):
                return masker
            if pattern == field:
                return masker
        return None

    @staticmethod
    def _mask_email(email: str) -> str:
        """Mask email address"""
        if "@" not in email:
            return email

        local, domain = email.split("@", 1)
        if len(local) <= 2:
            masked_local = "*" * len(local)
        else:
            masked_local = local[0] + "*" * (len(local) - 2) + local[-1]

        return f"{masked_local}@{domain}"

    @staticmethod
    def _mask_phone(phone: str) -> str:
        """Mask phone number"""
        phone = "".join(filter(str.isdigit, str(phone)))
        if len(phone) <= 4:
            return "*" * len(phone)
        return "*" * (len(phone) - 4) + phone[-4:]

    @staticmethod
    def _mask_ssn(ssn: str) -> str:
        """Mask SSN"""
        ssn = "".join(filter(str.isdigit, str(ssn)))
        if len(ssn) >= 4:
            return "***-**-" + ssn[-4:]
        return "*" * len(ssn)

    @staticmethod
    def _mask_credit_card(cc: str) -> str:
        """Mask credit card"""
        cc = "".join(filter(str.isdigit, str(cc).replace("-", "").replace(" ", "")))
        if len(cc) <= 4:
            return "*" * len(cc)
        return "**** **** **** " + cc[-4:]

    def mask(
        self,
        data: Dict[str, Any],
        model: str,
        actor_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Apply masking to data based on model, actor, and tenant context.

        Uses model-specific config if available, falls back to global rules.
        Context can be used for dynamic masking (e.g., actor-specific).
        """
        result = dict(data)
        config = self._configs.get(model, {})

        for field, value in list(result.items()):
            if value is None:
                continue

            # Model-specific
            mask_type = config.get(field)
            if mask_type:
                masker = self._get_masker(mask_type)
                if masker:
                    result[field] = masker(str(value))
                    continue

            # Global inference
            inferred_masker = self._infer_mask_type(field)
            if inferred_masker:
                result[field] = inferred_masker(str(value))
                continue

        # Context-based dynamic masking (e.g., hide all if not owner)
        if actor_id and tenant_id:
            result = self._apply_context_masking(result, actor_id, tenant_id, model)

        return result

    def _get_masker(self, mask_type: str) -> Optional[Callable]:
        """Get masker function by type"""
        maskers = {
            "email": self._mask_email,
            "phone": self._mask_phone,
            "ssn": self._mask_ssn,
            "credit_card": self._mask_credit_card,
            "redact": lambda x: "[REDACTED]",
        }
        return maskers.get(mask_type)

    def _apply_context_masking(
        self, data: Dict[str, Any], actor_id: str, tenant_id: str, model: str
    ) -> Dict[str, Any]:
        """Apply additional masking based on context (override in subclasses)"""
        # Example: Mask sensitive fields if not tenant owner
        # This is a placeholder; customize per app
        if "owner_id" in data and data["owner_id"] != actor_id:
            for sensitive in ["salary", "health_info"]:
                if sensitive in data:
                    data[sensitive] = "[RESTRICTED]"
        return data


@dataclass
class Policy:
    """RLS Policy entry"""

    name: str
    func: Callable[[Dict[str, Any], Dict[str, Any]], bool]
    apply_to: str  # 'read', 'write', or 'both'


class RowLevelSecurity:
    """Row-level security policies"""

    def __init__(self):
        self.policies: Dict[str, List[Policy]] = {}  # {model: [Policy instances]}
        self._read_filters: Dict[str, Dict[str, Any]] = {}  # {model: default_query_filters}
        self._write_filters: Dict[str, Dict[str, Any]] = {}  # {model: write_constraints}

    def add_policy(
        self,
        model: str,
        name: str,
        policy_func: Callable[[Dict[str, Any], Dict[str, Any]], bool],
        apply_to: str = "read",  # 'read', 'write', or 'both'
    ):
        """
        Add RLS policy

        Args:
            model: Model name
            name: Policy name (for logging/debug)
            policy_func: Function(row_or_data, context) -> bool
            apply_to: 'read' (post-filter), 'write' (pre-check), or 'both'
        """
        if model not in self.policies:
            self.policies[model] = []

        # Ensure unique names
        existing_names = {p.name for p in self.policies[model]}
        if name in existing_names:
            raise ValueError(f"Policy '{name}' already exists for model '{model}'")

        policy = Policy(name=name, func=policy_func, apply_to=apply_to)
        self.policies[model].append(policy)

    def _get_context(
        self,
        actor_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        roles: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Get current security context"""
        return {
            "actor_id": actor_id,
            "tenant_id": tenant_id,
            "roles": roles or [],
            "timestamp": os.getenv("REQUEST_TIMESTAMP", ""),  # Optional: from request
        }

    def check_access(
        self,
        model: str,
        item: Union[Dict[str, Any], Any],  # row for read, data for write
        context: Optional[Dict[str, Any]] = None,
        operation: str = "read",  # 'read' or 'write'
    ) -> bool:
        """Check if access allowed for item under context and operation"""
        if model not in self.policies:
            return True

        ctx = context or self._get_context()
        for policy in self.policies[model]:
            if operation in policy.apply_to or policy.apply_to == "both":
                if not policy.func(item, ctx):
                    # Log denial (in production, use logger)
                    logger.info(f"RLS denied: {policy.name} for {model}:{operation}")
                    return False

        return True

    def enforce_read(
        self, model: str, query: Dict[str, Any], context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Enforce RLS on read query: add pre-filters if possible, else return original (post-filter later)

        For simplicity, adds default filters from _read_filters; complex policies use post-filter.
        """
        result = dict(query or {})

        # Add static filters (e.g., tenant_id)
        if model in self._read_filters:
            for k, v in self._read_filters[model].items():
                if k not in result:
                    result[k] = v

        # Dynamic: if context provided, inject (e.g., tenant_id)
        ctx = context or self._get_context()
        if "tenant_id" in ctx and ctx["tenant_id"] and "tenant_id" not in result:
            result["tenant_id"] = ctx["tenant_id"]

        return result

    def filter_results(
        self, model: str, rows: List[Dict[str, Any]], context: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Post-query filter based on RLS policies"""
        if model not in self.policies:
            return rows

        ctx = context or self._get_context()
        return [row for row in rows if self.check_access(model, row, ctx, operation="read")]

    def enforce_write(
        self, model: str, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Enforce RLS on write: validate data against policies, inject constraints
        """
        # First, check if write is allowed
        ctx = context or self._get_context()
        if not self.check_access(model, data, ctx, operation="write"):
            raise PermissionError(f"RLS write denied for model '{model}'")

        result = dict(data)

        # Inject constraints (e.g., set tenant_id if not present)
        if model in self._write_filters:
            for k, v in self._write_filters[model].items():
                if k not in result:
                    result[k] = v

        # Dynamic injection
        if "tenant_id" in ctx and ctx["tenant_id"] and "tenant_id" not in result:
            result["tenant_id"] = ctx["tenant_id"]

        return result

    def set_default_filters(
        self, model: str, read_filters: Dict[str, Any], write_filters: Dict[str, Any]
    ):
        """Set default query filters/constraints for model"""
        if read_filters:
            self._read_filters[model] = read_filters
        if write_filters:
            self._write_filters[model] = write_filters


# Example RLS policies
def tenant_isolation_policy(item: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Ensure users only access their tenant's data"""
    tenant_id = context.get("tenant_id")
    if not tenant_id:
        return False
    return item.get("tenant_id") == tenant_id


def role_based_policy(row: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Check role-based access"""
    required_role = row.get("required_role")
    user_roles = context.get("roles", [])

    if not required_role:
        return True

    return required_role in user_roles


def ownership_policy(item: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Ensure users only access/modify their owned data"""
    actor_id = context.get("actor_id")
    if not actor_id:
        return False
    return item.get("owner_id") == actor_id or item.get("created_by") == actor_id


def sensitivity_policy(item: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Restrict access to sensitive data based on role"""
    sensitivity_level = item.get("sensitivity_level", "low")
    user_roles = context.get("roles", [])
    if sensitivity_level == "high" and "admin" not in user_roles:
        return False
    if sensitivity_level == "medium" and "admin" not in user_roles and "editor" not in user_roles:
        return False
    return True


def time_based_policy(item: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Restrict access based on data age (e.g., archive old data)"""
    from datetime import datetime, timedelta

    created_at = item.get("created_at")
    if not created_at:
        return True
    try:
        created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        if datetime.now(created_dt.tzinfo) - created_dt > timedelta(days=365):
            return "archivist" in context.get("roles", [])
    except ValueError:
        pass
    return True


# Example usage (in app init):
# rls = RowLevelSecurity()
# rls.add_policy('User', 'tenant_isolation', tenant_isolation_policy, apply_to='both')
# rls.add_policy('User', 'ownership', ownership_policy, apply_to='both')
# rls.add_policy('User', 'role_based', role_based_policy, apply_to='read')
# rls.add_policy('Document', 'sensitivity', sensitivity_policy, apply_to='read')
# rls.add_policy('Log', 'time_based', time_based_policy, apply_to='read')
# rls.set_default_filters('User', read_filters={'status': 'active'}, write_filters={'status': 'active'})
