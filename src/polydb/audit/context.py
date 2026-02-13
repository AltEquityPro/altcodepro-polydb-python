# src/polydb/audit/AuditContext.py
from contextvars import ContextVar
from typing import Optional, List

class AuditContext:
    """Context variables for audit trail"""
    
    actor_id: ContextVar[Optional[str]] = ContextVar("actor_id", default=None)
    roles: ContextVar[List[str]] = ContextVar("roles", default=[])
    tenant_id: ContextVar[Optional[str]] = ContextVar("tenant_id", default=None)
    trace_id: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)
    request_id: ContextVar[Optional[str]] = ContextVar("request_id", default=None)
    ip_address: ContextVar[Optional[str]] = ContextVar("ip_address", default=None)
    user_agent: ContextVar[Optional[str]] = ContextVar("user_agent", default=None)
    
    @classmethod
    def set(
        cls,
        *,
        actor_id: Optional[str] = None,
        roles: Optional[List[str]] = None,
        tenant_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        request_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ):
        """Set audit context for current request"""
        if actor_id is not None:
            cls.actor_id.set(actor_id)
        if roles is not None:
            cls.roles.set(roles)
        if tenant_id is not None:
            cls.tenant_id.set(tenant_id)
        if trace_id is not None:
            cls.trace_id.set(trace_id)
        if request_id is not None:
            cls.request_id.set(request_id)
        if ip_address is not None:
            cls.ip_address.set(ip_address)
        if user_agent is not None:
            cls.user_agent.set(user_agent)
    
    @classmethod
    def clear(cls):
        """Clear all context variables"""
        cls.actor_id.set(None)
        cls.roles.set([])
        cls.tenant_id.set(None)
        cls.trace_id.set(None)
        cls.request_id.set(None)
        cls.ip_address.set(None)
        cls.user_agent.set(None)