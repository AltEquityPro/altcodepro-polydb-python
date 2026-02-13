# src/polydb/query.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Union
from enum import Enum


class Operator(Enum):
    EQ = "=="
    NE = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"


@dataclass
class QueryFilter:
    field: str
    operator: Operator
    value: Any


@dataclass
class QueryBuilder:
    """LINQ-style query builder supporting all SQL clauses"""
    
    filters: List[QueryFilter] = field(default_factory=list)
    order_by_fields: List[tuple[str, bool]] = field(default_factory=list)  # (field, desc)
    skip_count: int = 0
    take_count: Optional[int] = None
    select_fields: Optional[List[str]] = None
    group_by_fields: Optional[List[str]] = None
    distinct: bool = False
    count_only: bool = False
    
    def where(self, field: str, operator: Union[Operator, str], value: Any) -> QueryBuilder:
        """Add filter condition"""
        if isinstance(operator, str):
            operator = Operator(operator)
        self.filters.append(QueryFilter(field, operator, value))
        return self
    
    def order_by(self, field: str, descending: bool = False) -> QueryBuilder:
        """Add ordering"""
        self.order_by_fields.append((field, descending))
        return self
    
    def skip(self, count: int) -> QueryBuilder:
        """Skip records"""
        self.skip_count = count
        return self
    
    def take(self, count: int) -> QueryBuilder:
        """Take records"""
        self.take_count = count
        return self
    
    def select(self, *fields: str) -> QueryBuilder:
        """Select specific fields"""
        self.select_fields = list(fields)
        return self
    
    def group_by(self, *fields: str) -> QueryBuilder:
        """Group by fields"""
        self.group_by_fields = list(fields)
        return self
    
    def distinct_on(self) -> QueryBuilder:
        """Return distinct results"""
        self.distinct = True
        return self
    
    def count(self) -> QueryBuilder:
        """Return count only"""
        self.count_only = True
        return self
    
    def to_sql_where(self) -> tuple[str, List[Any]]:
        """Convert to SQL WHERE clause"""
        if not self.filters:
            return "", []
        
        clauses = []
        params = []
        
        for f in self.filters:
            if f.operator == Operator.EQ:
                clauses.append(f"{f.field} = %s")
                params.append(f.value)
            elif f.operator == Operator.NE:
                clauses.append(f"{f.field} != %s")
                params.append(f.value)
            elif f.operator == Operator.GT:
                clauses.append(f"{f.field} > %s")
                params.append(f.value)
            elif f.operator == Operator.GTE:
                clauses.append(f"{f.field} >= %s")
                params.append(f.value)
            elif f.operator == Operator.LT:
                clauses.append(f"{f.field} < %s")
                params.append(f.value)
            elif f.operator == Operator.LTE:
                clauses.append(f"{f.field} <= %s")
                params.append(f.value)
            elif f.operator == Operator.IN:
                placeholders = ','.join(['%s'] * len(f.value))
                clauses.append(f"{f.field} IN ({placeholders})")
                params.extend(f.value)
            elif f.operator == Operator.NOT_IN:
                placeholders = ','.join(['%s'] * len(f.value))
                clauses.append(f"{f.field} NOT IN ({placeholders})")
                params.extend(f.value)
            elif f.operator == Operator.CONTAINS:
                clauses.append(f"{f.field} LIKE %s")
                params.append(f"%{f.value}%")
            elif f.operator == Operator.STARTS_WITH:
                clauses.append(f"{f.field} LIKE %s")
                params.append(f"{f.value}%")
            elif f.operator == Operator.ENDS_WITH:
                clauses.append(f"{f.field} LIKE %s")
                params.append(f"%{f.value}")
        
        return " AND ".join(clauses), params
    
    def to_nosql_filter(self) -> Dict[str, Any]:
        """Convert to NoSQL filter dict"""
        result = {}
        for f in self.filters:
            if f.operator == Operator.EQ:
                result[f.field] = f.value
            elif f.operator == Operator.IN:
                result[f"{f.field}__in"] = f.value
            elif f.operator == Operator.GT:
                result[f"{f.field}__gt"] = f.value
            elif f.operator == Operator.GTE:
                result[f"{f.field}__gte"] = f.value
            elif f.operator == Operator.LT:
                result[f"{f.field}__lt"] = f.value
            elif f.operator == Operator.LTE:
                result[f"{f.field}__lte"] = f.value
            elif f.operator == Operator.CONTAINS:
                result[f"{f.field}__contains"] = f.value
        return result