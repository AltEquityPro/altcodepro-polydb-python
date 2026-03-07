# src/polydb/query.py

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union
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
    """LINQ-style query builder supporting SQL and NoSQL"""

    filters: List[QueryFilter] = field(default_factory=list)
    order_by_fields: List[tuple[str, bool]] = field(default_factory=list)

    skip_count: int = 0
    take_count: Optional[int] = None

    selected_fields: Optional[List[str]] = None
    group_by_fields: Optional[List[str]] = None

    distinct_flag: bool = False
    count_only: bool = False

    # ------------------------------------------------
    # FILTERS
    # ------------------------------------------------

    def where(
        self, field: str, operator: Union["Operator", str, None], value: Any
    ) -> "QueryBuilder":
        # Allow legacy callers that pass "" meaning equality
        if operator in ("", None):
            op = Operator.EQ
        elif isinstance(operator, Operator):
            op = operator
        else:
            op = Operator(operator)

        self.filters.append(QueryFilter(field=field, operator=op, value=value))
        return self

    # ------------------------------------------------
    # ORDER
    # ------------------------------------------------

    def order_by(self, field: str, descending: bool = False) -> QueryBuilder:
        self.order_by_fields.append((field, descending))
        return self

    # ------------------------------------------------
    # PAGINATION
    # ------------------------------------------------

    def skip(self, count: int) -> QueryBuilder:
        self.skip_count = count
        return self

    def take(self, count: int) -> QueryBuilder:
        self.take_count = count
        return self

    # ------------------------------------------------
    # SELECT
    # ------------------------------------------------

    def select(self, *fields: str) -> QueryBuilder:
        self.selected_fields = list(fields)
        return self

    def select_fields(self, fields: List[str]) -> QueryBuilder:
        self.selected_fields = fields
        return self

    # ------------------------------------------------
    # GROUP
    # ------------------------------------------------

    def group_by(self, *fields: str) -> QueryBuilder:
        self.group_by_fields = list(fields)
        return self

    # ------------------------------------------------
    # DISTINCT
    # ------------------------------------------------

    def distinct(self) -> QueryBuilder:
        self.distinct_flag = True
        return self

    # ------------------------------------------------
    # COUNT
    # ------------------------------------------------

    def count(self) -> QueryBuilder:
        self.count_only = True
        return self

    # ------------------------------------------------
    # SQL WHERE
    # ------------------------------------------------

    def to_sql_where(self) -> tuple[str, List[Any]]:

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

                if isinstance(f.value, (list, tuple)):
                    placeholders = ",".join(["%s"] * len(f.value))
                    clauses.append(f"{f.field} IN ({placeholders})")
                    params.extend(f.value)

                else:
                    clauses.append(f"{f.field} LIKE %s")
                    params.append(f.value)

            elif f.operator == Operator.NOT_IN:
                placeholders = ",".join(["%s"] * len(f.value))
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

    # ------------------------------------------------
    # NOSQL FILTER
    # ------------------------------------------------

    def to_nosql_filter(self) -> Dict[str, Any]:

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
