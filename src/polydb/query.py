# src/polydb/query.py

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Union
from enum import Enum
from .errors import ValidationError
from .utils import validate_column_name

#: Escape character used for LIKE patterns built from user values.
LIKE_ESCAPE_CHAR = "\\"


def _escape_like(value: Any) -> str:
    """Neutralise LIKE metacharacters in a value before it is wrapped in wildcards.

    ``CONTAINS``/``STARTS_WITH``/``ENDS_WITH`` mean "this literal substring".
    The value is bound as a parameter (so there is no SQL injection here), but
    an unescaped ``%`` or ``_`` in it is still interpreted by the pattern
    matcher: it silently widens the match, and a leading ``%`` turns an
    index-friendly prefix scan into a full scan.
    """
    text = str(value)
    for char in (LIKE_ESCAPE_CHAR, "%", "_"):
        text = text.replace(char, LIKE_ESCAPE_CHAR + char)
    return text


def _reject_mapping_value(f: "QueryFilter") -> None:
    """Refuse dict filter values on the NoSQL path.

    A document store reads a dict on the right-hand side of a field as an
    operator expression, so a filter value that reaches here straight from
    request input - ``where("password", EQ, {"$gt": ""})`` - stops being a
    comparison and becomes "any document with a password", i.e. auth bypass
    plus full-collection extraction. Nothing goes through QueryBuilder with a
    legitimate dict value; lists stay allowed because ``IN`` needs them (and a
    list can never be read as an operator).
    """
    values = f.value if isinstance(f.value, (list, tuple, set)) else [f.value]
    if isinstance(f.value, Mapping) or any(isinstance(v, Mapping) for v in values):
        raise ValidationError(
            f"Invalid filter value for '{f.field}': dict values are not allowed "
            "in NoSQL filters (they would be interpreted as query operators)."
        )


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
            validate_column_name(f.field)

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
                    if not f.value:
                        clauses.append("1=0")  # empty IN → match nothing
                    else:
                        placeholders = ",".join(["%s"] * len(f.value))
                        clauses.append(f"{f.field} IN ({placeholders})")
                        params.extend(f.value)
                else:
                    clauses.append(f"{f.field} = %s")  # scalar IN == equality
                    params.append(f.value)

            elif f.operator == Operator.NOT_IN:
                placeholders = ",".join(["%s"] * len(f.value))
                clauses.append(f"{f.field} NOT IN ({placeholders})")
                params.extend(f.value)

            elif f.operator == Operator.CONTAINS:
                clauses.append(f"{f.field} LIKE %s ESCAPE '{LIKE_ESCAPE_CHAR}'")
                params.append(f"%{_escape_like(f.value)}%")

            elif f.operator == Operator.STARTS_WITH:
                clauses.append(f"{f.field} LIKE %s ESCAPE '{LIKE_ESCAPE_CHAR}'")
                params.append(f"{_escape_like(f.value)}%")

            elif f.operator == Operator.ENDS_WITH:
                clauses.append(f"{f.field} LIKE %s ESCAPE '{LIKE_ESCAPE_CHAR}'")
                params.append(f"%{_escape_like(f.value)}")

        return " AND ".join(clauses), params

    # ------------------------------------------------
    # NOSQL FILTER
    # ------------------------------------------------

    def to_nosql_filter(self) -> Dict[str, Any]:

        result = {}

        for f in self.filters:
            _reject_mapping_value(f)

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
