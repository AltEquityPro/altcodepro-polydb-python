# src/polydb/advanced_query.py
"""
Advanced query capabilities: JOIN, subqueries, aggregates
"""
from typing import List, Optional, Any, Dict
from dataclasses import dataclass, field
from enum import Enum


class JoinType(Enum):
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"


class AggregateFunction(Enum):
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"


@dataclass
class Join:
    table: str
    join_type: JoinType
    on_left: str
    on_right: str
    alias: Optional[str] = None


@dataclass
class Aggregate:
    function: AggregateFunction
    field: str
    alias: str


@dataclass
class AdvancedQueryBuilder:
    """Extended QueryBuilder with JOINs and aggregates"""

    table: str
    joins: List[Join] = field(default_factory=list)
    aggregates: List[Aggregate] = field(default_factory=list)
    group_by_fields: List[str] = field(default_factory=list)
    having_conditions: List[str] = field(default_factory=list)
    subqueries: Dict[str, "AdvancedQueryBuilder"] = field(default_factory=dict)

    def join(
        self,
        table: str,
        on_left: str,
        on_right: str,
        join_type: JoinType = JoinType.INNER,
        alias: Optional[str] = None,
    ) -> "AdvancedQueryBuilder":
        """Add JOIN clause"""
        self.joins.append(
            Join(table=table, join_type=join_type, on_left=on_left, on_right=on_right, alias=alias)
        )
        return self

    def aggregate(
        self, function: AggregateFunction, field: str, alias: str
    ) -> "AdvancedQueryBuilder":
        """Add aggregate function"""
        self.aggregates.append(Aggregate(function=function, field=field, alias=alias))
        return self

    def group_by(self, *fields: str) -> "AdvancedQueryBuilder":
        """Add GROUP BY"""
        self.group_by_fields.extend(fields)
        return self

    def having(self, condition: str) -> "AdvancedQueryBuilder":
        """Add HAVING clause"""
        self.having_conditions.append(condition)
        return self

    def to_sql(self) -> tuple[str, List[Any]]:
        """Generate complete SQL with JOINs"""
        params = []

        # SELECT clause
        if self.aggregates:
            select_parts = [
                f"{agg.function.value}({agg.field}) AS {agg.alias}" for agg in self.aggregates
            ]
            if self.group_by_fields:
                select_parts = list(self.group_by_fields) + select_parts
            sql = f"SELECT {', '.join(select_parts)}"
        else:
            sql = f"SELECT *"

        # FROM clause
        sql += f" FROM {self.table}"

        # JOIN clauses
        for join in self.joins:
            table = f"{join.table} AS {join.alias}" if join.alias else join.table
            sql += f" {join.join_type.value} {table}"
            sql += f" ON {join.on_left} = {join.on_right}"

        # GROUP BY clause
        if self.group_by_fields:
            sql += f" GROUP BY {', '.join(self.group_by_fields)}"

        # HAVING clause
        if self.having_conditions:
            sql += f" HAVING {' AND '.join(self.having_conditions)}"

        return sql, params


# Example usage helper
class QueryHelper:
    """Helper methods for common query patterns"""

    @staticmethod
    def count_by_field(table: str, field: str, group_field: str) -> AdvancedQueryBuilder:
        """Count occurrences grouped by field"""
        return (
            AdvancedQueryBuilder(table=table)
            .aggregate(AggregateFunction.COUNT, field, "count")
            .group_by(group_field)
        )

    @staticmethod
    def sum_by_category(table: str, sum_field: str, category_field: str) -> AdvancedQueryBuilder:
        """Sum values grouped by category"""
        return (
            AdvancedQueryBuilder(table=table)
            .aggregate(AggregateFunction.SUM, sum_field, "total")
            .group_by(category_field)
        )

    @staticmethod
    def join_with_filter(
        left_table: str, right_table: str, join_field: str
    ) -> AdvancedQueryBuilder:
        """Simple INNER JOIN"""
        return AdvancedQueryBuilder(table=left_table).join(
            right_table, f"{left_table}.{join_field}", f"{right_table}.{join_field}"
        )
