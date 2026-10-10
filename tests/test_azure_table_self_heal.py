"""A missing Azure Table is created again and the call retried; a failed creation is not remembered as success."""
from unittest.mock import MagicMock

from polydb.adapters.AzureTableStorageAdapter import _SelfHealingTableClient, _is_table_missing


class _Err(Exception):
    pass


def _adapter():
    a = MagicMock()
    a._ensured_tables = {"t"}
    return a


def test_detects_the_missing_table_error():
    assert _is_table_missing(_Err("ErrorCode:TableNotFound"))
    assert _is_table_missing(_Err("The table specified does not exist."))
    assert not _is_table_missing(_Err("timeout"))


def test_a_call_on_a_missing_table_creates_it_and_retries_once():
    client = MagicMock()
    client.get_entity.side_effect = [_Err("TableNotFound"), {"ok": 1}]
    a = _adapter()
    got = _SelfHealingTableClient(a, "t", client).get_entity(partition_key="p", row_key="r")
    assert got == {"ok": 1} and a._client.create_table_if_not_exists.called and "t" in a._ensured_tables


def test_other_errors_are_not_swallowed():
    client = MagicMock()
    client.get_entity.side_effect = _Err("boom")
    try:
        _SelfHealingTableClient(_adapter(), "t", client).get_entity()
        raise AssertionError("should raise")
    except _Err:
        pass


def test_a_lazy_query_on_a_missing_table_heals_before_the_first_row():
    client = MagicMock()
    def gone():
        raise _Err("TableNotFound")
        yield
    client.query_entities.side_effect = [gone(), iter([{"a": 1}, {"a": 2}])]
    a = _adapter()
    rows = list(_SelfHealingTableClient(a, "t", client).query_entities("x"))
    assert rows == [{"a": 1}, {"a": 2}] and a._client.create_table_if_not_exists.called


def test_a_failure_after_rows_were_produced_is_raised_not_replayed():
    client = MagicMock()
    def partial():
        yield {"a": 1}
        raise _Err("TableNotFound")
    client.query_entities.return_value = partial()
    it = _SelfHealingTableClient(_adapter(), "t", client).query_entities("x")
    assert next(it) == {"a": 1}
    try:
        next(it)
        raise AssertionError("should raise")
    except _Err:
        pass
