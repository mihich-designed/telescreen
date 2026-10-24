from datetime import UTC, datetime

import pytest

from telescreen.adapters.postgres import types
from telescreen.adapters.postgres.pgoutput import DecodeError, PgOutputDecoder
from telescreen.application.wal import (
    UNCHANGED_TOAST,
    BeginMessage,
    ColumnInfo,
    CommitMessage,
    DeleteMessage,
    InsertMessage,
    RelationMessage,
    TruncateMessage,
    UpdateMessage,
)
from telescreen.domain.lsn import Lsn
from telescreen.domain.table import TableName
from tests import pgoutput_builder as pg

OID = 16384
COLUMNS = [
    ("id", types.INT8, True),
    ("total", 1700, False),  # numeric stays a string
    ("paid", types.BOOL, False),
    ("meta", types.JSONB, False),
]


@pytest.fixture
def decoder() -> PgOutputDecoder:
    decoder = PgOutputDecoder()
    decoder.decode(pg.relation(OID, "public", "orders", COLUMNS))
    return decoder


def test_begin_and_commit() -> None:
    decoder = PgOutputDecoder()
    # 2024-01-01T00:00:00Z expressed in microseconds since 2000-01-01.
    ts = 757382400 * 1_000_000
    expected_time = datetime(2024, 1, 1, tzinfo=UTC)

    assert decoder.decode(pg.begin(0x1_0000_0010, ts, 731)) == BeginMessage(
        Lsn(0x1_0000_0010), expected_time, 731
    )
    assert decoder.decode(pg.commit(0x10, 0x20, ts)) == CommitMessage(
        Lsn(0x10), Lsn(0x20), expected_time
    )


def test_relation() -> None:
    message = PgOutputDecoder().decode(pg.relation(OID, "public", "orders", COLUMNS))

    assert message == RelationMessage(
        OID,
        TableName("public", "orders"),
        (
            ColumnInfo("id", types.INT8, True),
            ColumnInfo("total", 1700, False),
            ColumnInfo("paid", types.BOOL, False),
            ColumnInfo("meta", types.JSONB, False),
        ),
    )


def test_insert_converts_values_by_type(decoder: PgOutputDecoder) -> None:
    message = decoder.decode(pg.insert(OID, ["42", "19.84", "t", '{"a": [1]}']))

    assert message == InsertMessage(OID, (42, "19.84", True, {"a": [1]}))


def test_update_with_old_row_and_unchanged_toast(decoder: PgOutputDecoder) -> None:
    payload = pg.update(
        OID, new=["1", "2.00", "f", pg.UNCHANGED], old=["1", "1.00", "f", '{"big": true}']
    )

    message = decoder.decode(payload)

    assert message == UpdateMessage(
        OID,
        new=(1, "2.00", False, UNCHANGED_TOAST),
        old=(1, "1.00", False, {"big": True}),
        old_is_key_only=False,
    )


def test_update_without_old_row(decoder: PgOutputDecoder) -> None:
    message = decoder.decode(pg.update(OID, new=["1", None, "f", "{}"]))

    assert message == UpdateMessage(OID, new=(1, None, False, {}))


def test_delete_with_key_only(decoder: PgOutputDecoder) -> None:
    message = decoder.decode(pg.delete(OID, ["7", None, None, None], key_only=True))

    assert message == DeleteMessage(OID, (7, None, None, None), old_is_key_only=True)


def test_truncate() -> None:
    message = PgOutputDecoder().decode(pg.truncate([1, 2], cascade=True))

    assert message == TruncateMessage((1, 2), cascade=True, restart_identity=False)


@pytest.mark.parametrize("kind", [b"O", b"Y", b"M"])
def test_ignored_messages(kind: bytes) -> None:
    assert PgOutputDecoder().decode(kind + b"\x00" * 8) is None


def test_tuple_for_unknown_relation_is_rejected() -> None:
    with pytest.raises(DecodeError, match="unknown relation"):
        PgOutputDecoder().decode(pg.insert(OID, ["1"]))


def test_column_count_mismatch_is_rejected(decoder: PgOutputDecoder) -> None:
    with pytest.raises(DecodeError, match="tuple has 1 columns"):
        decoder.decode(pg.insert(OID, ["1"]))


def test_truncated_message_is_rejected() -> None:
    with pytest.raises(DecodeError, match="Unexpected end"):
        PgOutputDecoder().decode(pg.begin(1, 2, 3)[:-2])


def test_unknown_message_type_is_rejected() -> None:
    with pytest.raises(DecodeError, match="Unknown message type"):
        PgOutputDecoder().decode(b"Z")
