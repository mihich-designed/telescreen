import pytest

from telescreen.domain.errors import InvalidPipelineError
from telescreen.domain.lsn import Lsn
from telescreen.domain.pipeline import Pipeline, SinkSpec, SourceSpec
from telescreen.domain.table import TableName


class TestLsn:
    def test_round_trip(self) -> None:
        lsn = Lsn.parse("16/B374D848")
        assert lsn.value == (0x16 << 32) | 0xB374D848
        assert str(lsn) == "16/B374D848"

    def test_ordering(self) -> None:
        assert Lsn.parse("0/FFFFFFFF") < Lsn.parse("1/0")

    @pytest.mark.parametrize("text", ["", "16", "/1", "1/", "zz/1"])
    def test_malformed(self, text: str) -> None:
        with pytest.raises(ValueError, match=r"LSN|invalid literal"):
            Lsn.parse(text)

    def test_range(self) -> None:
        with pytest.raises(ValueError, match="out of range"):
            Lsn(-1)


class TestTableName:
    def test_parse_qualified(self) -> None:
        assert TableName.parse("sales.orders") == TableName("sales", "orders")

    def test_parse_defaults_to_public(self) -> None:
        assert TableName.parse("orders") == TableName("public", "orders")

    @pytest.mark.parametrize("text", ["", ".orders", "sales.", "a.b.c"])
    def test_malformed(self, text: str) -> None:
        with pytest.raises(ValueError, match="Malformed"):
            TableName.parse(text)


def make_pipeline(name: str = "shop", tables: tuple[TableName, ...] | None = None) -> Pipeline:
    return Pipeline(
        name=name,
        source=SourceSpec(dsn="env:DSN", publication="pub", slot="slot"),
        sink=SinkSpec(kind="kafka", topic_prefix="shop"),
        tables=tables if tables is not None else (TableName("public", "orders"),),
    )


class TestPipeline:
    def test_topic_for(self) -> None:
        assert make_pipeline().topic_for(TableName("public", "orders")) == "shop.public.orders"

    @pytest.mark.parametrize("name", ["", "has space", "dots.are.bad"])
    def test_invalid_name(self, name: str) -> None:
        with pytest.raises(InvalidPipelineError):
            make_pipeline(name=name)

    def test_requires_tables(self) -> None:
        with pytest.raises(InvalidPipelineError, match="no tables"):
            make_pipeline(tables=())

    def test_rejects_duplicate_tables(self) -> None:
        table = TableName("public", "orders")
        with pytest.raises(InvalidPipelineError, match="twice"):
            make_pipeline(tables=(table, table))
