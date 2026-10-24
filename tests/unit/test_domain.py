import pytest

from telescreen.domain.errors import InvalidPipelineError, InvalidTransitionError
from telescreen.domain.lsn import Lsn
from telescreen.domain.pipeline import (
    Pipeline,
    PipelineStatus,
    SinkSpec,
    SnapshotMode,
    SourceSpec,
)
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


class TestPipelineLifecycle:
    def test_pause_and_resume(self) -> None:
        pipeline = make_pipeline()
        pipeline.pause()
        assert pipeline.status is PipelineStatus.PAUSED
        pipeline.resume()
        assert pipeline.status is PipelineStatus.ACTIVE

    def test_fail_and_retry(self) -> None:
        pipeline = make_pipeline()
        pipeline.fail("slot is gone")
        assert pipeline.status is PipelineStatus.FAILED
        assert pipeline.last_error == "slot is gone"
        pipeline.retry()
        assert pipeline.status is PipelineStatus.ACTIVE
        assert pipeline.last_error is None

    def test_delete_is_final(self) -> None:
        pipeline = make_pipeline()
        pipeline.delete()
        assert pipeline.status is PipelineStatus.DELETING
        for action in (pipeline.pause, pipeline.resume, pipeline.retry, pipeline.delete):
            with pytest.raises(InvalidTransitionError):
                action()

    def test_resume_requires_paused(self) -> None:
        with pytest.raises(InvalidTransitionError, match="not paused"):
            make_pipeline().resume()

    def test_retry_requires_failed(self) -> None:
        with pytest.raises(InvalidTransitionError, match="not failed"):
            make_pipeline().retry()

    def test_paused_pipeline_cannot_fail(self) -> None:
        pipeline = make_pipeline()
        pipeline.pause()
        with pytest.raises(InvalidTransitionError):
            pipeline.fail("boom")


class TestSnapshot:
    def test_new_pipeline_waits_for_initial_snapshot(self) -> None:
        pipeline = make_pipeline()
        assert pipeline.snapshot is SnapshotMode.INITIAL
        assert pipeline.needs_snapshot

        pipeline.complete_snapshot()

        assert pipeline.snapshot_completed
        assert not pipeline.needs_snapshot

    def test_snapshot_is_completed_once(self) -> None:
        pipeline = make_pipeline()
        pipeline.complete_snapshot()
        with pytest.raises(InvalidTransitionError):
            pipeline.complete_snapshot()

    def test_streaming_only_pipeline_never_snapshots(self) -> None:
        pipeline = make_pipeline()
        pipeline.snapshot = SnapshotMode.NEVER
        assert not pipeline.needs_snapshot
        with pytest.raises(InvalidTransitionError):
            pipeline.complete_snapshot()
