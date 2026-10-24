from pathlib import Path

import pytest
from pydantic import ValidationError

from telescreen.adapters.config.yaml_pipelines import load_pipelines
from telescreen.adapters.secrets import EnvSecretResolver, SecretNotFoundError
from telescreen.domain.pipeline import SinkSpec, SourceSpec
from telescreen.domain.table import TableName


def test_load_pipelines_applies_defaults(tmp_path: Path) -> None:
    path = tmp_path / "pipelines.yaml"
    path.write_text(
        """
pipelines:
  - name: shop-main
    source:
      dsn: env:SHOP_DSN
    tables: [customers, sales.orders]
  - name: billing
    source:
      dsn: env:BILLING_DSN
      publication: billing_pub
      slot: billing_slot
    tables: [public.invoices]
    sink:
      kind: kafka
      topic_prefix: cdc.billing
""",
        encoding="utf-8",
    )

    shop, billing = load_pipelines(path)

    assert shop.source == SourceSpec("env:SHOP_DSN", "telescreen_shop_main", "telescreen_shop_main")
    assert shop.sink == SinkSpec("kafka", "shop-main")
    assert shop.tables == (TableName("public", "customers"), TableName("sales", "orders"))
    assert billing.source == SourceSpec("env:BILLING_DSN", "billing_pub", "billing_slot")
    assert billing.topic_for(TableName("public", "invoices")) == "cdc.billing.public.invoices"


def test_load_pipelines_rejects_unknown_fields(tmp_path: Path) -> None:
    path = tmp_path / "pipelines.yaml"
    path.write_text(
        "pipelines:\n  - name: x\n    source: {dsn: 'env:X'}\n    tables: [t]\n    oops: 1\n",
        encoding="utf-8",
    )

    with pytest.raises(ValidationError):
        load_pipelines(path)


class TestEnvSecretResolver:
    def test_resolves_env_reference(self) -> None:
        assert EnvSecretResolver({"DSN": "postgresql://x"}).resolve("env:DSN") == "postgresql://x"

    @pytest.mark.parametrize("reference", ["DSN", "vault:DSN", "env:", "env:MISSING"])
    def test_rejects_bad_references(self, reference: str) -> None:
        with pytest.raises(SecretNotFoundError):
            EnvSecretResolver({"DSN": "x"}).resolve(reference)
