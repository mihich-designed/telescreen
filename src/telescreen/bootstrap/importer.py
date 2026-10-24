"""Imports pipeline definitions from a YAML file into the metadata database."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from dishka import make_async_container

from telescreen.adapters.config.yaml_pipelines import load_pipelines
from telescreen.application.pipelines import PipelineAlreadyExistsError, PipelineService
from telescreen.bootstrap.ioc import PersistenceProvider, ServicesProvider
from telescreen.bootstrap.settings import Settings


async def run(path: Path) -> None:
    container = make_async_container(
        PersistenceProvider(), ServicesProvider(), context={Settings: Settings()}
    )
    try:
        service = await container.get(PipelineService)
        for command in load_pipelines(path):
            try:
                await service.create(command)
                print(f"created {command.name}")
            except PipelineAlreadyExistsError as exc:
                print(f"skipped {command.name}: {exc}")
    finally:
        await container.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", type=Path, help="path to pipelines.yaml")
    asyncio.run(run(parser.parse_args().file))


if __name__ == "__main__":
    main()
