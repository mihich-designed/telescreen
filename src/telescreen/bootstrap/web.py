"""HTTP process: API and status pages."""

from __future__ import annotations

import uvicorn
from dishka import make_async_container
from fastapi import FastAPI

from telescreen.adapters.http.app import create_app as build_app
from telescreen.bootstrap.ioc import PersistenceProvider, ServicesProvider
from telescreen.bootstrap.settings import Settings


def create_app() -> FastAPI:
    settings = Settings()
    container = make_async_container(
        PersistenceProvider(), ServicesProvider(), context={Settings: settings}
    )
    return build_app(container)


def main() -> None:
    settings = Settings()
    uvicorn.run(
        "telescreen.bootstrap.web:create_app",
        factory=True,
        host="0.0.0.0",
        port=8000,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
