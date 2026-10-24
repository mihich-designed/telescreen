from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from dishka import AsyncContainer
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from telescreen import __version__
from telescreen.adapters.http import api, pages
from telescreen.application.pipelines import PipelineAlreadyExistsError, PipelineNotFoundError
from telescreen.domain.errors import DomainError, InvalidTransitionError


def _error(status_code: int) -> Callable[[Request, Exception], JSONResponse]:
    def handler(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=status_code)

    return handler


def create_app(container: AsyncContainer) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        await container.close()

    app = FastAPI(
        title="telescreen",
        version=__version__,
        description="Big Brother is watching your WAL.",
        lifespan=lifespan,
    )
    setup_dishka(container, app)
    app.include_router(api.router)
    app.include_router(pages.router)

    # Starlette picks the handler registered for the closest class in the MRO.
    app.add_exception_handler(PipelineNotFoundError, _error(404))
    app.add_exception_handler(PipelineAlreadyExistsError, _error(409))
    app.add_exception_handler(InvalidTransitionError, _error(409))
    app.add_exception_handler(DomainError, _error(422))

    @app.get("/health", tags=["service"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
