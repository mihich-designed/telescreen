"""Server-rendered status pages."""

from pathlib import Path
from typing import Annotated

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from telescreen.application.pipelines import (
    CreatePipeline,
    PipelineAlreadyExistsError,
    PipelineNotFoundError,
    PipelineService,
)
from telescreen.application.queries import PipelineQueries
from telescreen.domain.errors import DomainError

router = APIRouter(include_in_schema=False, route_class=DishkaRoute)

templates = Jinja2Templates(directory=Path(__file__).parent / "templates")


def human_bytes(value: int | None) -> str:
    if value is None:
        return "—"
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024 or unit == "GiB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    raise AssertionError("unreachable")


templates.env.filters["human_bytes"] = human_bytes


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, queries: FromDishka[PipelineQueries]) -> Response:
    views = await queries.list()
    return templates.TemplateResponse(request, "dashboard.html", {"views": views})


@router.get("/partials/pipelines", response_class=HTMLResponse)
async def pipeline_rows(request: Request, queries: FromDishka[PipelineQueries]) -> Response:
    views = await queries.list()
    return templates.TemplateResponse(request, "partials/pipeline_rows.html", {"views": views})


@router.get("/pipelines/new", response_class=HTMLResponse)
async def new_pipeline(request: Request) -> Response:
    return templates.TemplateResponse(request, "new_pipeline.html", {"form": {}, "error": None})


@router.post("/pipelines", response_class=HTMLResponse)
async def create_pipeline(
    request: Request,
    service: FromDishka[PipelineService],
    name: Annotated[str, Form()],
    dsn: Annotated[str, Form()],
    tables: Annotated[str, Form()],
    topic_prefix: Annotated[str, Form()] = "",
) -> Response:
    command = CreatePipeline(
        name=name.strip(),
        dsn=dsn.strip(),
        tables=[t for t in tables.replace(",", "\n").splitlines() if t.strip()],
        topic_prefix=topic_prefix.strip() or None,
    )
    try:
        await service.create(command)
    except (DomainError, PipelineAlreadyExistsError) as exc:
        form = {"name": name, "dsn": dsn, "tables": tables, "topic_prefix": topic_prefix}
        return templates.TemplateResponse(
            request, "new_pipeline.html", {"form": form, "error": str(exc)}, status_code=422
        )
    return RedirectResponse(f"/pipelines/{command.name}", status_code=303)


@router.get("/pipelines/{name}", response_class=HTMLResponse)
async def pipeline_detail(
    request: Request, name: str, queries: FromDishka[PipelineQueries]
) -> Response:
    try:
        view = await queries.get(name)
    except PipelineNotFoundError:
        return RedirectResponse("/", status_code=303)
    return templates.TemplateResponse(request, "pipeline.html", {"view": view})


@router.post("/pipelines/{name}/{action}")
async def pipeline_action(name: str, action: str, service: FromDishka[PipelineService]) -> Response:
    handlers = {
        "pause": service.pause,
        "resume": service.resume,
        "retry": service.retry,
        "delete": service.delete,
    }
    handler = handlers.get(action)
    if handler is None:
        return Response(status_code=404)
    await handler(name)
    return RedirectResponse(f"/pipelines/{name}", status_code=303)
