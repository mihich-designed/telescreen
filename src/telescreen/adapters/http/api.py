"""JSON API for managing pipelines."""

from enum import StrEnum

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, status

from telescreen.adapters.http.schemas import PipelineIn, PipelineOut
from telescreen.application.pipelines import PipelineService
from telescreen.application.queries import PipelineQueries

router = APIRouter(prefix="/api", tags=["pipelines"], route_class=DishkaRoute)


class Action(StrEnum):
    PAUSE = "pause"
    RESUME = "resume"
    RETRY = "retry"


@router.get("/pipelines")
async def list_pipelines(queries: FromDishka[PipelineQueries]) -> list[PipelineOut]:
    return [PipelineOut.from_view(view) for view in await queries.list()]


@router.post("/pipelines", status_code=status.HTTP_201_CREATED)
async def create_pipeline(
    body: PipelineIn,
    service: FromDishka[PipelineService],
    queries: FromDishka[PipelineQueries],
) -> PipelineOut:
    pipeline = await service.create(body.to_command())
    return PipelineOut.from_view(await queries.get(pipeline.name))


@router.get("/pipelines/{name}")
async def get_pipeline(name: str, queries: FromDishka[PipelineQueries]) -> PipelineOut:
    return PipelineOut.from_view(await queries.get(name))


@router.post("/pipelines/{name}/{action}")
async def change_pipeline(
    name: str,
    action: Action,
    service: FromDishka[PipelineService],
    queries: FromDishka[PipelineQueries],
) -> PipelineOut:
    match action:
        case Action.PAUSE:
            await service.pause(name)
        case Action.RESUME:
            await service.resume(name)
        case Action.RETRY:
            await service.retry(name)
    return PipelineOut.from_view(await queries.get(name))


@router.delete("/pipelines/{name}", status_code=status.HTTP_202_ACCEPTED)
async def delete_pipeline(
    name: str,
    service: FromDishka[PipelineService],
    queries: FromDishka[PipelineQueries],
) -> PipelineOut:
    """Mark the pipeline for deletion. A worker drops its slot and publication."""
    await service.delete(name)
    return PipelineOut.from_view(await queries.get(name))
