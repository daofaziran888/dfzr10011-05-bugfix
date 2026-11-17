"""结果聚合归并接口：维护聚合任务，覆盖启动归并、重试归并、归档任务等动作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.merge import MergeService

router = APIRouter(prefix="/api/merge", tags=["结果聚合归并"])

service = MergeService()

LIST_FIELDS = [
    "任务编号", "采集计划", "分片总数", "已归并数", "归并策略", "输出路径",
    "数据总量", "任务状态", "失败范围",
]
STATUSES = ["待归并", "归并中", "已完成", "归并失败"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按任务编号检索"),
    status: str | None = Query(default=None, description="待归并、归并中、已完成、归并失败"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按任务编号与状态过滤结果聚合归并列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出结果聚合归并清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "merge", "total": total, "items": items}


@router.get("/repair-issues", response_model=dict)
def repair_issues() -> dict[str, Any]:
    """归并下游的修复分片字段丢失待处理清单，供另一个业务页面同步采用。"""
    return {"module": "merge", **service.repair_summary()}


@router.get("/retry-comparison", response_model=dict)
def retry_comparison(
    entry_id: int = Query(..., description="归属的聚合任务编号"),
    shard_number: str = Query(..., description="分片编号，例如 SHAR-0002"),
) -> dict[str, Any]:
    """对比失败分片的两种重试起点：确认点重试产生重复扫描，接收点重试产生尾部补扫。"""
    payload = service.retry_comparison(entry_id, shard_number)
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail=f"聚合任务 {entry_id} 或分片 {shard_number} 不存在",
        )
    return payload


@router.get("/{entry_id}/downstream", response_model=dict)
def downstream(entry_id: int) -> dict[str, Any]:
    """归并下游同步数据：聚合进度、修复分片字段丢失清单与分片执行结果。"""
    payload = service.downstream_feed(entry_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"聚合任务 {entry_id} 不存在或已归档")
    return payload


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条聚合任务明细；聚合进度按重试起点生成。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"聚合任务 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条聚合任务，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段或字段无效：{'、'.join(missing)}")
    return ActionResult(ok=True, message="聚合任务已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条聚合任务执行启动归并、重试归并、归档任务；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action, payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
