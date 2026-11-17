"""任务分片调度接口：维护分片任务，覆盖分配节点、重试分片、标记完成等动作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.shard import ShardService

router = APIRouter(prefix="/api/shard", tags=["任务分片调度"])

service = ShardService()

LIST_FIELDS = [
    "分片编号", "采集计划", "分片范围", "分配节点", "开始时间", "结束时间",
    "重试次数", "分片状态", "失败范围",
]
STATUSES = ["待分配", "执行中", "已完成", "执行失败"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按分片编号检索"),
    status: str | None = Query(default=None, description="待分配、执行中、已完成、执行失败"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按分片编号与状态过滤任务分片调度列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出任务分片调度清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "shard", "total": total, "items": items}


@router.get("/results", response_model=dict)
def list_results() -> dict[str, Any]:
    """分片执行结果数据：供聚合归并与归并下游页面同步采用。"""
    items, total = service.list_entries(page=1, size=10000)
    results = [service.execution_result(int(item["id"])) for item in items]
    return {
        "module": "shard",
        "total": total,
        "items": [result for result in results if result is not None],
    }


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条分片任务明细；与列表使用同一份字段口径，不会出现两边对不上。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"分片任务 {entry_id} 不存在或已归档")
    entry["执行结果"] = service.execution_result(entry_id)
    return entry


@router.get("/{entry_id}/result", response_model=dict)
def get_result(entry_id: int) -> dict:
    """读取分片执行结果：完整分配字段、唯一失败范围与重试起点。"""
    result = service.execution_result(entry_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"分片任务 {entry_id} 不存在或已归档")
    return result


@router.get("/{entry_id}/retry-options", response_model=dict)
def get_retry_options(entry_id: int) -> dict:
    """两种重试起点的对比：确认点重试产生重复扫描，接收点重试产生尾部补扫。"""
    payload = service.retry_options(entry_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"分片任务 {entry_id} 不存在或已归档")
    return payload


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条分片任务，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段或字段无效：{'、'.join(missing)}")
    return ActionResult(ok=True, message="分片任务已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条分片任务执行分配节点、重试分片、标记完成、标记失败。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action, payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
