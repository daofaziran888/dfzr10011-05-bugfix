"""采集节点集群业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.store import StoreError, store

MODULE = "cluster"
REQUIRED_FIELDS = ["节点编号", "节点地址", "节点角色"]
STATUS_ORDER = ["待加入", "运行中", "已离线", "已移除"]
ACTION_RULES = {"加入集群": "待加入", "下线节点": "运行中", "移除节点": "已离线"}
NEGATIVE_ACTIONS = ["下线节点"]


class ClusterService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("节点编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        try:
            store.append(MODULE, entry, owner=MODULE)
        except StoreError as exc:
            return None, [str(exc)]
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"集群节点 {entry_id} 不存在或已归档"
        entry = dict(entry)
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集节点集群可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        try:
            store.write_row(MODULE, entry_id, entry, owner=MODULE)
        except StoreError as exc:
            return None, str(exc)
        return entry, f"集群节点已{action}"
