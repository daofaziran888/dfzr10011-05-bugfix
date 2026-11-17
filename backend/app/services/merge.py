"""结果聚合归并业务规则：状态流转、字段校验与筛选口径都收在这里。

聚合任务不维护分片执行结果，只读取任务分片调度落库的执行结果，按重试起点
生成待归并进度。归并下游的「修复分片字段丢失待处理清单」也从同一份快照推导，
保证另一个页面看到的分片、节点、重试次数与起点口径一致。
"""
from __future__ import annotations

import copy
from typing import Any

from app.services.ranges import (
    RangeError,
    dedupe_fields,
    format_range,
    format_ranges,
    normalize_ranges,
    parse_position,
    parse_range_bounds,
    parse_range_text,
    range_texts,
)

from app.services.shard import (
    RETRY_FROM_CONFIRMED,
    RETRY_FROM_RECEIVED,
    ShardService,
    build_retry_plan,
)
from app.store import OwnershipError, RevisionConflict, StoreError, store

MODULE = "merge"
REQUIRED_FIELDS = ["任务编号", "采集计划", "分片总数"]
STATUS_ORDER = ["待归并", "归并中", "已完成", "归并失败"]
ACTION_RULES = {"启动归并": "待归并", "重试归并": "归并中", "归档任务": "归并失败"}
NEGATIVE_ACTIONS = []

# 归并下游的修复分片字段丢失待处理清单：字段名 → 定位说明。
REPAIR_FIELD_ORDER = ["分片范围", "分配节点", "重试次数", "失败范围"]


class MergeService:
    def __init__(self, shard_service: ShardService | None = None) -> None:
        self._shards = shard_service or ShardService()

    # ---- 列表与详情 -----------------------------------------------------
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
            rows = [row for row in rows if keyword in str(row.get("任务编号", ""))]
        if status:
            rows = [row for row in rows if row.get("任务状态") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return [self._public(row) for row in rows[start:start + size]], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        row = store.find(MODULE, entry_id)
        if row is None:
            return None
        entry = self._public(row)
        entry["聚合进度"] = self.build_progress(entry)
        return entry

    def find_by_number(self, task_number: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if str(row.get("任务编号")) == str(task_number):
                return self._public(row)
        return None

    # ---- 登记 -----------------------------------------------------------
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        try:
            total_shards = parse_position(values.get("分片总数"), "分片总数")
        except RangeError as exc:
            return None, [str(exc)]
        if total_shards <= 0:
            return None, ["分片总数必须大于 0"]
        rows = store.rows(MODULE)
        if any(str(row.get("任务编号")) == str(values.get("任务编号")) for row in rows):
            return None, [f"任务编号「{values.get('任务编号')}」已存在，不能重复登记"]

        entry_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        entry = {
            "id": entry_id,
            "任务编号": str(values.get("任务编号")).strip(),
            "采集计划": str(values.get("采集计划")).strip(),
            "分片总数": total_shards,
            "已归并数": 0,
            "归并策略": str(values.get("归并策略") or "").strip(),
            "输出路径": str(values.get("输出路径") or "").strip(),
            "数据总量": 0,
            "任务状态": STATUS_ORDER[0],
            "分片编号清单": dedupe_fields(
                values.get("分片编号清单") or values.get("分片编号")
            ),
            "status": STATUS_ORDER[0],
            "pending": True,
            "abnormal": False,
        }
        try:
            store.append(MODULE, entry, owner=MODULE)
        except (StoreError, OwnershipError) as exc:
            return None, [str(exc)]
        return self.get_entry(entry_id), []

    # ---- 动作 -----------------------------------------------------------
    def run_action(
        self,
        entry_id: int,
        action: str,
        values: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        try:
            row, message = self._dispatch(entry_id, action, values or {})
        except (RangeError, StoreError, OwnershipError, RevisionConflict) as exc:
            return None, str(exc)
        return row, message

    def _dispatch(
        self,
        entry_id: int,
        action: str,
        values: dict[str, Any],
    ) -> tuple[dict[str, Any], str]:
        row = store.find(MODULE, entry_id)
        if row is None:
            raise StoreError(f"聚合任务 {entry_id} 不存在或已归档")
        if action not in ACTION_RULES:
            raise StoreError(f"动作「{action}」不属于结果聚合归并可执行范围")
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            raise StoreError(f"目标状态「{target}」不在允许的状态序列里")

        updated = copy.deepcopy(row)
        if action == "启动归并" and values.get("分片编号清单"):
            updated["分片编号清单"] = dedupe_fields(values.get("分片编号清单"))
        if str(row.get("任务状态")) == "已完成":
            raise StoreError(
                f"聚合任务 {row.get('任务编号')} 已是终态「已完成」，不能再变更状态"
            )
        if action == "重试归并":
            updated["重试次数"] = parse_position(row.get("重试次数") or 0, "重试次数") + 1
            updated["失败范围"] = ""

        updated.update({
            "任务状态": target,
            "status": target,
            "pending": target != STATUS_ORDER[-1],
            "abnormal": action in NEGATIVE_ACTIONS,
        })
        if target == "已完成":
            progress = self.build_progress(updated)
            updated["已归并数"] = progress["已归并分片数"]
            updated["数据总量"] = progress["本批扫描量"]
            updated["失败范围"] = format_ranges(
                self._coverage_gaps(updated, progress)
            )
        elif target == "归并失败":
            updated["失败范围"] = format_ranges(
                self._coverage_gaps(updated, self.build_progress(updated))
            )
        if action == "启动归并":
            # 启动归并会按当前执行结果刷新进度；重试归并保留上次的失败残留，
            # 由后续完成 / 失败动作再统一改写，便于核对失败范围来源。
            updated["已归并数"] = self.build_progress(updated)["已归并分片数"]

        self._save(row, updated)
        return self.get_entry(entry_id) or self._public(store.find(MODULE, entry_id)), f"聚合任务已{action}"

    def _save(self, row: dict[str, Any], updated: dict[str, Any]) -> None:
        store.write_row(
            MODULE,
            int(row["id"]),
            updated,
            owner=MODULE,
            expected_revision=store.revision(MODULE, int(row["id"])),
        )

    # ---- 聚合进度 -------------------------------------------------------
    def build_progress(self, merge_row: dict[str, Any]) -> dict[str, Any]:
        """读取分片执行结果，按重试起点生成待归并进度。"""
        snapshots = self.resolve_shards(merge_row)
        entries: list[dict[str, Any]] = []
        pending: list[dict[str, Any]] = []
        duplicates: list[str] = []
        uncovered: list[str] = []
        total_scan = 0

        for shard in snapshots:
            entry = self._progress_entry(shard)
            entries.append(entry)
            total_scan += entry["扫描量"]
            if entry["重复扫描区间"]:
                duplicates.extend(entry["重复扫描区间清单"])
            if entry["未覆盖区间"]:
                uncovered.extend(entry["未覆盖区间清单"])
            if entry["待归并"]:
                pending.append({
                    "分片编号": entry["分片编号"],
                    "重试起点类型": entry["重试起点类型"],
                    "重试起点": entry["重试起点"],
                    "待归并区间": entry["待归并区间"],
                })

        completed = sum(1 for entry in entries if entry["分片状态"] == "已完成")
        gaps = self._coverage_gaps(merge_row, {"分片明细": entries})
        return {
            "任务编号": merge_row.get("任务编号"),
            "分片总数": len(entries),
            "已归并分片数": completed,
            "待归并分片数": len(entries) - completed,
            "已归并数": completed,
            "本批扫描量": total_scan,
            "重复扫描区间": format_ranges(_merge_text_ranges(duplicates)),
            "重复扫描区间清单": range_texts(_merge_text_ranges(duplicates)),
            "未覆盖区间": format_ranges(_merge_text_ranges(uncovered)),
            "未覆盖区间清单": range_texts(_merge_text_ranges(uncovered)),
            "覆盖缺口": format_ranges(gaps),
            "待归并进度": pending,
            "分片明细": entries,
        }

    def _progress_entry(self, shard: dict[str, Any]) -> dict[str, Any]:
        basis = shard.get("重试起点类型") or ""
        start = shard.get("重试起点")
        duplicate = normalize_ranges(shard.get("重复扫描区间"))
        uncovered = normalize_ranges(shard.get("未覆盖区间"))
        failed = normalize_ranges(shard.get("失败范围"))
        shard_range = parse_range_bounds(shard.get("分片范围"))

        if shard_range is None:
            # 分片范围缺失时进度保留可读说明，不因单条脏数据让聚合详情整体报错；
            # 该缺失同时会出现在归并下游的字段丢失待处理清单里。
            return {
                "分片编号": shard.get("分片编号"),
                "分片范围": "",
                "分配节点": shard.get("分配节点") or "",
                "重试次数": parse_position(shard.get("重试次数") or 0, "重试次数"),
                "重试起点类型": basis,
                "重试起点": start,
                "扫描范围": "",
                "扫描量": 0,
                "失败范围": format_ranges(failed),
                "失败范围清单": range_texts(failed),
                "重复扫描区间": format_ranges(duplicate),
                "重复扫描区间清单": range_texts(duplicate),
                "未覆盖区间": format_ranges(uncovered),
                "未覆盖区间清单": range_texts(uncovered),
                "审批补扫清单": shard.get("补扫清单") or "",
                "待归并区间": "",
                "分片状态": str(shard.get("分片状态") or ""),
                "待归并": False,
                "进度说明": "分片范围缺失或无法解析，无法换算待归并区间",
            }

        if start is None:
            start = shard_range[0]
        scan_start = int(start)
        status = str(shard.get("分片状态") or "")
        scan_range = format_range(scan_start, shard_range[1]) if scan_start <= shard_range[1] else ""
        scan_volume = max(shard_range[1] - scan_start + 1, 0)
        if status == "待分配":
            # 尚未分配节点的分片没有产生扫描，进度里不能把它显示成已覆盖。
            scan_range = ""
            scan_volume = 0

        return {
            "分片编号": shard.get("分片编号"),
            "分片范围": format_range(*shard_range),
            "分配节点": shard.get("分配节点") or "",
            "重试次数": parse_position(shard.get("重试次数") or 0, "重试次数"),
            "重试起点类型": basis,
            "重试起点": scan_start,
            "扫描范围": scan_range,
            "扫描量": scan_volume,
            "失败范围": format_ranges(failed),
            "失败范围清单": range_texts(failed),
            "重复扫描区间": format_ranges(duplicate),
            "重复扫描区间清单": range_texts(duplicate),
            "未覆盖区间": format_ranges(uncovered),
            "未覆盖区间清单": range_texts(uncovered),
            "审批补扫清单": shard.get("补扫清单") or "",
            "待归并区间": scan_range,
            "分片状态": status,
            "待归并": status == "执行中" and bool(scan_range),
        }

    # ---- 修复分片字段丢失清单 -------------------------------------------
    def repair_field_issues(self) -> list[dict[str, Any]]:
        """归并下游的修复分片字段丢失待处理清单。

        以任务分片调度的执行结果为准，逐条检查分片范围、分配节点、重试次数、
        失败范围是否齐全；缺失的字段按分片编号汇总，供另一个页面同步采用。
        """
        issues: list[dict[str, Any]] = []
        for row in store.rows("shard"):
            snapshot = self._shards.execution_snapshot_for_row(row)
            missing: list[str] = []
            for field in REPAIR_FIELD_ORDER:
                value = snapshot.get(field)
                if field == "分片范围":
                    # 分片范围必须能解析成区间，重试起点与待归并进度都依赖它。
                    filled = parse_range_bounds(value) is not None
                elif field == "失败范围":
                    # 执行中的分片允许暂时没有失败范围，但失败分片必须有。
                    filled = bool(value) or snapshot.get("分片状态") != "执行失败"
                elif field == "重试次数":
                    # 重试次数是整数，0 次重试是完整值，不能当成缺失。
                    filled = value is not None and str(value) != ""
                else:
                    filled = bool(str(value or "").strip())
                if not filled:
                    missing.append(field)
            if missing:
                issues.append({
                    "分片编号": snapshot["分片编号"],
                    "采集计划": snapshot["采集计划"],
                    "分片范围": snapshot["分片范围"],
                    "分片状态": snapshot["分片状态"],
                    "分配节点": snapshot["分配节点"],
                    "重试次数": snapshot["重试次数"],
                    "缺失字段": missing,
                })
        return issues

    def repair_summary(self) -> dict[str, Any]:
        issues = self.repair_field_issues()
        field_totals = {field: 0 for field in REPAIR_FIELD_ORDER}
        for issue in issues:
            for field in issue["缺失字段"]:
                field_totals[field] += 1
        return {
            "total": len(issues),
            "field_totals": field_totals,
            "items": issues,
        }

    # ---- 两种重试起点的比较 ---------------------------------------------
    def retry_comparison(self, entry_id: int, shard_number: str) -> dict[str, Any] | None:
        """对比「从上次确认位置重试」和「从最后一次接收位置重试」两种做法。

        两种写法都不写库，只把各自会生成的待归并进度、重复扫描区间和尾部补扫
        清单列出来，便于在页面上核对实际采用哪条路径。
        """
        entry = store.find(MODULE, entry_id)
        row = self._shards._find_row_by_number(shard_number)
        if entry is None or row is None:
            return None
        snapshot = self._shards.execution_snapshot_for_row(row)
        confirmed = snapshot.get("上次确认位置")
        received = snapshot.get("最后一次接收位置")
        if confirmed in (None, "") or received in (None, ""):
            return {
                "分片编号": shard_number,
                "可用": False,
                "说明": "该分片没有同时记录上次确认位置和最后一次接收位置，暂不构成重试起点冲突",
                "两种路径": [],
            }

        plans: list[dict[str, Any]] = []
        for basis in (RETRY_FROM_CONFIRMED, RETRY_FROM_RECEIVED):
            try:
                plan = build_retry_plan(
                    basis=basis,
                    shard_range=parse_range_text(snapshot.get("分片范围")),
                    failed_ranges=normalize_ranges(snapshot.get("失败范围")),
                    confirmed_position=confirmed,
                    received_position=received,
                )
            except RangeError as exc:
                plans.append({"重试起点类型": basis, "可用": False, "说明": str(exc)})
                continue
            plans.append({
                "重试起点类型": basis,
                "可用": True,
                "重试起点": plan["重试起点"],
                "扫描范围": plan["扫描范围"],
                "重复扫描区间": plan["重复扫描区间"] or "无",
                "重复扫描清单": (
                    ["确认点至接收点将被再次扫描：" + plan["重复扫描区间"]]
                    if plan["重复扫描区间"] else []
                ),
                "未覆盖区间": plan["未覆盖区间"] or "无",
                "尾部补扫清单": plan["尾部补扫清单"],
                "补扫清单": plan["补扫清单"],
            })
        return {
            "分片编号": shard_number,
            "可用": True,
            "说明": "两种起点都不修改数据；选定后由任务分片调度按相同口径落库",
            "两种路径": plans,
        }

    # ---- 下游读取 -------------------------------------------------------
    def downstream_feed(self, merge_id: int) -> dict[str, Any] | None:
        """归并下游页面同步采用的清单与执行结果数据。"""
        entry = store.find(MODULE, merge_id)
        if entry is None:
            return None
        public = self._public(entry)
        public["聚合进度"] = self.build_progress(public)
        public["修复分片字段丢失待处理清单"] = self.repair_field_issues()
        public["分片执行结果数据"] = [
            self._shards.execution_snapshot_for_row(row)
            for row in store.rows("shard")
        ]
        return public

    # ---- 内部工具 -------------------------------------------------------
    def resolve_shards(self, merge_row: dict[str, Any]) -> list[dict[str, Any]]:
        numbers = dedupe_fields(merge_row.get("分片编号清单"))
        if numbers:
            snapshots = []
            for number in numbers:
                snapshot = self._shards.execution_snapshot(number)
                if snapshot is not None:
                    snapshots.append(snapshot)
            if snapshots:
                return snapshots
        return [
            self._shards.execution_snapshot_for_row(row)
            for row in store.rows("shard")
        ]

    def _coverage_gaps(
        self,
        merge_row: dict[str, Any],
        progress: dict[str, Any],
    ) -> list[tuple[int, int]]:
        """计算尚未被扫描覆盖的分片区间，作为聚合任务的失败范围。

        已完成的分片整段归并完毕；执行中的分片，上次确认位置之前的内容已经确认过，
        本次从重试起点扫到分片末尾，只有重试起点决定的「未覆盖区间」才是缺口；
        待分配 / 执行失败的分片从未产生有效结果，整段都算缺口。
        """
        gaps: list[tuple[int, int]] = []
        for entry in progress["分片明细"]:
            bounds = parse_range_bounds(entry["分片范围"])
            if bounds is None:
                continue
            status = entry["分片状态"]
            if status == "已完成":
                continue
            if status == "执行中":
                gaps.extend(normalize_ranges(entry["未覆盖区间"]))
                continue
            gaps.append(bounds)
        return normalize_ranges(gaps)

    @staticmethod
    def _public(row: dict[str, Any]) -> dict[str, Any]:
        data = copy.deepcopy(row)
        data["任务状态"] = str(row.get("任务状态") or row.get("status") or "")
        data["status"] = data["任务状态"]
        data["已归并数"] = parse_position(row.get("已归并数") or 0, "已归并数")
        data["分片总数"] = parse_position(row.get("分片总数") or 0, "分片总数")
        return data


def _merge_text_ranges(items: list[str]) -> list[tuple[int, int]]:
    """把多个「100-200」文本合并为唯一区间，避免重复 / 重叠范围进入进度。"""
    ranges: list[tuple[int, int]] = []
    for item in items:
        ranges.extend(normalize_ranges(item))
    return normalize_ranges(ranges)


__all__ = [
    "ACTION_RULES",
    "MODULE",
    "REPAIR_FIELD_ORDER",
    "MergeService",
    "STATUS_ORDER",
]
