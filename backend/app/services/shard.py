"""任务分片调度业务规则：状态流转、字段校验与筛选口径都收在这里。

分片执行结果是任务分片调度自己的数据，聚合归并只读不写：

* 分配节点、重试次数、失败范围跟随分片记录一次性整行保存，任何校验失败都不会留下半个结果；
* 失败分片保留两种可重试起点（上次确认位置 / 最后一次接收位置），由调用方显式选择；
* 分片状态只由本服务维护，并带版本号做并发保护，旧失败结果不能覆盖新的终态。

聚合进度由 :mod:`app.services.merge` 读取本服务的执行结果快照，本模块不反向写聚合任务。
"""
from __future__ import annotations

import copy
from typing import Any

from app.services.ranges import (
    RangeError,
    format_range,
    format_ranges,
    normalize_ranges,
    parse_position,
    parse_range_text,
    range_texts,
    subtract_ranges,
)
from app.store import OwnershipError, RevisionConflict, StoreError, store

MODULE = "shard"
REQUIRED_FIELDS = ["分片编号", "采集计划", "分片范围"]
STATUS_ORDER = ["待分配", "执行中", "已完成", "执行失败"]
# 动作 → 目标状态；「分配节点」只写执行结果，不改状态，因此目标记为空。
ACTION_RULES = {
    "分配节点": "",
    "重试分片": "执行中",
    "标记完成": "已完成",
    "标记失败": "执行失败",
}
NEGATIVE_ACTIONS = []

# 分配节点、重试次数与失败范围属于同一次执行结果，统一打包保存。
RESULT_FIELDS = ["分片范围", "分配节点", "重试次数", "失败范围"]

# 终态：一旦写入，后续状态更新只能被拒绝，不能被旧结果覆盖。
TERMINAL_STATUS = "已完成"

# 重试起点：确认点重试会回退并产生重复扫描区间；接收点重试只补扫尾部。
RETRY_FROM_CONFIRMED = "上次确认位置"
RETRY_FROM_RECEIVED = "最后一次接收位置"
RETRY_BASES = [RETRY_FROM_CONFIRMED, RETRY_FROM_RECEIVED]


class ShardService:
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
            rows = [row for row in rows if keyword in str(row.get("分片编号", ""))]
        if status:
            rows = [row for row in rows if row.get("分片状态") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return [self._public(row) for row in rows[start:start + size]], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        row = store.find(MODULE, entry_id)
        return self._public(row) if row is not None else None

    def find_by_number(self, shard_number: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if str(row.get("分片编号")) == str(shard_number):
                return self._public(row)
        return None

    def execution_result(self, entry_id: int) -> dict[str, Any] | None:
        """聚合归并读取的执行结果视图：完整分配字段 + 唯一失败范围。"""
        row = store.find(MODULE, entry_id)
        if row is None:
            return None
        return self.execution_snapshot_for_row(row)

    def execution_snapshot(self, shard_number: str) -> dict[str, Any] | None:
        row = self._find_row_by_number(shard_number)
        if row is None:
            return None
        return self.execution_snapshot_for_row(row)

    # ---- 登记 -----------------------------------------------------------
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        confirmed_value = values.get("上次确认位置")
        received_value = values.get("最后一次接收位置")
        try:
            shard_range = parse_range_text(values.get("分片范围"))
            failed = normalize_ranges(values.get("失败范围"))
            attempt = parse_position(values.get("重试次数") or 0, "重试次数")
            entry_id = max(
                (int(row.get("id", 0)) for row in store.rows(MODULE)), default=0
            ) + 1
            confirmed = (
                parse_position(confirmed_value, "上次确认位置")
                if confirmed_value not in (None, "")
                else ""
            )
            received = (
                parse_position(received_value, "最后一次接收位置")
                if received_value not in (None, "")
                else ""
            )
            if confirmed != "" and received != "":
                confirmed, received = _guard_offsets(confirmed, received, shard_range)
        except RangeError as exc:
            return None, [str(exc)]
        rows = store.rows(MODULE)
        if any(str(row.get("分片编号")) == str(values.get("分片编号")) for row in rows):
            return None, [f"分片编号「{values.get('分片编号')}」已存在，不能重复登记"]

        entry = {
            "id": entry_id,
            "分片编号": str(values.get("分片编号")).strip(),
            "采集计划": str(values.get("采集计划")).strip(),
            "分片范围": format_range(*shard_range),
            "分配节点": str(values.get("分配节点") or "").strip(),
            "开始时间": values.get("开始时间"),
            "结束时间": values.get("结束时间"),
            "重试次数": attempt,
            "分片状态": STATUS_ORDER[0],
            "失败范围": format_ranges(failed),
            "上次确认位置": confirmed,
            "最后一次接收位置": received,
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
            raise StoreError(f"分片任务 {entry_id} 不存在或已归档")
        if action not in ACTION_RULES:
            raise StoreError(f"动作「{action}」不属于任务分片调度可执行范围")
        target = ACTION_RULES[action]
        if target and target not in STATUS_ORDER:
            raise StoreError(f"目标状态「{target}」不在允许的状态序列里")
        expected_revision = self._parse_revision(values.get("revision"))
        self._reject_cross_status(values, row)

        if action == "分配节点":
            return self._assign(row, values, expected_revision)
        if action == "重试分片":
            return self._retry(row, values, expected_revision)
        return self._finish(row, action, target, values, expected_revision)

    def _reject_cross_status(self, values: dict[str, Any], row: dict[str, Any]) -> None:
        """其他模块提交的分片状态与当前记录不一致时直接拒绝。"""
        incoming_status = str(values.get("分片状态") or "").strip()
        if incoming_status and incoming_status != row.get("分片状态"):
            raise StoreError(
                f"分片状态由任务分片调度维护为「{row.get('分片状态')}」，"
                f"提交的「{incoming_status}」被拒绝；其他模块不能覆盖分片状态"
            )

    # ---- 分配节点 -------------------------------------------------------
    def _assign(
        self,
        row: dict[str, Any],
        values: dict[str, Any],
        expected_revision: int | None,
    ) -> tuple[dict[str, Any], str]:
        """分配节点是执行结果的一部分：节点、重试次数、失败范围整包写入。"""
        if self._is_terminal(row):
            raise StoreError(
                f"分片任务 {row.get('分片编号')} 已是终态「{row.get('分片状态')}」，"
                "不能再重新分配节点"
            )
        node = str(values.get("分配节点") or row.get("分配节点") or "").strip()
        if not node:
            raise RangeError("分配节点不能为空，请指定采集节点后再保存")
        attempt = parse_position(
            values.get("重试次数", row.get("重试次数")), "重试次数"
        )
        failed = normalize_ranges(values.get("失败范围", row.get("失败范围")))
        confirmed_value = values.get("上次确认位置", row.get("上次确认位置"))
        received_value = values.get("最后一次接收位置", row.get("最后一次接收位置"))
        if confirmed_value not in (None, "") and received_value not in (None, ""):
            _guard_offsets(
                confirmed_value,
                received_value,
                parse_range_text(row.get("分片范围")),
            )

        updated = copy.deepcopy(row)
        updated.update({
            "分配节点": node,
            "重试次数": attempt,
            "失败范围": format_ranges(failed),
        })
        if confirmed_value not in (None, ""):
            updated["上次确认位置"] = parse_position(confirmed_value, "上次确认位置")
        if received_value not in (None, ""):
            updated["最后一次接收位置"] = parse_position(
                received_value, "最后一次接收位置"
            )
        if str(row.get("分片状态")) == "待分配":
            # 待分配 -> 执行中属于本模块自己的状态流转；已在执行 / 已失败的分片
            # 保持原状态，节点分配不再把状态或失败残留覆盖掉。
            updated.update({
                "分片状态": "执行中",
                "status": "执行中",
                "pending": True,
            })
        # 本次执行结果只有在校验全部通过后才整行保存，不会先写节点再写失败。
        return self._save(row, updated, expected_revision), "分片任务已分配节点"

    # ---- 完成 / 失败 ----------------------------------------------------
    def _finish(
        self,
        row: dict[str, Any],
        action: str,
        target: str,
        values: dict[str, Any],
        expected_revision: int | None,
    ) -> tuple[dict[str, Any], str]:
        if self._is_terminal(row):
            if str(row.get("分片状态")) == target:
                # 同一终态重复提交视为幂等返回，不再写第二次终态。
                return self._public(row), f"分片任务已是终态「{target}」，本次未重复写入"
            raise StoreError(
                f"分片任务 {row.get('分片编号')} 已是终态「{row.get('分片状态')}」，"
                "同一分片只能写入一个终态，旧结果不能覆盖新结果"
            )

        updated = copy.deepcopy(row)
        updated.update({
            "分片状态": target,
            "status": target,
            "pending": target != STATUS_ORDER[-1],
            "abnormal": action in NEGATIVE_ACTIONS,
        })
        if target == "执行失败":
            failed = normalize_ranges(values.get("失败范围", row.get("失败范围")))
            if not failed:
                raise RangeError("标记失败必须给出失败范围，聚合进度需要唯一失败范围")
            _guard_failed_ranges(failed, parse_range_text(row.get("分片范围")))
            updated["失败范围"] = format_ranges(failed)
            if values.get("开始时间"):
                updated["开始时间"] = values.get("开始时间")
            if values.get("结束时间"):
                updated["结束时间"] = values.get("结束时间")
        if target == TERMINAL_STATUS:
            # 完成即清零失败残留与待补扫区间；实际采用的起点与扫描范围保留，
            # 聚合清单仍能显示这一批从哪里开始扫描。
            updated["失败范围"] = ""
            updated["未覆盖区间"] = ""
            updated["补扫清单"] = ""
        return self._save(row, updated, expected_revision), f"分片任务已{action}"

    # ---- 重试 -----------------------------------------------------------
    def _retry(
        self,
        row: dict[str, Any],
        values: dict[str, Any],
        expected_revision: int | None,
    ) -> tuple[dict[str, Any], str]:
        """重试必须显式选择起点；两种起点产生不同的重复扫描 / 尾部补扫结果。"""
        basis = str(values.get("重试起点") or values.get("起点") or "").strip()
        if basis not in RETRY_BASES:
            raise RangeError(
                "重试分片必须指定重试起点："
                f"「{RETRY_FROM_CONFIRMED}」或「{RETRY_FROM_RECEIVED}」"
            )
        node = str(values.get("分配节点") or row.get("分配节点") or "").strip()
        if not node:
            raise RangeError("重试分片前必须先指定分配节点")
        attempt = parse_position(row.get("重试次数") or 0, "重试次数") + 1
        if not str(row.get("分片范围") or "").strip():
            raise RangeError("分片范围缺失，无法按重试起点生成待归并进度")
        shard_range = parse_range_text(row.get("分片范围"))
        confirmed_value = values.get("上次确认位置", row.get("上次确认位置"))
        received_value = values.get("最后一次接收位置", row.get("最后一次接收位置"))
        if confirmed_value in (None, "") or received_value in (None, ""):
            raise RangeError(
                "重试分片需要同时登记上次确认位置和最后一次接收位置，"
                "否则无法生成重复扫描区间与尾部补扫清单"
            )

        plan = build_retry_plan(
            basis=basis,
            shard_range=shard_range,
            failed_ranges=normalize_ranges(row.get("失败范围")),
            confirmed_position=confirmed_value,
            received_position=received_value,
        )

        updated = copy.deepcopy(row)
        updated.update({
            "分配节点": node,
            "重试次数": attempt,
            "重试起点类型": basis,
            "重试起点": plan["重试起点"],
            "上次确认位置": plan["上次确认位置"],
            "最后一次接收位置": plan["最后一次接收位置"],
            "扫描范围": plan["扫描范围"],
            "重复扫描区间": plan["重复扫描区间"],
            "未覆盖区间": plan["未覆盖区间"],
            "补扫清单": plan["补扫清单"],
            "分片状态": "执行中",
            "status": "执行中",
            "pending": True,
            "abnormal": False,
        })
        return self._save(row, updated, expected_revision), "分片任务已重试分片"

    # ---- 整行保存 -------------------------------------------------------
    def _save(
        self,
        row: dict[str, Any],
        updated: dict[str, Any],
        expected_revision: int | None,
    ) -> dict[str, Any]:
        """一次性写回整行；写失败会回滚，不会留下只更新一半的执行结果。"""
        entry_id = int(row["id"])
        revision = (
            store.revision(MODULE, entry_id)
            if expected_revision is None
            else expected_revision
        )
        saved = store.write_row(
            MODULE,
            entry_id,
            updated,
            owner=MODULE,
            expected_revision=revision,
        )
        return self._public(store.find(MODULE, entry_id) or saved)

    # ---- 内部工具 -------------------------------------------------------
    @staticmethod
    def _parse_revision(value: Any) -> int | None:
        if value in ("", None):
            return None
        try:
            revision = int(value)
        except (TypeError, ValueError) as exc:
            raise RangeError("版本号必须是整数") from exc
        if revision < 0:
            raise RangeError("版本号不能为负数")
        return revision

    @staticmethod
    def _is_terminal(row: dict[str, Any]) -> bool:
        return str(row.get("分片状态") or row.get("status")) == TERMINAL_STATUS

    @staticmethod
    def _public(row: dict[str, Any]) -> dict[str, Any]:
        """列表与详情共用同一份字段口径，两者不会对不上。"""
        data = copy.deepcopy(row)
        data["分片状态"] = str(row.get("分片状态") or row.get("status") or "")
        data["status"] = data["分片状态"]
        data["失败范围"] = format_ranges(normalize_ranges(row.get("失败范围")))
        data["失败范围清单"] = range_texts(normalize_ranges(row.get("失败范围")))
        data["重试次数"] = parse_position(row.get("重试次数") or 0, "重试次数")
        data["revision"] = store.revision(MODULE, int(row.get("id", 0)))
        return data

    def retry_options(self, entry_id: int) -> dict[str, Any] | None:
        """返回该分片的两种重试起点方案，供页面核对后再选择落库。"""
        row = store.find(MODULE, entry_id)
        if row is None:
            return None
        snapshot = self.execution_snapshot_for_row(row)
        confirmed = snapshot.get("上次确认位置")
        received = snapshot.get("最后一次接收位置")
        if confirmed in (None, "") or received in (None, ""):
            return {
                "分片编号": snapshot["分片编号"],
                "可用": False,
                "说明": "该分片没有同时记录上次确认位置和最后一次接收位置，"
                        "不构成重试起点冲突",
                "两种路径": [],
            }
        plans: list[dict[str, Any]] = []
        for basis in RETRY_BASES:
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
                "未覆盖区间": plan["未覆盖区间"] or "无",
                "尾部补扫清单": plan["尾部补扫清单"],
                "补扫清单": plan["补扫清单"],
            })
        return {
            "分片编号": snapshot["分片编号"],
            "可用": True,
            "说明": "两种起点都不修改数据；选定后由任务分片调度按相同口径落库",
            "两种路径": plans,
        }

    @staticmethod
    def _find_row_by_number(shard_number: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if str(row.get("分片编号")) == str(shard_number):
                return row
        return None

    def execution_snapshot_for_row(self, row: dict[str, Any]) -> dict[str, Any]:
        failed = normalize_ranges(row.get("失败范围"))
        return {
            "分片编号": row.get("分片编号"),
            "采集计划": row.get("采集计划"),
            "分片范围": row.get("分片范围"),
            "分配节点": str(row.get("分配节点") or ""),
            "重试次数": parse_position(row.get("重试次数") or 0, "重试次数"),
            "重试起点类型": row.get("重试起点类型") or "",
            "重试起点": row.get("重试起点") if row.get("重试起点") not in (None, "") else None,
            "上次确认位置": row.get("上次确认位置"),
            "最后一次接收位置": row.get("最后一次接收位置"),
            "扫描范围": row.get("扫描范围") or "",
            "失败范围": format_ranges(failed),
            "失败范围清单": range_texts(failed),
            "重复扫描区间": row.get("重复扫描区间") or "",
            "未覆盖区间": row.get("未覆盖区间") or "",
            "补扫清单": row.get("补扫清单") or "",
            "分片状态": str(row.get("分片状态") or row.get("status") or ""),
            "revision": store.revision(MODULE, int(row["id"])),
        }


def _guard_offsets(
    confirmed_position: Any,
    received_position: Any,
    shard_range: tuple[int, int] | None,
) -> tuple[int, int]:
    """校验两个采集偏移的关系，错位时给出可读提示。"""
    confirmed = parse_position(confirmed_position, "上次确认位置")
    received = parse_position(received_position, "最后一次接收位置")
    if confirmed > received:
        raise RangeError(
            f"上次确认位置 {confirmed} 大于最后一次接收位置 {received}，采集偏移存在错位"
        )
    if shard_range is not None:
        shard_start, shard_end = shard_range
        if not (shard_start <= confirmed <= shard_end):
            raise RangeError(
                f"上次确认位置 {confirmed} 超出分片范围 {format_range(shard_start, shard_end)}"
            )
        if not (shard_start <= received <= shard_end):
            raise RangeError(
                f"最后一次接收位置 {received} 超出分片范围 {format_range(shard_start, shard_end)}"
            )
    return confirmed, received


def _guard_failed_ranges(
    failed_ranges: list[tuple[int, int]],
    shard_range: tuple[int, int] | None,
) -> None:
    """失败范围必须落在分片范围内，否则聚合进度会出现跨分片的错位区间。"""
    if shard_range is None:
        return
    shard_start, shard_end = shard_range
    for start, end in failed_ranges:
        if start < shard_start or end > shard_end:
            raise RangeError(
                f"失败范围 {format_range(start, end)} 超出分片范围 "
                f"{format_range(shard_start, shard_end)}"
            )


def build_retry_plan(
    *,
    basis: str,
    shard_range: tuple[int, int] | None,
    failed_ranges: list[tuple[int, int]],
    confirmed_position: Any,
    received_position: Any,
) -> dict[str, Any]:
    """按重试起点计算实际扫描范围。

    * 从上次确认位置重试：起点回退到确认点，确认点到接收点之间会被再次扫描；
    * 从最后一次接收位置重试：起点从接收点继续，只需补扫尾部。
    """
    confirmed, received = _guard_offsets(
        confirmed_position, received_position, shard_range
    )
    if shard_range is None:
        raise RangeError("分片范围缺失，无法按重试起点生成待归并进度")
    shard_start, shard_end = shard_range

    if basis == RETRY_FROM_CONFIRMED:
        start = confirmed
        duplicate = format_range(confirmed, received) if received > confirmed else ""
        tail: list[tuple[int, int]] = []
    else:
        start = received
        duplicate = ""
        tail_start = received + 1
        tail = [(tail_start, shard_end)] if shard_end >= tail_start else []

    scan_range = format_range(start, shard_end) if start <= shard_end else ""
    # 未覆盖区间：失败范围中落在本次扫描起点之后、尚未被扫描覆盖的部分。
    uncovered = subtract_ranges(failed_ranges, (start, shard_end))
    return {
        "重试起点类型": basis,
        "重试起点": start,
        "上次确认位置": confirmed,
        "最后一次接收位置": received,
        "扫描范围": scan_range,
        "重复扫描区间": duplicate,
        "未覆盖区间": format_ranges(uncovered),
        "尾部补扫清单": range_texts(tail),
        "补扫清单": "、".join(range_texts(tail)) if tail else "无尾部补扫",
    }


__all__ = [
    "ACTION_RULES",
    "MODULE",
    "RESULT_FIELDS",
    "RETRY_BASES",
    "RETRY_FROM_CONFIRMED",
    "RETRY_FROM_RECEIVED",
    "STATUS_ORDER",
    "TERMINAL_STATUS",
    "ShardService",
    "build_retry_plan",
]
