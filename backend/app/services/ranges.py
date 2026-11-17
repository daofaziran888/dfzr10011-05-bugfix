"""采集偏移区间与失败范围的处理工具。

分片执行结果里的「失败范围」「重复扫描区间」「未覆盖区间」都必须按同一口径
规范化：区间端点用非负整数表示，多个区间会合并重叠与相邻部分，最终只保留
唯一区间。这样聚合进度里不会出现重复或互相覆盖的失败范围。
"""
from __future__ import annotations

import re
from typing import Any, Iterable

Range = tuple[int, int]

_RANGE_SEPARATORS = re.compile(r"[,，;；、\s]+")
_SINGLE_RANGE = re.compile(r"^\s*(-?\d+)\s*(?:-|~|至)\s*(-?\d+)\s*$")


class RangeError(ValueError):
    """区间文本无法解析时抛出，由上层转成可读的校验提示。"""


def parse_position(value: Any, field: str = "采集偏移") -> int:
    """把位置字段解析成非负整数，空值按 0 处理。"""
    if value is None or value == "":
        return 0
    if isinstance(value, bool):
        raise RangeError(f"{field}必须是整数偏移，不能是布尔值")
    try:
        position = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise RangeError(f"{field}「{value}」不是有效的整数偏移") from exc
    if position < 0:
        raise RangeError(f"{field}不能为负数")
    return position


def parse_range_text(text: Any) -> Range:
    """解析单个区间，支持 `100-200`、`100~200`、`100至200` 三种写法。"""
    match = _SINGLE_RANGE.match(str(text or ""))
    if not match:
        raise RangeError(f"失败范围「{text}」不是有效的区间，应写成 起始偏移-结束偏移")
    start, end = int(match.group(1)), int(match.group(2))
    if start < 0 or end < 0:
        raise RangeError("失败范围不能使用负偏移")
    if start > end:
        raise RangeError(f"失败范围「{text}」的起始偏移大于结束偏移")
    return start, end


def normalize_ranges(value: Any) -> list[Range]:
    """把多种输入形态规范化成唯一区间列表。

    支持：`"100-200"`、`"100-200;300-400"`、`[{"start": 1, "end": 2}]`、
    `[[1, 2], [3, 4]]` 以及上述混合列表。重叠或首尾相接的区间会被合并。
    """
    if value is None or value == "":
        return []
    raw_items: Iterable[Any]
    if isinstance(value, (list, tuple, set)):
        raw_items = value
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        if _SINGLE_RANGE.match(stripped):
            # 允许「100 - 200」这种带空格的单个区间，先整体解析再拆分。
            raw_items = [stripped]
        else:
            raw_items = [item for item in _RANGE_SEPARATORS.split(stripped) if item]
    else:
        raw_items = [value]

    parsed: list[Range] = []
    for item in raw_items:
        if isinstance(item, dict):
            start = parse_position(item.get("start", item.get("起始偏移", item.get("from"))))
            end = parse_position(item.get("end", item.get("结束偏移", item.get("to"))))
            if start > end:
                raise RangeError("失败范围的起始偏移大于结束偏移")
            parsed.append((start, end))
        elif isinstance(item, (list, tuple)) and len(item) == 2:
            start = parse_position(item[0])
            end = parse_position(item[1])
            if start > end:
                raise RangeError("失败范围的起始偏移大于结束偏移")
            parsed.append((start, end))
        else:
            parsed.append(parse_range_text(item))
    return merge_ranges(parsed)


def merge_ranges(ranges: Iterable[Range]) -> list[Range]:
    """合并重叠或首尾相接的区间，返回按起点排序的唯一区间。"""
    ordered = sorted(ranges)
    merged: list[Range] = []
    for start, end in ordered:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def format_range(start: int, end: int) -> str:
    return f"{start}-{end}"


def format_ranges(ranges: Iterable[Range]) -> str:
    return "、".join(format_range(start, end) for start, end in ranges)


def range_texts(ranges: Iterable[Range]) -> list[str]:
    return [format_range(start, end) for start, end in ranges]


def parse_range_bounds(text: Any) -> Range | None:
    """解析分片范围，无法解析时返回 None。"""
    try:
        return parse_range_text(text)
    except RangeError:
        return None


def subtract_ranges(ranges: Iterable[Range], covered: Range) -> list[Range]:
    """返回 ``ranges`` 中未被 ``covered`` 覆盖的部分，作为未覆盖区间。"""
    cover_start, cover_end = covered
    remaining: list[Range] = []
    for start, end in merge_ranges(ranges):
        if end < cover_start or start > cover_end:
            remaining.append((start, end))
            continue
        if start < cover_start:
            remaining.append((start, cover_start - 1))
        if end > cover_end:
            remaining.append((cover_end + 1, end))
    return merge_ranges(remaining)


def unique_texts(values: Iterable[Any]) -> list[str]:
    """按首次出现顺序去重非空文本。"""
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def dedupe_fields(value: Any) -> list[str]:
    """字段丢失清单去重：支持列表、逗号/分号分隔的文本。"""
    if value is None or value == "":
        return []
    if isinstance(value, (list, tuple, set)):
        raw = list(value)
    else:
        raw = _RANGE_SEPARATORS.split(str(value))
    return unique_texts(raw)
