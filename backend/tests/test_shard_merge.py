"""分片执行结果与聚合进度的回归测试。

运行：PYTHONPATH=.venv:. python3 -m unittest discover -s tests -v
"""
from __future__ import annotations

import unittest

from app.services.merge import MergeService
from app.services.ranges import RangeError, normalize_ranges
from app.services.shard import (
    RETRY_FROM_CONFIRMED,
    RETRY_FROM_RECEIVED,
    ShardService,
    build_retry_plan,
)
from app.store import OwnershipError, StoreError, store


class ShardResultTestCase(unittest.TestCase):
    def setUp(self) -> None:
        store.reset()
        self.shards = ShardService()
        self.merge = MergeService(self.shards)

    # ---- 执行结果整行保存 -----------------------------------------------
    def test_assign_saves_all_result_fields_together(self) -> None:
        row, message = self.shards.run_action(
            1,
            "分配节点",
            {"分配节点": "CLUS-0003", "重试次数": 2, "失败范围": "10-20;15-25"},
        )
        self.assertIsNotNone(row, message)
        stored = store.find("shard", 1)
        self.assertEqual(stored["分配节点"], "CLUS-0003")
        self.assertEqual(stored["重试次数"], 2)
        self.assertEqual(stored["失败范围"], "10-25", "重叠失败范围必须合并为唯一区间")
        self.assertEqual(len(row.get("失败范围清单", [])), 1)

    def test_failed_save_leaves_no_half_result(self) -> None:
        before = dict(store.find("shard", 1))

        def explode(module: str, entry_id: int, row: dict) -> None:
            raise RuntimeError("磁盘写入失败")

        store.write_hook = explode
        row, message = self.shards.run_action(
            1,
            "分配节点",
            {"分配节点": "CLUS-0003", "重试次数": 4, "失败范围": "1-5"},
        )
        self.assertIsNone(row)
        self.assertIn("未写入", message)
        self.assertEqual(dict(store.find("shard", 1)), before)

    def test_invalid_retry_count_rejected_without_partial_write(self) -> None:
        before = dict(store.find("shard", 2))
        row, message = self.shards.run_action(
            2, "分配节点", {"分配节点": "CLUS-0009", "重试次数": "三次"}
        )
        self.assertIsNone(row)
        self.assertIn("重试次数", message)
        self.assertEqual(dict(store.find("shard", 2)), before)

    # ---- 失败范围唯一 ---------------------------------------------------
    def test_failed_ranges_merge_and_reject_reversed(self) -> None:
        self.assertEqual(normalize_ranges("260-300;290-320"), [(260, 320)])
        with self.assertRaises(RangeError):
            normalize_ranges("320-260")

    def test_failed_status_stores_unique_range(self) -> None:
        self.shards.run_action(
            2, "标记失败", {"失败范围": "200-220;210-240;300-310"}
        )
        stored = store.find("shard", 2)
        self.assertEqual(stored["失败范围"], "200-240、300-310")
        self.assertEqual(stored["分片状态"], "执行失败")

    def test_failed_status_requires_range_in_shard_bounds(self) -> None:
        row, message = self.shards.run_action(2, "标记失败", {"失败范围": ""})
        self.assertIsNone(row)
        self.assertIn("失败范围", message)
        row, message = self.shards.run_action(2, "标记失败", {"失败范围": "390-450"})
        self.assertIsNone(row)
        self.assertIn("超出分片范围", message)

    # ---- 两种重试起点 ---------------------------------------------------
    def test_retry_from_confirmed_has_duplicate_scan(self) -> None:
        row, message = self.shards.run_action(
            2, "重试分片", {"重试起点": RETRY_FROM_CONFIRMED}
        )
        self.assertIsNotNone(row, message)
        self.assertEqual(row["重试起点"], 250)
        self.assertEqual(row["重复扫描区间"], "250-320")
        self.assertEqual(row["未覆盖区间"], "")
        self.assertEqual(row["重试次数"], 2)
        self.assertEqual(row["分片状态"], "执行中")

    def test_retry_from_received_has_tail_rescan(self) -> None:
        row, message = self.shards.run_action(
            2,
            "重试分片",
            {
                "重试起点": RETRY_FROM_RECEIVED,
                "上次确认位置": 250,
                "最后一次接收位置": 320,
            },
        )
        self.assertIsNotNone(row, message)
        self.assertEqual(row["重试起点"], 320)
        self.assertEqual(row["重复扫描区间"], "")
        self.assertEqual(row["补扫清单"], "321-399")
        self.assertEqual(row["未覆盖区间"], "260-319")

    def test_retry_requires_basis(self) -> None:
        row, message = self.shards.run_action(2, "重试分片", {})
        self.assertIsNone(row)
        self.assertIn("重试起点", message)

    def test_retry_rejects_misaligned_offsets(self) -> None:
        row, message = self.shards.run_action(
            2,
            "重试分片",
            {
                "重试起点": RETRY_FROM_RECEIVED,
                "上次确认位置": 360,
                "最后一次接收位置": 320,
            },
        )
        self.assertIsNone(row)
        self.assertIn("错位", message)

    def test_retry_plan_difference(self) -> None:
        confirmed = build_retry_plan(
            basis=RETRY_FROM_CONFIRMED,
            shard_range=(200, 399),
            failed_ranges=[(260, 320)],
            confirmed_position=250,
            received_position=320,
        )
        received = build_retry_plan(
            basis=RETRY_FROM_RECEIVED,
            shard_range=(200, 399),
            failed_ranges=[(260, 320)],
            confirmed_position=250,
            received_position=320,
        )
        self.assertEqual(confirmed["重复扫描区间"], "250-320")
        self.assertEqual(confirmed["尾部补扫清单"], [])
        self.assertEqual(received["重复扫描区间"], "")
        self.assertEqual(received["尾部补扫清单"], ["321-399"])

    # ---- 终态唯一与并发 -------------------------------------------------
    def test_concurrent_completion_keeps_single_terminal_state(self) -> None:
        revision_2 = store.revision("shard", 2)
        revision_3 = store.revision("shard", 3)
        self.shards.run_action(2, "标记完成", {"revision": revision_2})
        self.shards.run_action(3, "标记完成", {"revision": revision_3})
        self.assertEqual(store.find("shard", 2)["分片状态"], "已完成")
        self.assertEqual(store.find("shard", 3)["分片状态"], "已完成")

        row, message = self.shards.run_action(
            2, "标记失败", {"revision": revision_2, "失败范围": "200-250"}
        )
        self.assertIsNone(row)
        self.assertIn("终态", message)
        self.assertEqual(store.find("shard", 2)["分片状态"], "已完成")
        self.assertEqual(store.find("shard", 2)["失败范围"], "")

    def test_completion_clears_stale_retry_residue(self) -> None:
        self.shards.run_action(2, "重试分片", {"重试起点": RETRY_FROM_CONFIRMED})
        row, _ = self.shards.run_action(2, "标记完成", {})
        self.assertEqual(row["失败范围"], "")
        self.assertEqual(row["未覆盖区间"], "")
        self.assertEqual(row["补扫清单"], "")
        # 实际采用的起点与扫描范围要保留，聚合清单才能看出这一批从哪里开始。
        self.assertEqual(row["重试起点类型"], RETRY_FROM_CONFIRMED)
        self.assertEqual(row["重试起点"], 250)
        detail = self.shards.get_entry(2)
        self.assertEqual(detail["失败范围"], "")
        self.assertEqual(detail["未覆盖区间"], "")

    def test_revision_conflict_blocks_stale_write(self) -> None:
        stale = store.revision("shard", 2)
        self.shards.run_action(2, "标记完成", {"revision": stale})
        row, message = self.shards.run_action(
            2, "标记完成", {"revision": stale}
        )
        # 同一终态重复提交按幂等处理，但不会再写第二次。
        self.assertIsNotNone(row, message)

    def test_other_module_cannot_overwrite_shard_status(self) -> None:
        with self.assertRaises(OwnershipError):
            store.write_row(
                "shard",
                2,
                dict(store.find("shard", 2)),
                owner="merge",
            )
        row, message = self.shards.run_action(
            2, "标记失败", {"分片状态": "已完成", "失败范围": "200-210"}
        )
        self.assertIsNone(row)
        self.assertIn("不能覆盖分片状态", message)

    # ---- 列表与详情一致 -------------------------------------------------
    def test_list_and_detail_share_field_shape(self) -> None:
        items, total = self.shards.list_entries(page=1, size=10)
        self.assertEqual(total, 3)
        detail = self.shards.get_entry(2)
        listed = next(item for item in items if item["id"] == 2)
        for field in ("分片范围", "分配节点", "重试次数", "分片状态", "失败范围"):
            self.assertEqual(listed[field], detail[field], field)
        self.assertEqual(listed["重试次数"], 1)

    # ---- 聚合进度 -------------------------------------------------------
    def test_merge_progress_uses_retry_start(self) -> None:
        progress = self.merge.get_entry(1)["聚合进度"]
        shard_2 = next(
            item for item in progress["分片明细"] if item["分片编号"] == "SHAR-0002"
        )
        self.assertEqual(shard_2["重试起点类型"], RETRY_FROM_CONFIRMED)
        self.assertEqual(shard_2["重试起点"], 250)
        self.assertEqual(shard_2["待归并区间"], "250-399")
        self.assertEqual(progress["重复扫描区间"], "250-320")
        self.assertEqual(progress["待归并进度"][0]["重试起点"], 250)

    def test_merge_progress_after_received_retry(self) -> None:
        self.shards.run_action(
            2,
            "重试分片",
            {
                "重试起点": RETRY_FROM_RECEIVED,
                "上次确认位置": 250,
                "最后一次接收位置": 320,
            },
        )
        progress = self.merge.get_entry(1)["聚合进度"]
        shard_2 = next(
            item for item in progress["分片明细"] if item["分片编号"] == "SHAR-0002"
        )
        self.assertEqual(shard_2["重试起点类型"], RETRY_FROM_RECEIVED)
        self.assertEqual(shard_2["重试起点"], 320)
        self.assertEqual(shard_2["待归并区间"], "320-399")
        self.assertEqual(shard_2["未覆盖区间"], "260-319")
        self.assertIn("321-399", shard_2["审批补扫清单"])

    def test_merge_progress_includes_complete_assignment_fields(self) -> None:
        progress = self.merge.get_entry(1)["聚合进度"]
        for entry in progress["分片明细"]:
            self.assertIn("分配节点", entry)
            self.assertIn("重试次数", entry)
            self.assertIn("失败范围", entry)
        shard_2 = next(
            item for item in progress["分片明细"] if item["分片编号"] == "SHAR-0002"
        )
        self.assertEqual(shard_2["分配节点"], "CLUS-0001")
        self.assertEqual(shard_2["失败范围"], "260-320")

    def test_retry_comparison_lists_two_paths(self) -> None:
        payload = self.merge.retry_comparison(1, "SHAR-0002")
        self.assertTrue(payload["可用"])
        paths = {item["重试起点类型"]: item for item in payload["两种路径"]}
        self.assertEqual(paths[RETRY_FROM_CONFIRMED]["重复扫描区间"], "250-320")
        self.assertEqual(paths[RETRY_FROM_RECEIVED]["补扫清单"], "321-399")

    # ---- 覆盖缺口 -------------------------------------------------------
    def test_coverage_gap_only_counts_unscanned_shards(self) -> None:
        progress = self.merge.get_entry(1)["聚合进度"]
        # SHAR-0001 还没分配节点，整段都没扫；SHAR-0002 已从确认点重试过。
        self.assertEqual(progress["覆盖缺口"], "0-199")
        self.assertEqual(progress["本批扫描量"], 150 + 200)

    def test_coverage_gap_grows_with_received_retry(self) -> None:
        self.shards.run_action(
            2,
            "重试分片",
            {
                "重试起点": RETRY_FROM_RECEIVED,
                "上次确认位置": 250,
                "最后一次接收位置": 320,
            },
        )
        progress = self.merge.get_entry(1)["聚合进度"]
        self.assertEqual(progress["覆盖缺口"], "0-199、260-319")
        self.assertEqual(progress["未覆盖区间"], "260-319")

    def test_completed_merge_stores_unique_failure_scope(self) -> None:
        row, message = self.merge.run_action(1, "归档任务", {})
        self.assertIsNotNone(row, message)
        self.assertEqual(row["失败范围"], "0-199")
        self.assertEqual(row["任务状态"], "归并失败")

    # ---- 下游清单 -------------------------------------------------------
    def test_repair_issue_list_tracks_missing_fields(self) -> None:
        issues = {item["分片编号"]: item for item in self.merge.repair_field_issues()}
        self.assertIn("SHAR-0001", issues)
        self.assertIn("分配节点", issues["SHAR-0001"]["缺失字段"])
        self.assertNotIn("重试次数", issues["SHAR-0001"]["缺失字段"])

        self.shards.run_action(
            1, "分配节点", {"分配节点": "CLUS-0003", "重试次数": 0}
        )
        issues = {item["分片编号"]: item for item in self.merge.repair_field_issues()}
        self.assertNotIn("SHAR-0001", issues)

    def test_downstream_feed_bundles_repairs_and_results(self) -> None:
        payload = self.merge.downstream_feed(1)
        self.assertIn("聚合进度", payload)
        self.assertIn("修复分片字段丢失待处理清单", payload)
        self.assertEqual(len(payload["分片执行结果数据"]), 3)
        shard_2 = next(
            item
            for item in payload["分片执行结果数据"]
            if item["分片编号"] == "SHAR-0002"
        )
        self.assertEqual(shard_2["分配节点"], "CLUS-0001")
        self.assertEqual(shard_2["重试次数"], 1)

    def test_missing_shard_range_reports_error(self) -> None:
        store.find("shard", 2)["分片范围"] = ""
        row, message = self.shards.run_action(
            2, "重试分片", {"重试起点": RETRY_FROM_CONFIRMED}
        )
        self.assertIsNone(row)
        self.assertIn("分片范围", message)


class StoreTestCase(unittest.TestCase):
    def setUp(self) -> None:
        store.reset()

    def test_write_row_is_all_or_nothing(self) -> None:
        before = dict(store.find("shard", 1))
        new_row = dict(before)
        new_row["分配节点"] = "CLUS-0009"

        def explode(*_: object) -> None:
            raise RuntimeError("boom")

        store.write_hook = explode
        with self.assertRaises(StoreError):
            store.write_row("shard", 1, new_row, owner="shard")
        self.assertEqual(dict(store.find("shard", 1)), before)

    def test_cross_module_append_rejected(self) -> None:
        with self.assertRaises(OwnershipError):
            store.append("shard", {"id": 99}, owner="merge")


if __name__ == "__main__":
    unittest.main()
