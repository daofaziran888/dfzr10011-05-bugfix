"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

除了简单的增删查，这里还提供两件分片执行结果依赖的能力：

* ``write_row`` 一次性替换整行，并在异常时回滚，避免只落一半字段；
* ``transaction`` 在多步写入中提供快照，任何一步失败都整体回滚。

写入方必须声明 ``owner``，且只能写自己模块的行。分片状态由此只允许
任务分片调度服务修改，其他模块只能读。
"""
from __future__ import annotations

import copy
import threading
from contextlib import contextmanager
from typing import Any, Callable, Iterator

from app.seed import SEED_ROWS


class StoreError(RuntimeError):
    """仓库写入失败。"""


class OwnershipError(StoreError):
    """跨模块写入：只有行所属模块才能修改自己的状态。"""


class RevisionConflict(StoreError):
    """乐观锁冲突：行在读取之后已被其他请求改写。"""


class Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._sequence = 0
        # 故障注入点：写入前调用，抛异常即可验证「保存失败不留下半条结果」。
        self.write_hook: Callable[[str, int, dict[str, Any]], None] | None = None
        self.reset()

    def reset(self) -> None:
        """恢复示例数据，测试和本地复现时可以反复从同一份初始态开始。"""
        with self._lock:
            self._tables: dict[str, list[dict[str, Any]]] = {
                name: [copy.deepcopy(row) for row in rows]
                for name, rows in SEED_ROWS.items()
            }
            self._revisions: dict[str, dict[int, int]] = {
                name: {int(row.get("id", 0)): 0 for row in rows}
                for name, rows in self._tables.items()
            }
            self._sequence = 0
            self.write_hook = None

    # ---- 读 -------------------------------------------------------------
    def module_names(self) -> list[str]:
        with self._lock:
            return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        with self._lock:
            return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        with self._lock:
            for row in self.rows(module):
                if int(row.get("id", 0)) == entry_id:
                    return row
            return None

    def revision(self, module: str, entry_id: int) -> int:
        with self._lock:
            return int(self._revisions.setdefault(module, {}).get(entry_id, 0))

    def next_sequence(self) -> int:
        with self._lock:
            self._sequence += 1
            return self._sequence

    # ---- 新增 -----------------------------------------------------------
    def append(self, module: str, row: dict[str, Any], *, owner: str) -> dict[str, Any]:
        """追加一条记录；同样是整行写入，不会出现登记到一半的记录。"""
        if owner != module:
            raise OwnershipError(
                f"模块「{owner}」不能向模块「{module}」写入记录"
            )
        with self._lock:
            rows = self.rows(module)
            entry_id = int(row.get("id", 0))
            if any(int(item.get("id", 0)) == entry_id for item in rows):
                raise StoreError(f"记录 {entry_id} 已存在，不能重复登记")
            payload = copy.deepcopy(row)
            try:
                if self.write_hook is not None:
                    self.write_hook(module, entry_id, copy.deepcopy(payload))
                rows.append(payload)
                self._revisions.setdefault(module, {})[entry_id] = 0
                self._sequence += 1
            except Exception as exc:
                raise StoreError(
                    f"记录 {entry_id} 保存失败，本次登记未写入：{exc}"
                ) from exc
            return copy.deepcopy(row)

    # ---- 写 -------------------------------------------------------------
    def write_row(
        self,
        module: str,
        entry_id: int,
        new_row: dict[str, Any],
        *,
        owner: str,
        expected_revision: int | None = None,
    ) -> dict[str, Any]:
        """整行替换一条记录。

        ``expected_revision`` 用来做乐观锁：读到旧版本后再提交会被拒绝，
        从而保证并发完成时只有一个终态真正落库。
        """
        if owner != module:
            raise OwnershipError(
                f"模块「{owner}」不能改写模块「{module}」的记录，分片状态只能由所属模块维护"
            )
        with self._lock:
            row = self.find(module, entry_id)
            if row is None:
                raise StoreError(f"记录 {entry_id} 不存在或已归档")
            current = self.revision(module, entry_id)
            if expected_revision is not None and int(expected_revision) != current:
                raise RevisionConflict(
                    f"记录 {entry_id} 已被其他请求更新（版本 {current}），本次结果未写入"
                )
            backup = copy.deepcopy(row)
            payload = copy.deepcopy(new_row)
            try:
                if self.write_hook is not None:
                    self.write_hook(module, entry_id, copy.deepcopy(payload))
                # 就地替换，保证此前拿到的行引用仍然指向同一条记录。
                row.clear()
                row.update(payload)
                self._revisions.setdefault(module, {})[entry_id] = current + 1
                self._sequence += 1
            except Exception as exc:
                row.clear()
                row.update(backup)
                raise StoreError(f"记录 {entry_id} 保存失败，本次执行结果未写入：{exc}") from exc
            return copy.deepcopy(row)

    # ---- 事务 -----------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "tables": copy.deepcopy(self._tables),
                "revisions": copy.deepcopy(self._revisions),
                "sequence": self._sequence,
            }

    def restore(self, snapshot: dict[str, Any]) -> None:
        with self._lock:
            self._tables = snapshot["tables"]
            self._revisions = snapshot["revisions"]
            self._sequence = snapshot["sequence"]

    @contextmanager
    def transaction(self) -> Iterator["Store"]:
        """多步写入快照：块内任何异常都会把全部模块恢复到进入前的状态。"""
        snapshot = self.snapshot()
        try:
            yield self
        except Exception:
            self.restore(snapshot)
            raise

    # ---- 概览 -----------------------------------------------------------
    def overview(self) -> dict[str, object]:
        with self._lock:
            modules: list[dict[str, object]] = []
            for name in self.module_names():
                rows = self.rows(name)
                modules.append({
                    "name": name,
                    "created": len(rows),
                    "pending": sum(1 for row in rows if row.get("pending")),
                    "abnormal": sum(1 for row in rows if row.get("abnormal")),
                })
            cards = [
                {"label": "业务模块", "value": len(modules)},
                {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
                {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
                {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
            ]
            return {"cards": cards, "modules": modules}


store = Store()
