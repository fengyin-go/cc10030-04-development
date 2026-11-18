"""分区策略领域服务：维护分区规则主数据，并支撑合并时机的阈值判定与改动点定位。"""
# 数据工程核心：数据入湖存储与分区管理 — 跨模块数据产物
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class PartitionPolicyService(BaseModuleService):
    MODULE = "partition_policy"
    ENTITY = "分区规则"
    OPERATION_TERM = "合并"
    LIST_FIELDS = ["规则名称", "目标数据集", "分区键", "分区粒度", "生命周期", "合并策略", "最大分区数"]
    REQUIRED_FIELDS = ["规则名称", "目标数据集", "分区键"]
    STATUS_ORDER = ["草稿", "已生效", "已过期"]
    ACTION_RULES = {"启用规则": "草稿", "废弃规则": "已生效"}
    NEGATIVE_ACTIONS = []

    def locate_change_points(self, policy_id: int) -> dict[str, Any]:
        """分区策略主数据定位改动点：历史小分区与当天新增分区同时触顶时给出依据。

        水位以分区合并计划最近一次同源快照为准，分区策略负责解释阈值与改动点。
        """
        policy = store.find(self.MODULE, policy_id)
        if policy is None:
            return {"ok": False, "error": f"分区策略 {policy_id} 不存在"}

        max_partitions = self._int(policy.get("最大分区数"))
        latest_waterline: list[dict[str, Any]] = []
        for version in reversed(store.channel.versions_of("partition_water_levels")):
            if version.metadata.get("policy_id") == policy_id:
                latest_waterline = list(version.rows)
                break

        historical_small = [p["分区标识"] for p in latest_waterline
                            if not p.get("是否当天新增") and p.get("是否小分区")]
        today_new = [p["分区标识"] for p in latest_waterline if p.get("是否当天新增")]
        expired = [p["分区标识"] for p in latest_waterline if p.get("是否过期")]
        current_count = len(latest_waterline)
        return {
            "ok": True,
            "策略ID": policy_id,
            "规则名称": policy.get("规则名称"),
            "最大分区数": max_partitions,
            "当前分区数": current_count,
            "是否触顶": current_count >= max_partitions if max_partitions else False,
            "历史小分区": historical_small,
            "当天新增分区": today_new,
            "过期分区": expired,
            "改动点": "当天新增分区计入分区分母但不参与合并；合并历史小分区/过期分区回收空间",
        }

    @staticmethod
    def _int(value: Any) -> int:
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return 0
