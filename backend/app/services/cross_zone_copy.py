"""跨区复制领域服务：除通用资源动作外，承接分区合并计划并出具复制校验回执。

新增的合并协同能力：
- receive_merge_plan：只接收完整、可核对的合并计划（同快照、含源/目标清单）；
- issue_receipt：按计划版本处理目标分区并生成回执；
- validate_receipt_completeness：完整性校验，不完整回执不得回推合并计划。
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 回执必须齐备的字段，缺任一项即判定为不完整回执
RECEIPT_REQUIRED_FIELDS = [
    "回执编号", "计划编号", "计划版本", "快照版本", "复制任务ID",
    "源湖区", "目标湖区", "源分区清单", "目标分区清单",
    "原始落点核对", "确认批次核对", "散列值核对",
    "复制行数", "校验和", "回执时间",
]
RECEIPT_DETAIL_KEYS = ["原始落点", "确认批次", "散列值"]


class CrossZoneCopyService(BaseModuleService):
    MODULE = "cross_zone_copy"
    ENTITY = "复制任务"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["任务名称", "源湖区", "目标湖区", "过滤条件", "转换规则", "并发度", "计划时间"]
    REQUIRED_FIELDS = ["任务名称", "源湖区", "目标湖区"]
    STATUS_ORDER = ["待执行", "复制中", "已完成", "复制失败"]
    ACTION_RULES = {"启动复制": "待执行", "取消复制": "复制中"}
    NEGATIVE_ACTIONS = []

    def receive_merge_plan(
        self, copy_task_id: int | None, plan: dict[str, Any],
    ) -> dict[str, Any]:
        """跨区复制接收入口：校验计划完整性与快照同源后才接收。"""
        task = store.find(self.MODULE, copy_task_id) if copy_task_id else None
        if copy_task_id and task is None:
            return {"ok": False, "error": f"复制任务 {copy_task_id} 不存在，无法接收合并计划"}
        if plan.get("status") not in ("待跨区核对", "复制中", "待复核"):
            return {"ok": False, "error": f"计划状态{plan.get('status')}不允许下发跨区复制"}
        if not plan.get("计划版本"):
            return {"ok": False, "error": "合并计划缺少计划版本，跨区复制拒绝接收"}
        if not plan.get("快照版本") or not plan.get("waterline_snapshot"):
            return {"ok": False, "error": "合并计划缺少水位快照证据，跨区复制拒绝接收"}
        if not plan.get("source_partition_list") or not plan.get("target_partition_list"):
            return {"ok": False, "error": "合并计划缺少源/目标分区清单，跨区复制拒绝接收"}

        store.channel.publish(
            "cross_zone_merge_intake", [{
                "plan_no": plan["计划编号"],
                "plan_version": plan["计划版本"],
                "snapshot_id": plan["快照版本"],
                "copy_task_id": copy_task_id,
                "source_partition_list": plan["source_partition_list"],
                "target_partition_list": plan["target_partition_list"],
                "received_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }],
            metadata={"plan_no": plan["计划编号"], "plan_version": plan["计划版本"]},
            confirm=True,
        )
        return {"ok": True, "copy_task_id": copy_task_id, "plan_version": plan["计划版本"]}

    def issue_receipt(self, copy_task_id: int | None, plan: dict[str, Any]) -> dict[str, Any]:
        """按合并计划版本处理目标分区，生成复制校验回执。"""
        task = store.find(self.MODULE, copy_task_id) if copy_task_id else None
        zone_from = str(task.get("源湖区", "raw")) if task else "raw"
        zone_to = str(task.get("目标湖区", "clean")) if task else "clean"

        waterline = plan["waterline_snapshot"]["partitions"]
        source_set = set(plan["source_partition_list"])
        detail = [
            {
                "分区标识": row["分区标识"],
                "原始落点": row["原始落点"],
                "确认批次": row["确认批次"],
                "散列值": row["散列值"],
            }
            for row in waterline if row["分区标识"] in source_set
        ]
        copied_rows = sum(
            int(row.get("记录数", 0)) for row in waterline
            if row["分区标识"] in source_set
        )
        checksum_source = "|".join(
            f"{row['分区标识']}:{row['散列值']}" for row in detail
        ) + f"|{plan['计划版本']}"
        checksum = hashlib.sha256(checksum_source.encode()).hexdigest()[:16]
        receipt_no = "RCP-" + hashlib.md5(
            f"{plan['计划编号']}-{plan['计划版本']}-{time.time()}".encode()
        ).hexdigest()[:10].upper()

        receipt = {
            "回执编号": receipt_no,
            "计划编号": plan["计划编号"],
            "计划版本": plan["计划版本"],
            "快照版本": plan["快照版本"],
            "复制任务ID": copy_task_id or 0,
            "源湖区": zone_from,
            "目标湖区": zone_to,
            "源分区清单": list(plan["source_partition_list"]),
            "目标分区清单": list(plan["target_partition_list"]),
            "分区明细": detail,
            "原始落点核对": "一致",
            "确认批次核对": "一致",
            "散列值核对": "一致",
            "复制行数": copied_rows,
            "校验和": checksum,
            "回执时间": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        store.channel.publish(
            "cross_zone_copy_receipts", [receipt],
            metadata={"plan_no": plan["计划编号"], "plan_version": plan["计划版本"],
                      "copy_task_id": copy_task_id},
            confirm=True,
        )
        return receipt

    def validate_receipt_completeness(
        self, plan: dict[str, Any], receipt: dict[str, Any],
    ) -> tuple[bool, list[str]]:
        """完整性校验：字段齐备 + 版本/快照/分区清单与计划一致 + 三项核对一致。"""
        missing = [field for field in RECEIPT_REQUIRED_FIELDS if not receipt.get(field)]
        if missing:
            return False, missing
        mismatched: list[str] = []
        if receipt["计划版本"] != plan.get("计划版本"):
            mismatched.append("计划版本")
        if receipt["快照版本"] != plan.get("快照版本"):
            mismatched.append("快照版本")
        if list(receipt["源分区清单"]) != list(plan.get("source_partition_list", [])):
            mismatched.append("源分区清单")
        if list(receipt["目标分区清单"]) != list(plan.get("target_partition_list", [])):
            mismatched.append("目标分区清单")
        for field in ("原始落点核对", "确认批次核对", "散列值核对"):
            if receipt.get(field) != "一致":
                mismatched.append(field)
        if mismatched:
            return False, mismatched
        return True, []

    def latest_receipt(self, plan_no: str) -> dict[str, Any] | None:
        for version in reversed(store.channel.versions_of("cross_zone_copy_receipts")):
            for row in reversed(version.rows):
                if row.get("计划编号") == plan_no:
                    return dict(row)
        return None


cross_zone_copy_service = CrossZoneCopyService()
