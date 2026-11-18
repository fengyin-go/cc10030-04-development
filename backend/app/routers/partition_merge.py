"""分区合并计划专用路由：阶段化处理"分区策略 → 合并计划 → 跨区复制"链路。

通用资源 CRUD/动作仍走 resource 路由；本路由只承载合并链路的阶段接口，
路径均为固定前缀 /api/partition-merge，必须先于 /api/{module} 动态路由注册。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.partition_merge_plan import (
    RELEASE_MODE_SNAPSHOT,
    partition_merge_plan_service as merge_service,
)
from app.services.partition_policy import PartitionPolicyService

router = APIRouter(prefix="/api/partition-merge", tags=["分区合并计划"])


class GeneratePlanPayload(BaseModel):
    policy_id: int
    copy_task_id: int | None = None
    release_mode: str = RELEASE_MODE_SNAPSHOT
    partitions: list[dict[str, Any]] | None = None
    draft_id: int | None = None


def _require_ok(result: dict[str, Any]) -> dict[str, Any]:
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message") or result.get("error"))
    return result


@router.get("/waterline")
def scan_waterline(policy_id: int, small_threshold_mb: int = 128) -> dict[str, Any]:
    """阶段1：读取分区规则与分区水位，产出同源快照。"""
    result = merge_service.scan_waterline(
        policy_id, small_threshold_mb=small_threshold_mb)
    if not result["ok"]:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.get("/change-points")
def change_points(policy_id: int) -> dict[str, Any]:
    """分区策略主数据定位改动点（历史小分区与当天新增分区同时触顶）。"""
    result = PartitionPolicyService().locate_change_points(policy_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.post("/plans")
def generate_plan(payload: GeneratePlanPayload) -> dict[str, Any]:
    """阶段2/3：阈值判定、分组对账、事务落库生成分区合并计划。"""
    result = merge_service.generate_plan(
        policy_id=payload.policy_id,
        copy_task_id=payload.copy_task_id,
        release_mode=payload.release_mode,
        partitions=payload.partitions,
        draft_id=payload.draft_id,
    )
    if not result["ok"]:
        raise HTTPException(
            status_code=400, detail=result.get("message") or result.get("error"))
    return result


@router.get("/plans/{plan_id}")
def plan_detail(plan_id: int) -> dict[str, Any]:
    plan = merge_service.get_entry(plan_id)
    if plan is None:
        raise HTTPException(status_code=404, detail=f"分区合并计划 {plan_id} 不存在")
    return plan


@router.post("/plans/{plan_id}/execute")
def execute_merge(plan_id: int) -> dict[str, Any]:
    """阶段4：执行合并，中断重试未完成分组并保留回滚依据。"""
    return _require_ok(merge_service.execute_merge(plan_id))


@router.post("/plans/{plan_id}/recheck")
def recheck_plan(plan_id: int) -> dict[str, Any]:
    """阶段5：阈值/口径/判定/规则/上限/范围/条件/标准八维复核。"""
    return _require_ok(merge_service.recheck_plan(plan_id))


@router.post("/plans/{plan_id}/dispatch")
def dispatch_to_copy(plan_id: int) -> dict[str, Any]:
    """阶段6：跨区复制按计划版本处理目标分区并生成回执。"""
    return _require_ok(merge_service.dispatch_to_copy(plan_id))


@router.post("/plans/{plan_id}/receipt")
def confirm_receipt(plan_id: int, receipt: dict[str, Any] | None = None) -> dict[str, Any]:
    """阶段7：只接收完整复制校验回执，结果回写待复核清单。"""
    return _require_ok(merge_service.receive_receipt(plan_id, result_receipt=receipt))


@router.post("/plans/{plan_id}/archive")
def archive_plan(plan_id: int) -> dict[str, Any]:
    """阶段8：归档消费侧结果，乙口径在此延迟释放存储。"""
    return _require_ok(merge_service.archive_plan(plan_id))


@router.post("/plans/{plan_id}/rollback")
def rollback_plan(plan_id: int) -> dict[str, Any]:
    """按回滚依据恢复合并前分区状态。"""
    return _require_ok(merge_service.rollback_plan(plan_id))


@router.get("/pending-queue")
def pending_queue() -> dict[str, Any]:
    """超出生命周期上限/最大分区限制的对象队列。"""
    items = merge_service.pending_queue()
    return {"items": items, "total": len(items)}


@router.get("/review-checklist")
def review_checklist() -> dict[str, Any]:
    """跨区复制确认结果回写的待复核清单。"""
    items = merge_service.review_checklist()
    return {"items": items, "total": len(items)}


@router.get("/cross-status")
def cross_status(plan_no: str | None = None) -> dict[str, Any]:
    """跨模块状态流转记录。"""
    items = merge_service.cross_module_status(plan_no)
    return {"items": items, "total": len(items)}


@router.get("/snapshots")
def snapshots() -> dict[str, Any]:
    """水位快照登记本：核对计划与水位是否同源。"""
    items = merge_service.snapshots()
    return {"items": items, "total": len(items)}
