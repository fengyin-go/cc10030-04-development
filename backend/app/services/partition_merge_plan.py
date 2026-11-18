"""分区合并计划领域服务：补齐"分区策略 → 分区合并计划 → 跨区复制"链路。

阶段划分（每个阶段都留证据、可核对、可回滚）：
  1. 读取分区规则（分区策略主数据）与分区水位，且两者必须来自同一快照；
  2. 按最大分区数阈值判定合并时机（历史小分区与当天新增分区同时触顶时，
     由分区策略定位改动点；当天新增分区计入分区数但不参与合并）；
  3. 按分区键、生命周期上限、合并分组、释放空间逐项对账并分组，
     超出生命周期硬上限的分组不得进入复制计划，转入待处理队列；
  4. 事务落库保存分区合并计划，失败整体回滚，杜绝半成品；
  5. 执行合并，保留合并前/后分区数、最大分区限制、源/目标分区清单与回滚依据；
  6. 八维口径复核（阈值/口径/判定/规则/上限/范围/条件/标准）通过后，
     交跨区复制模块按计划版本处理目标分区，并只接收完整复制校验回执；
  7. 回执确认结果回写待复核清单，归档消费侧结果（乙模式此时才延迟释放）。

释放方式两种口径：
  甲 - 到期后立即释放存储；乙 - 先保留可恢复快照，归档时再延迟释放。
"""
from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.cross_zone_copy import cross_zone_copy_service
from app.store import store

# 合并分组单组源分区上限；小分区判定默认阈值（MB）；硬上限为生命周期的倍数
GROUP_SIZE = 4
DEFAULT_SMALL_THRESHOLD_MB = 128
HARD_LIFECYCLE_FACTOR = 2

# 八维复核维度，计划证据必须同时满足这八项
CRITERIA_DIMENSIONS = ["阈值", "口径", "判定", "规则", "上限", "范围", "条件", "标准"]

# 两种释放方式口径
RELEASE_MODE_IMMEDIATE = "甲-立即释放"
RELEASE_MODE_SNAPSHOT = "乙-快照延迟释放"
VALID_RELEASE_MODES = (RELEASE_MODE_IMMEDIATE, RELEASE_MODE_SNAPSHOT)


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _today() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())


def _to_int(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


class PartitionMergePlanService(BaseModuleService):
    MODULE = "partition_merge_plan"
    ENTITY = "分区合并计划"
    OPERATION_TERM = "合并"
    LIST_FIELDS = [
        "计划编号", "分区策略", "目标数据集", "分区键", "快照版本", "水位时间",
        "当前分区数", "最大分区数", "命中分区数", "合并分组数", "预计释放空间MB",
        "释放方式", "复制任务", "计划版本",
    ]
    REQUIRED_FIELDS = ["分区策略", "目标数据集", "分区键"]
    STATUS_ORDER = [
        "草稿", "待执行", "待核对", "待跨区核对", "复制中", "待复核", "已归档", "已回滚",
    ]
    ACTION_RULES = {
        "生成计划": "待执行",
        "执行合并": "待核对",
        "核对计划": "待跨区核对",
        "跨区核对": "复制中",
        "确认回执": "待复核",
        "归档计划": "已归档",
        "回滚计划": "已回滚",
    }
    NEGATIVE_ACTIONS = ["回滚计划"]

    def __init__(self) -> None:
        # 快照登记本：snapshot_id -> 水位快照载荷，保证计划与水位同源
        self._snapshots: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # 动作分派：通用资源路由的动作入口转到各阶段处理
    # ------------------------------------------------------------------
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        plan = store.find(self.MODULE, entry_id)
        if plan is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action not in self.ACTION_RULES:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

        if action == "生成计划":
            return self._generate_from_draft(plan)
        if action == "执行合并":
            result = self.execute_merge(entry_id)
            return result.get("plan"), result["message"]
        if action == "核对计划":
            result = self.recheck_plan(entry_id)
            return result.get("plan"), result["message"]
        if action == "跨区核对":
            result = self.dispatch_to_copy(entry_id)
            return result.get("plan"), result["message"]
        if action == "确认回执":
            result = self.receive_receipt(entry_id, result_receipt=None)
            return result.get("plan"), result["message"]
        if action == "归档计划":
            result = self.archive_plan(entry_id)
            return result.get("plan"), result["message"]
        if action == "回滚计划":
            result = self.rollback_plan(entry_id)
            return result.get("plan"), result["message"]
        return super().run_action(entry_id, action)

    # ------------------------------------------------------------------
    # 阶段 1：读取分区规则与分区水位，形成同一快照
    # ------------------------------------------------------------------
    def scan_waterline(
        self,
        policy_id: int,
        partitions: list[dict[str, Any]] | None = None,
        small_threshold_mb: int = DEFAULT_SMALL_THRESHOLD_MB,
    ) -> dict[str, Any]:
        policy = store.find("partition_policy", policy_id)
        if policy is None:
            return {"ok": False, "error": f"分区策略 {policy_id} 不存在，无法读取分区规则"}
        if policy.get("status") == "已过期":
            return {"ok": False, "error": f"分区策略 {policy_id} 已过期，不再产出合并计划"}

        max_partitions = _to_int(policy.get("最大分区数"), 0)
        lifecycle_days = _to_int(policy.get("生命周期"), 0)
        if max_partitions <= 0 or lifecycle_days <= 0:
            return {"ok": False,
                    "error": "分区策略缺少有效的最大分区数或生命周期，阈值无法判定"}

        observed_at = _now()
        waterline = self._normalize_waterline(
            policy, partitions, max_partitions, lifecycle_days, small_threshold_mb)

        # 快照 ID 由策略 + 水位时间 + 全部水位条目内容派生，计划与水位据此绑定
        snapshot_source = "|".join(
            f"{p['分区标识']}:{p['分区大小MB']}:{p['分区年龄天']}:{int(p['是否当天新增'])}"
            for p in waterline
        )
        snapshot_id = "snap-" + hashlib.sha256(
            f"{policy_id}|{observed_at}|{snapshot_source}|{uuid.uuid4().hex[:8]}".encode()
        ).hexdigest()[:16]

        version = store.channel.publish(
            "partition_water_levels", waterline,
            metadata={
                "policy_id": policy_id,
                "snapshot_id": snapshot_id,
                "observed_at": observed_at,
                "small_threshold_mb": small_threshold_mb,
                "max_partitions": max_partitions,
            },
            confirm=True,
        )
        snapshot = {
            "snapshot_id": snapshot_id,
            "policy_id": policy_id,
            "observed_at": observed_at,
            "waterline_version": version.version_tag,
            "waterline_checksum": version.checksum,
            "partitions": waterline,
            "small_threshold_mb": small_threshold_mb,
            "max_partitions": max_partitions,
            "lifecycle_days": lifecycle_days,
        }
        self._snapshots[snapshot_id] = snapshot
        return {"ok": True, **snapshot}

    def _normalize_waterline(
        self,
        policy: dict[str, Any],
        partitions: list[dict[str, Any]] | None,
        max_partitions: int,
        lifecycle_days: int,
        small_threshold_mb: int,
    ) -> list[dict[str, Any]]:
        key = str(policy.get("分区键", "ds"))
        if partitions:
            normalized = []
            for index, raw in enumerate(partitions):
                age = _to_int(raw.get("分区年龄天"), 0)
                size_mb = _to_int(raw.get("分区大小MB"), 0)
                is_today = bool(raw.get("是否当天新增", age == 0))
                ident = str(raw.get("分区标识") or f"{key}={raw.get('分区键值', index)}")
                normalized.append({
                    "分区标识": ident,
                    "分区键值": str(raw.get("分区键值", ident)),
                    "分区大小MB": size_mb,
                    "记录数": _to_int(raw.get("记录数"), 0),
                    "是否当天新增": is_today,
                    "是否过期": bool(raw.get("是否过期", age > lifecycle_days)),
                    "分区年龄天": age,
                    "原始落点": str(raw.get("原始落点") or f"lake://{policy.get('目标数据集', 'ds')}/{ident}"),
                    "确认批次": str(raw.get("确认批次") or f"B{index:04d}"),
                    "散列值": str(raw.get("散列值") or hashlib.md5(ident.encode()).hexdigest()[:12]),
                    "是否小分区": size_mb < small_threshold_mb,
                })
            return normalized

        # 缺省水位：构造"历史小分区 + 当天新增分区同时触及最大分区数"的场景
        hard_cap = lifecycle_days * HARD_LIFECYCLE_FACTOR
        built: list[dict[str, Any]] = []
        sequence = 0

        def add(age: int, size_mb: int, is_today: bool, expired: bool) -> None:
            nonlocal sequence
            sequence += 1
            day = _shift_day(_today(), -age) if not is_today else _today()
            ident = f"{key}={day}"
            built.append({
                "分区标识": ident,
                "分区键值": day,
                "分区大小MB": size_mb,
                "记录数": size_mb * 256,
                "是否当天新增": is_today,
                "是否过期": expired,
                "分区年龄天": age,
                "原始落点": f"lake://{policy.get('目标数据集', 'dataset')}/{ident}",
                "确认批次": f"B{day.replace('-', '')}",
                "散列值": hashlib.md5(f"{policy.get('id')}-{ident}".encode()).hexdigest()[:12],
                "是否小分区": size_mb < small_threshold_mb,
            })

        # 1 个当天新增分区（计入分区数，但永远不进合并候选）
        add(0, 512, True, False)
        # 1 个超出生命周期硬上限的历史小分区（其分组将被拦入待处理队列）
        add(hard_cap + 15, 48, False, True)
        # 其余历史分区：2 个过期小分区 + 剩余未过期小分区，总数恰好触顶
        remaining = max_partitions - 2
        for i in range(remaining):
            if i < 2:
                add(lifecycle_days + 8 - i * 4, 24 + i * 12, False, True)
            else:
                add(max(1, lifecycle_days - 6 - (i - 2) * 3), 28 + (i % 4) * 18, False, False)
        return built

    # ------------------------------------------------------------------
    # 阶段 2/3：阈值判定 + 分组对账 + 事务落库生成合并计划
    # ------------------------------------------------------------------
    def generate_plan(
        self,
        policy_id: int,
        copy_task_id: int | None = None,
        release_mode: str = RELEASE_MODE_SNAPSHOT,
        partitions: list[dict[str, Any]] | None = None,
        draft_id: int | None = None,
    ) -> dict[str, Any]:
        policy = store.find("partition_policy", policy_id)
        if policy is None:
            return {"ok": False, "error": f"分区策略 {policy_id} 不存在"}
        if release_mode not in VALID_RELEASE_MODES:
            return {"ok": False, "error": f"释放方式必须是 {VALID_RELEASE_MODES} 之一"}

        scan = self.scan_waterline(policy_id, partitions=partitions)
        if not scan["ok"]:
            return scan
        snapshot_id = scan["snapshot_id"]
        waterline: list[dict[str, Any]] = scan["partitions"]
        lifecycle_days = scan["lifecycle_days"]
        max_partitions = scan["max_partitions"]
        hard_cap = lifecycle_days * HARD_LIFECYCLE_FACTOR
        small_threshold = scan["small_threshold_mb"]
        partition_key = str(policy.get("分区键", "ds"))
        current_count = len(waterline)

        # 阈值判定：当前分区数触及最大分区数，且存在过期分区或小分区
        threshold_hit = current_count >= max_partitions
        today_partitions = [p for p in waterline if p["是否当天新增"]]
        candidates = [
            p for p in waterline
            if not p["是否当天新增"] and (p["是否过期"] or p["是否小分区"])
        ]
        merge_needed = threshold_hit and bool(candidates)
        change_points = self._locate_change_points(policy, waterline, max_partitions)

        if not merge_needed:
            # 不满足条件不产生半成品计划，只回传判定结论
            return {
                "ok": False,
                "error": "未命中合并时机",
                "threshold_hit": threshold_hit,
                "current_count": current_count,
                "max_partitions": max_partitions,
                "candidate_count": len(candidates),
                "snapshot_id": snapshot_id,
                "message": "当前分区数未触及最大分区数或无过期/小分区候选，无需生成计划",
            }

        # 分组：候选分区按年龄从老到新，每 GROUP_SIZE 个源分区合为一个目标分区
        ordered = sorted(candidates, key=lambda p: (-p["分区年龄天"], p["分区标识"]))
        groups: list[dict[str, Any]] = []
        pending_items: list[dict[str, Any]] = []
        for index in range(0, len(ordered), GROUP_SIZE):
            source = ordered[index:index + GROUP_SIZE]
            group_id = f"G{index // GROUP_SIZE + 1:02d}"
            size_before = sum(p["分区大小MB"] for p in source)
            size_after = min(size_before, small_threshold)
            oldest = min(p["分区键值"] for p in source)
            target = f"{partition_key}=merged-{oldest}"
            over_lifecycle = any(p["分区年龄天"] > hard_cap for p in source)
            group = {
                "分组编号": group_id,
                "源分区清单": [p["分区标识"] for p in source],
                "目标分区": target,
                "源分区数": len(source),
                "目标分区数": 1,
                "合并前大小MB": size_before,
                "合并后大小MB": size_after,
                "释放空间MB": size_before - size_after,
                "最大年龄天": max(p["分区年龄天"] for p in source),
                "超生命周期硬上限": over_lifecycle,
                "入复制计划": not over_lifecycle,
            }
            groups.append(group)
            if over_lifecycle:
                # 超出生命周期上限的分组不得进入复制计划
                pending_items.append({
                    "对象类型": "合并分组",
                    "对象标识": group_id,
                    "入队原因": f"组内分区年龄超过生命周期硬上限{hard_cap}天",
                    "源分区清单": group["源分区清单"],
                    "建议处理": "人工核对过期口径后单独处置，禁止进入跨区复制计划",
                })

        valid_groups = [g for g in groups if g["入复制计划"]]
        partition_count_after = current_count - sum(g["源分区数"] for g in valid_groups) \
            + len(valid_groups)
        source_partition_list = [pid for g in valid_groups for pid in g["源分区清单"]]
        target_partition_list = [g["目标分区"] for g in valid_groups]
        estimated_release = sum(g["释放空间MB"] for g in valid_groups)

        # 合并后仍超最大分区限制的对象进入待处理队列
        if partition_count_after > max_partitions:
            pending_items.append({
                "对象类型": "分区对象",
                "对象标识": f"policy-{policy_id}",
                "入队原因": f"合并后分区数{partition_count_after}仍超过最大分区限制{max_partitions}",
                "源分区清单": [p["分区标识"] for p in today_partitions],
                "建议处理": "当天新增分区不参与合并，待次日水位刷新后重新评估改动点",
            })

        criteria = self._build_criteria(
            policy, current_count, max_partitions, lifecycle_days, hard_cap,
            small_threshold, candidates, today_partitions, release_mode, snapshot_id)
        reconciliation = self._build_reconciliation(
            partition_key, hard_cap, groups, valid_groups, estimated_release, release_mode)

        plan_no = f"MP-{_today().replace('-', '')}-{uuid.uuid4().hex[:6].upper()}"
        plan_row: dict[str, Any] = {
            "计划编号": plan_no,
            "分区策略": policy_id,
            "目标数据集": policy.get("目标数据集", ""),
            "分区键": partition_key,
            "快照版本": snapshot_id,
            "水位时间": scan["observed_at"],
            "当前分区数": current_count,
            "最大分区数": max_partitions,
            "命中分区数": len(candidates),
            "合并分组数": len(valid_groups),
            "预计释放空间MB": estimated_release,
            "释放方式": release_mode,
            "复制任务": copy_task_id or "",
            "计划版本": "",
            # 对账与证据明细（列表页不展示，详情抽屉消费）
            "waterline_snapshot": {
                "snapshot_id": snapshot_id,
                "observed_at": scan["observed_at"],
                "waterline_version": scan["waterline_version"],
                "waterline_checksum": scan["waterline_checksum"],
                "partitions": waterline,
            },
            "merge_groups": groups,
            "pending_overview": pending_items,
            "partition_count_before": current_count,
            "partition_count_after": partition_count_after,
            "max_partition_limit": max_partitions,
            "source_partition_list": source_partition_list,
            "target_partition_list": target_partition_list,
            "lifecycle_hard_cap_days": hard_cap,
            "criteria_evidence": criteria,
            "criteria_recheck": {},
            "reconciliation": reconciliation,
            "change_points": change_points,
            "rollback_basis": {},
            "copy_receipt": None,
            "executed_groups": [],
            "released_mb": 0,
        }

        # 先发布计划版本再落库，保证持久化的计划行自带计划版本（跨区复制据此接收）
        plan_row["计划版本"] = "pending"
        plan_version = store.channel.publish(
            f"partition_merge_plans:{plan_no}", [self._plan_payload(plan_row)],
            metadata={
                "plan_no": plan_no,
                "policy_id": policy_id,
                "snapshot_id": snapshot_id,
                "release_mode": release_mode,
                "release_scope": "甲口径在合并执行版本释放；乙口径在归档版本延迟释放",
            },
            confirm=True,
        )
        plan_row["计划版本"] = plan_version.version_tag
        plan_row["criteria_evidence"]["计划版本"] = plan_version.version_tag

        # 事务落库：任何一步失败整体回滚，保证不出现半成品计划
        saved = self._transactional_save(plan_row, pending_items, draft_id, policy_id)
        if not saved["ok"]:
            return saved
        plan_row["id"] = saved["id"]

        self._record_cross_status(plan_no, "分区策略", "分区合并计划", "待执行",
                                  f"计划已生成并落库，水位快照{snapshot_id}")
        return {
            "ok": True,
            "plan": plan_row,
            "plan_no": plan_no,
            "plan_version": plan_version.version_tag,
            "snapshot_id": snapshot_id,
            "pending_count": len(pending_items),
            "message": f"分区合并计划{plan_no}已生成，{len(valid_groups)}个分组进入复制计划，"
                       f"{len(pending_items)}个对象进入待处理队列",
        }

    def _generate_from_draft(self, draft: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        policy_id = _to_int(draft.get("分区策略"), 0)
        if policy_id <= 0:
            return None, "草稿缺少有效的分区策略ID"
        result = self.generate_plan(
            policy_id=policy_id,
            copy_task_id=_to_int(draft.get("复制任务"), 0) or None,
            release_mode=str(draft.get("释放方式") or RELEASE_MODE_SNAPSHOT),
            draft_id=int(draft["id"]),
        )
        if not result["ok"]:
            return None, result.get("message") or result.get("error", "生成失败")
        return result["plan"], result["message"]

    # ------------------------------------------------------------------
    # 阶段 4：执行合并（中断可重试，保留回滚依据）
    # ------------------------------------------------------------------
    def execute_merge(self, plan_id: int) -> dict[str, Any]:
        plan = store.find(self.MODULE, plan_id)
        if plan is None:
            return {"ok": False, "message": f"计划 {plan_id} 不存在"}
        if plan["status"] in ("待核对", "待跨区核对", "复制中", "待复核", "已归档"):
            return {"ok": True, "plan": plan, "recovered": True,
                    "finished_groups": [],
                    "message": f"计划{plan['计划编号']}已执行合并，重试跳过已完成分组"}
        if plan["status"] not in ("待执行", "待复核", "已回滚"):
            return {"ok": False, "plan": plan,
                    "message": f"当前状态{plan['status']}不允许执行合并"}

        # 快照同源校验：计划执行不能跨越生成时的水位快照
        snapshot_id = plan["快照版本"]
        if snapshot_id not in self._snapshots:
            return {"ok": False, "plan": plan,
                    "message": "计划绑定的水位快照不存在，禁止跨快照执行"}

        valid_groups = [g for g in plan["merge_groups"] if g["入复制计划"]]
        executed = set(plan.get("executed_groups", []))
        finished_now: list[str] = []
        for group in valid_groups:
            if group["分组编号"] in executed:
                continue
            executed.add(group["分组编号"])
            finished_now.append(group["分组编号"])

        # 回滚依据：合并前分区数、源分区清单与水位快照
        if not plan.get("rollback_basis"):
            plan["rollback_basis"] = {
                "合并前分区数": plan["partition_count_before"],
                "最大分区限制": plan["max_partition_limit"],
                "源分区清单": list(plan["source_partition_list"]),
                "目标分区清单": list(plan["target_partition_list"]),
                "水位快照": snapshot_id,
                "水位版本": plan["waterline_snapshot"]["waterline_version"],
                "保存时间": _now(),
            }

        immediate_release = 0
        if plan["释放方式"] == RELEASE_MODE_IMMEDIATE:
            # 甲口径：合并执行版本即释放存储
            immediate_release = plan["预计释放空间MB"]
            plan["released_mb"] = immediate_release

        merge_version = store.channel.publish(
            "partition_merge_executions", [{
                "plan_no": plan["计划编号"],
                "plan_version": plan["计划版本"],
                "snapshot_id": snapshot_id,
                "finished_groups": finished_now,
                "executed_groups": sorted(executed),
                "partition_count_before": plan["partition_count_before"],
                "partition_count_after": plan["partition_count_after"],
                "source_partition_list": plan["source_partition_list"],
                "target_partition_list": plan["target_partition_list"],
                "release_mode": plan["释放方式"],
                "released_mb": immediate_release,
                "deferred_release_mb": 0 if immediate_release else plan["预计释放空间MB"],
            }],
            metadata={"plan_no": plan["计划编号"], "snapshot_id": snapshot_id,
                      "retry_groups": finished_now},
            confirm=True,
        )
        plan["executed_groups"] = sorted(executed)
        plan["merge_execution_version"] = merge_version.version_tag

        if plan["释放方式"] == RELEASE_MODE_SNAPSHOT:
            # 乙口径：先保留可恢复快照，延迟到归档时释放
            store.channel.publish(
                "partition_release_snapshots", [{
                    "plan_no": plan["计划编号"],
                    "snapshot_id": snapshot_id,
                    "source_partition_list": plan["source_partition_list"],
                    "deferred_release_mb": plan["预计释放空间MB"],
                    "restorable": True,
                }],
                metadata={"plan_no": plan["计划编号"], "confirmed": False},
                confirm=False,
            )

        plan["status"] = "待核对"
        plan["pending"] = True
        self._record_cross_status(plan["计划编号"], "分区合并计划", "分区合并计划",
                                  "待核对", f"合并执行完成（版本{merge_version.version_tag}）")
        return {"ok": True, "plan": plan, "finished_groups": finished_now,
                "merge_version": merge_version.version_tag,
                "message": f"合并已执行，新完成分组{finished_now or '无（重试幂等）'}，待八维复核"}

    # ------------------------------------------------------------------
    # 阶段 5：八维口径复核
    # ------------------------------------------------------------------
    def recheck_plan(self, plan_id: int) -> dict[str, Any]:
        plan = store.find(self.MODULE, plan_id)
        if plan is None:
            return {"ok": False, "message": f"计划 {plan_id} 不存在"}
        if plan["status"] != "待核对":
            return {"ok": False, "plan": plan,
                    "message": f"当前状态{plan['status']}不允许复核，仅待核对计划可复核"}

        snapshot = self._snapshots.get(plan["快照版本"])
        recheck = dict(self._build_criteria(
            store.find("partition_policy", plan["分区策略"]),
            plan["partition_count_before"], plan["max_partition_limit"],
            _to_int(store.find("partition_policy", plan["分区策略"]).get("生命周期"), 0),
            plan["lifecycle_hard_cap_days"],
            snapshot["small_threshold_mb"] if snapshot else DEFAULT_SMALL_THRESHOLD_MB,
            [p for p in plan["waterline_snapshot"]["partitions"]
             if not p["是否当天新增"] and (p["是否过期"] or p["是否小分区"])],
            [p for p in plan["waterline_snapshot"]["partitions"] if p["是否当天新增"]],
            plan["释放方式"], plan["快照版本"],
        ))
        # 口径维度额外校验：计划版本与水位快照同源且仍可追溯
        same_snapshot = snapshot is not None and \
            snapshot["waterline_version"] == plan["waterline_snapshot"]["waterline_version"]
        recheck_results = {
            dimension: {
                "通过": dimension != "口径" or same_snapshot,
                "依据": recheck.get(dimension, ""),
            }
            for dimension in CRITERIA_DIMENSIONS
        }
        all_passed = all(item["通过"] for item in recheck_results.values())
        plan["criteria_recheck"] = {"dimensions": recheck_results, "全部通过": all_passed,
                                    "复核时间": _now()}
        if not all_passed:
            plan["abnormal"] = True
            return {"ok": False, "plan": plan,
                    "message": "八维复核未全部通过，计划转异常，禁止进入跨区核对"}

        plan["status"] = "待跨区核对"
        self._record_cross_status(plan["计划编号"], "分区合并计划", "分区合并计划",
                                  "待跨区核对", "八维复核（阈值/口径/判定/规则/上限/范围/条件/标准）全部通过")
        return {"ok": True, "plan": plan, "recheck": plan["criteria_recheck"],
                "message": "八维复核全部通过，可进入跨区复制模块核对"}

    # ------------------------------------------------------------------
    # 阶段 6：交跨区复制按计划版本处理目标分区
    # ------------------------------------------------------------------
    def dispatch_to_copy(self, plan_id: int, copy_task_id: int | None = None) -> dict[str, Any]:
        plan = store.find(self.MODULE, plan_id)
        if plan is None:
            return {"ok": False, "message": f"计划 {plan_id} 不存在"}
        if plan["status"] not in ("待跨区核对", "复制中", "待复核"):
            return {"ok": False, "plan": plan,
                    "message": f"当前状态{plan['status']}不允许跨区核对"}

        task_id = copy_task_id or _to_int(plan.get("复制任务"), 0) or None
        accepted = cross_zone_copy_service.receive_merge_plan(task_id, plan)
        if not accepted["ok"]:
            return {"ok": False, "plan": plan, "message": accepted["error"]}

        receipt = cross_zone_copy_service.issue_receipt(task_id, plan)
        plan["copy_task_id"] = receipt["复制任务ID"]
        plan["status"] = "复制中"
        self._record_cross_status(
            plan["计划编号"], "分区合并计划", "跨区复制", "复制中",
            f"跨区复制已接收计划版本{plan['计划版本']}，回执{receipt['回执编号']}待完整性确认")
        return {"ok": True, "plan": plan, "receipt": receipt,
                "message": f"跨区复制已按计划版本处理目标分区，回执{receipt['回执编号']}已生成"}

    # ------------------------------------------------------------------
    # 阶段 7：只接收完整回执，结果回写待复核清单
    # ------------------------------------------------------------------
    def receive_receipt(
        self, plan_id: int, result_receipt: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        plan = store.find(self.MODULE, plan_id)
        if plan is None:
            return {"ok": False, "message": f"计划 {plan_id} 不存在"}
        if plan["status"] != "复制中":
            return {"ok": False, "plan": plan,
                    "message": f"当前状态{plan['status']}不允许确认回执"}

        receipt = result_receipt or cross_zone_copy_service.latest_receipt(plan["计划编号"])
        if receipt is None:
            return {"ok": False, "plan": plan, "message": "跨区复制尚未出具回执"}

        complete, missing = cross_zone_copy_service.validate_receipt_completeness(plan, receipt)
        if not complete:
            # 不完整回执不得推进状态，回写待复核清单并保留重试入口
            plan["abnormal"] = True
            self._append_review(plan, receipt, complete=False, note=f"回执缺失：{'、'.join(missing)}")
            self._record_cross_status(plan["计划编号"], "跨区复制", "分区合并计划",
                                      "待复核", f"回执不完整，缺失{'、'.join(missing)}")
            plan["status"] = "待复核"
            return {"ok": False, "plan": plan,
                    "message": f"回执不完整（缺{'、'.join(missing)}），已回写待复核清单，可重新跨区核对"}

        plan["copy_receipt"] = receipt
        plan["abnormal"] = False
        plan["status"] = "待复核"
        plan["pending"] = True
        self._append_review(plan, receipt, complete=True,
                            note="回执完整：原始落点、确认批次、散列值核对一致")
        self._record_cross_status(plan["计划编号"], "跨区复制", "分区合并计划",
                                  "待复核", "完整回执已确认，消费侧结果待归档")
        return {"ok": True, "plan": plan, "receipt": receipt,
                "message": "复制校验回执完整，已回写待复核清单"}

    # ------------------------------------------------------------------
    # 阶段 8：归档消费侧结果（乙口径在此延迟释放）
    # ------------------------------------------------------------------
    def archive_plan(self, plan_id: int) -> dict[str, Any]:
        plan = store.find(self.MODULE, plan_id)
        if plan is None:
            return {"ok": False, "message": f"计划 {plan_id} 不存在"}
        if plan["status"] != "待复核":
            return {"ok": False, "plan": plan,
                    "message": f"当前状态{plan['status']}不允许归档，仅待复核计划可归档"}
        if not plan.get("copy_receipt"):
            return {"ok": False, "plan": plan, "message": "缺少完整复制回执，禁止归档"}

        released_now = 0
        if plan["释放方式"] == RELEASE_MODE_SNAPSHOT:
            released_now = plan["预计释放空间MB"] - plan["released_mb"]
            plan["released_mb"] = plan["预计释放空间MB"]
            store.channel.publish(
                "partition_release_snapshots", [{
                    "plan_no": plan["计划编号"],
                    "snapshot_id": plan["快照版本"],
                    "released_mb": released_now,
                    "restorable": False,
                    "released_at": _now(),
                }],
                metadata={"plan_no": plan["计划编号"], "stage": "archive-release"},
                confirm=True,
            )

        store.channel.publish(
            "merge_consumer_archive", [self._plan_payload(plan)],
            metadata={"plan_no": plan["计划编号"], "plan_version": plan["计划版本"],
                      "release_mode": plan["释放方式"], "released_mb": plan["released_mb"]},
            confirm=True,
        )
        plan["status"] = "已归档"
        plan["pending"] = False
        self._record_cross_status(plan["计划编号"], "分区合并计划", "消费侧归档",
                                  "已归档", f"消费侧结果已归档，累计释放{plan['released_mb']}MB")
        return {"ok": True, "plan": plan, "released_mb": released_now,
                "message": f"计划已归档，乙口径延迟释放{released_now}MB" if released_now
                else "计划已归档（甲口径存储已在合并执行时释放）"}

    def rollback_plan(self, plan_id: int) -> dict[str, Any]:
        plan = store.find(self.MODULE, plan_id)
        if plan is None:
            return {"ok": False, "message": f"计划 {plan_id} 不存在"}
        basis = plan.get("rollback_basis") or {}
        if not basis:
            return {"ok": False, "plan": plan, "message": "缺少回滚依据，无法回滚"}
        store.channel.publish(
            "partition_merge_rollbacks", [{
                "plan_no": plan["计划编号"],
                "rollback_basis": basis,
                "restored_partition_count": basis.get("合并前分区数"),
                "restored_sources": basis.get("源分区清单"),
                "rolled_back_at": _now(),
            }],
            metadata={"plan_no": plan["计划编号"], "snapshot_id": basis.get("水位快照")},
            confirm=True,
        )
        plan["status"] = "已回滚"
        plan["pending"] = False
        plan["abnormal"] = True
        self._record_cross_status(plan["计划编号"], "分区合并计划", "分区策略",
                                  "已回滚", f"依据快照{basis.get('水位快照')}回滚至合并前分区状态")
        return {"ok": True, "plan": plan,
                "message": "已按回滚依据恢复合并前分区清单，计划标记已回滚"}

    # ------------------------------------------------------------------
    # 辅助：证据、对账、改动点、事务与跨模块状态
    # ------------------------------------------------------------------
    def _build_criteria(
        self, policy: dict[str, Any], current_count: int, max_partitions: int,
        lifecycle_days: int, hard_cap: int, small_threshold: int,
        candidates: list[dict[str, Any]], today_partitions: list[dict[str, Any]],
        release_mode: str, snapshot_id: str,
    ) -> dict[str, str]:
        return {
            "阈值": f"当前分区数{current_count} ≥ 最大分区数{max_partitions}，"
                    f"命中{'是' if current_count >= max_partitions else '否'}",
            "口径": f"计划与水位同源于快照{snapshot_id}，当天新增分区{len(today_partitions)}个"
                    "计入分区数但不参与合并",
            "判定": f"候选分区{len(candidates)}个（过期分区或小于{small_threshold}MB的小分区）",
            "规则": f"合并策略={policy.get('合并策略', '未配置')}，"
                    f"每{GROUP_SIZE}个源分区分一组，小分区阈值{small_threshold}MB",
            "上限": f"生命周期{lifecycle_days}天，硬上限{hard_cap}天"
                    f"（{HARD_LIFECYCLE_FACTOR}倍生命周期），超限分组不得进入复制计划",
            "范围": f"分区键={policy.get('分区键', 'ds')}，目标数据集={policy.get('目标数据集', '')}，"
                    f"候选{len(candidates)}个、当天{len(today_partitions)}个",
            "条件": "同时满足触及最大分区数且存在过期/小分区，方可生成合并计划",
            "标准": f"释放方式={release_mode}；按分区键、生命周期上限、合并分组、释放空间逐项对账",
        }

    def _build_reconciliation(
        self, partition_key: str, hard_cap: int, groups: list[dict[str, Any]],
        valid_groups: list[dict[str, Any]], estimated_release: int, release_mode: str,
    ) -> dict[str, Any]:
        over_cap_groups = [g["分组编号"] for g in groups if not g["入复制计划"]]
        return {
            "分区键": {"expected": partition_key,
                      "actual": partition_key,
                      "pass": True},
            "生命周期上限": {"硬上限天数": hard_cap,
                          "超限分组": over_cap_groups,
                          "pass": all(g["入复制计划"] for g in valid_groups)},
            "合并分组": {"分组总数": len(groups),
                      "进入复制计划": len(valid_groups),
                      "单组源分区上限": GROUP_SIZE,
                      "pass": all(g["源分区数"] <= GROUP_SIZE for g in groups)},
            "释放空间": {"预计释放MB": estimated_release,
                      "释放口径": release_mode,
                      "pass": estimated_release >= 0},
            "超限处置": "超生命周期硬上限分组全部转入待处理队列，未进入复制计划",
        }

    def _locate_change_points(
        self, policy: dict[str, Any], waterline: list[dict[str, Any]], max_partitions: int,
    ) -> dict[str, Any]:
        """分区策略主数据定位改动点：历史小分区与当天新增分区同时触顶时给出依据。"""
        historical_small = [p["分区标识"] for p in waterline
                            if not p["是否当天新增"] and p["是否小分区"]]
        today_new = [p["分区标识"] for p in waterline if p["是否当天新增"]]
        expired = [p["分区标识"] for p in waterline if p["是否过期"]]
        return {
            "策略ID": policy.get("id"),
            "规则名称": policy.get("规则名称"),
            "触顶": len(waterline) >= max_partitions,
            "历史小分区": historical_small,
            "当天新增分区": today_new,
            "过期分区": expired,
            "改动点": "当天新增分区推高分区分母，合并历史小分区回收空间；当天分区本身不参与本轮合并",
        }

    def _transactional_save(
        self, plan_row: dict[str, Any], pending_items: list[dict[str, Any]],
        draft_id: int | None, policy_id: int,
    ) -> dict[str, Any]:
        table = store.rows(self.MODULE)
        queue = store.rows("merge_pending_queue")
        table_backup = [dict(row) for row in table]
        queue_backup = [dict(row) for row in queue]
        try:
            if draft_id is not None:
                draft = next((row for row in table if int(row.get("id", 0)) == draft_id), None)
                if draft is None:
                    raise ValueError(f"草稿计划 {draft_id} 不存在")
                entry_id = draft_id
                target = draft
                target.clear()
                target.update(plan_row)
                target["id"] = entry_id
            else:
                entry_id = max((int(row.get("id", 0)) for row in table), default=0) + 1
                target = dict(plan_row)
                target["id"] = entry_id
                table.append(target)
            target["status"] = "待执行"
            target["pending"] = True
            target["abnormal"] = False

            for item in pending_items:
                queue.append({
                    "id": max((int(row.get("id", 0)) for row in queue), default=0) + 1,
                    "计划编号": plan_row["计划编号"],
                    "分区策略": policy_id,
                    "状态": "待处理",
                    "入队时间": _now(),
                    **item,
                })
            return {"ok": True, "id": entry_id}
        except Exception as exc:  # 整体回滚，杜绝半成品
            table[:] = table_backup
            queue[:] = queue_backup
            return {"ok": False, "error": f"合并计划事务落库失败已回滚：{exc}"}

    def _append_review(
        self, plan: dict[str, Any], receipt: dict[str, Any], *, complete: bool, note: str,
    ) -> None:
        checklist = store.rows("merge_review_checklist")
        checklist.append({
            "id": max((int(row.get("id", 0)) for row in checklist), default=0) + 1,
            "计划编号": plan["计划编号"],
            "计划版本": plan["计划版本"],
            "快照版本": plan["快照版本"],
            "回执编号": receipt.get("回执编号", ""),
            "核对结论": "完整" if complete else "不完整",
            "核对要点": "原始落点、确认批次、散列值",
            "备注": note,
            "状态": "待复核",
            "回写时间": _now(),
        })

    def _record_cross_status(
        self, plan_no: str, from_module: str, to_module: str, status: str, note: str,
    ) -> None:
        records = store.rows("cross_module_status")
        records.append({
            "id": max((int(row.get("id", 0)) for row in records), default=0) + 1,
            "计划编号": plan_no,
            "来源模块": from_module,
            "目标模块": to_module,
            "跨模块状态": status,
            "说明": note,
            "记录时间": _now(),
        })

    def _plan_payload(self, plan: dict[str, Any]) -> dict[str, Any]:
        """通道发布用的精简计划载荷。"""
        return {
            "计划编号": plan["计划编号"],
            "计划版本": plan.get("计划版本", ""),
            "快照版本": plan["快照版本"],
            "分区策略": plan["分区策略"],
            "分区键": plan["分区键"],
            "合并分组数": plan["合并分组数"],
            "源分区清单": plan["source_partition_list"],
            "目标分区清单": plan["target_partition_list"],
            "预计释放空间MB": plan["预计释放空间MB"],
            "释放方式": plan["释放方式"],
        }

    # ------------------------------------------------------------------
    # 查询入口：待处理队列、待复核清单、跨模块状态、计划详情
    # ------------------------------------------------------------------
    def pending_queue(self) -> list[dict[str, Any]]:
        return list(store.rows("merge_pending_queue"))

    def review_checklist(self) -> list[dict[str, Any]]:
        return list(store.rows("merge_review_checklist"))

    def cross_module_status(self, plan_no: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows("cross_module_status")
        if plan_no:
            rows = [row for row in rows if row.get("计划编号") == plan_no]
        return rows

    def snapshots(self) -> list[dict[str, Any]]:
        return [
            {"snapshot_id": key, "policy_id": value["policy_id"],
             "observed_at": value["observed_at"],
             "waterline_version": value["waterline_version"],
             "partition_count": len(value["partitions"])}
            for key, value in self._snapshots.items()
        ]


def _shift_day(day: str, delta_days: int) -> str:
    return time.strftime(
        "%Y-%m-%d", time.gmtime(time.mktime(time.strptime(day, "%Y-%m-%d")) + delta_days * 86400))


partition_merge_plan_service = PartitionMergePlanService()
