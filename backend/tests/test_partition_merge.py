"""分区合并计划链路测试：阈值判定、同源快照、分组对账、跨区回执与失败恢复。

覆盖需求约束：
- 计划与水位必须来自同一快照，执行不能跨越快照/生命周期上限；
- 历史小分区与当天新增分区同时触顶时由分区策略定位改动点；
- 超生命周期硬上限的分组不得进入复制计划，转入待处理队列；
- 事务落库不产生半成品，中断重试幂等，回滚保留依据；
- 跨区复制只接收完整回执（原始落点/确认批次/散列值）；
- 甲乙两种释放口径产生不同版本与释放时机。
"""
from __future__ import annotations

import pytest

from app.services.cross_zone_copy import cross_zone_copy_service
from app.services.partition_merge_plan import (
    DEFAULT_SMALL_THRESHOLD_MB,
    RELEASE_MODE_IMMEDIATE,
    RELEASE_MODE_SNAPSHOT,
    partition_merge_plan_service as merge_service,
)
from app.services.partition_policy import PartitionPolicyService
from app.store import store


def _partition(ident, age, size_mb, *, today=False, expired=False, records=1000):
    return {
        "分区标识": ident,
        "分区键值": ident.split("=", 1)[-1],
        "分区年龄天": age,
        "分区大小MB": size_mb,
        "记录数": records,
        "是否当天新增": today,
        "是否过期": expired,
    }


@pytest.fixture(autouse=True)
def reset_merge_tables():
    """每个用例从空的合并链路表开始，避免用例间计划编号与队列相互干扰。"""
    store._tables["partition_merge_plan"] = []
    store._tables["merge_pending_queue"] = []
    store._tables["merge_review_checklist"] = []
    store._tables["cross_module_status"] = []
    merge_service._snapshots.clear()
    yield


class TestWaterlineAndThreshold:
    """阶段1：分区规则与水位同源快照 + 最大分区数阈值判定。"""

    def test_scan_produces_same_snapshot_for_plan_and_waterline(self):
        scan = merge_service.scan_waterline(2)
        assert scan["ok"] is True
        assert scan["snapshot_id"].startswith("snap-")
        # 水位通道版本与快照登记本指向同一 checksum
        version = store.channel.latest_confirmed_of("partition_water_levels")
        assert version is not None
        assert version.checksum == scan["waterline_checksum"]
        assert merge_service._snapshots[scan["snapshot_id"]]["waterline_version"] == \
            scan["waterline_version"]

    def test_threshold_hit_with_historical_small_and_today_partitions(self):
        # 策略2：最大分区数8；构造 7 个历史小分区 + 1 个当天新增，恰好触顶
        partitions = [_partition(f"ds=2026-11-{d:02d}", 60 - d, 32 + d * 5,
                                 expired=(d < 3)) for d in range(1, 8)]
        partitions.append(_partition("ds=2026-11-18", 0, 512, today=True))
        result = merge_service.generate_plan(2, copy_task_id=1, partitions=partitions)
        assert result["ok"] is True
        plan = result["plan"]
        assert plan["当前分区数"] == 8
        assert plan["当前分区数"] >= plan["最大分区数"]
        # 当天新增分区计入分区分母，但不在任何源分区清单中
        assert "ds=2026-11-18" not in plan["source_partition_list"]
        assert plan["change_points"]["当天新增分区"] == ["ds=2026-11-18"]
        assert plan["change_points"]["触顶"] is True

    def test_no_plan_when_threshold_not_hit(self):
        # 只有 3 个分区，远低于最大分区数 8，不得产生半成品计划
        partitions = [_partition(f"ds=2026-11-{d:02d}", 30 - d, 40) for d in range(1, 4)]
        result = merge_service.generate_plan(2, partitions=partitions)
        assert result["ok"] is False
        assert result["threshold_hit"] is False
        assert store.rows("partition_merge_plan") == []

    def test_invalid_policy_rejected(self):
        assert merge_service.scan_waterline(999)["ok"] is False
        assert merge_service.generate_plan(999)["ok"] is False


class TestMergePlanGroupingAndReconciliation:
    """阶段2/3：分组、生命周期硬上限、逐项对账与待处理队列。"""

    def test_over_lifecycle_group_excluded_from_copy_plan(self):
        # 生命周期60天，硬上限120天；150天的过期分区所在分组必须被拦截
        partitions = [
            _partition("ds=old-150", 150, 20, expired=True),
            _partition("ds=old-70", 70, 24, expired=True),
            _partition("ds=old-66", 66, 28, expired=True),
            _partition("ds=old-62", 62, 32, expired=True),
            _partition("ds=mid-40", 40, 40),
            _partition("ds=mid-30", 30, 48),
            _partition("ds=mid-20", 20, 56),
            _partition("ds=today", 0, 512, today=True),
        ]
        result = merge_service.generate_plan(2, copy_task_id=1, partitions=partitions)
        assert result["ok"] is True
        plan = result["plan"]
        groups = plan["merge_groups"]
        over_cap = [g for g in groups if not g["入复制计划"]]
        valid = [g for g in groups if g["入复制计划"]]
        assert over_cap and over_cap[0]["最大年龄天"] > plan["lifecycle_hard_cap_days"]
        # 超限分组的源分区不得出现在复制计划清单里
        for group in over_cap:
            for pid in group["源分区清单"]:
                assert pid not in plan["source_partition_list"]
        # 待处理队列登记了超限分组
        queue = merge_service.pending_queue()
        assert any(item["对象类型"] == "合并分组" and "硬上限" in item["入队原因"]
                   for item in queue)
        # 对账证据：生命周期上限维度记录了超限分组
        recon = plan["reconciliation"]
        assert recon["生命周期上限"]["超限分组"] == [g["分组编号"] for g in over_cap]
        assert recon["合并分组"]["单组源分区上限"] == 4
        assert valid, "仍应有可进入复制计划的分组"

    def test_group_size_capped_and_reconciliation_covers_four_items(self):
        partitions = [_partition(f"ds=h-{d}", 50 - d, 20 + d) for d in range(7)]
        partitions.append(_partition("ds=today", 0, 600, today=True))
        result = merge_service.generate_plan(2, partitions=partitions)
        plan = result["plan"]
        assert all(g["源分区数"] <= 4 for g in plan["merge_groups"])
        for key in ("分区键", "生命周期上限", "合并分组", "释放空间"):
            assert key in plan["reconciliation"]
        # 合并前/后分区数、最大分区限制、源/目标清单全部保留
        assert plan["partition_count_before"] == 8
        assert plan["partition_count_after"] < 8
        assert plan["max_partition_limit"] == 8
        assert len(plan["source_partition_list"]) > 0
        assert len(plan["target_partition_list"]) == \
            len([g for g in plan["merge_groups"] if g["入复制计划"]])

    def test_exceeding_max_limit_after_merge_goes_to_pending_queue(self):
        # 当天分区多、可合并候选少，合并后仍超限时入队（当天分区不参与合并）
        partitions = [_partition("ds=old", 70, 24, expired=True)]
        partitions += [_partition(f"ds=t{d}", 0, 500, today=True) for d in range(1, 9)]
        result = merge_service.generate_plan(2, partitions=partitions)
        plan = result["plan"]
        queue = merge_service.pending_queue()
        assert any(item["对象类型"] == "分区对象" and "最大分区限制" in item["入队原因"]
                   for item in queue)
        assert plan["partition_count_after"] > plan["max_partition_limit"]


class TestFullLifecycle:
    """端到端：生成 → 执行 → 八维复核 → 跨区核对 → 回执 → 归档。"""

    def _make_plan(self, release_mode=RELEASE_MODE_SNAPSHOT):
        partitions = [_partition(f"ds=h-{d}", 55 - d, 24 + d * 6, expired=(d < 2))
                      for d in range(7)]
        partitions.append(_partition("ds=today", 0, 512, today=True))
        result = merge_service.generate_plan(
            2, copy_task_id=1, release_mode=release_mode, partitions=partitions)
        assert result["ok"] is True
        return int(result["plan"]["id"])

    def test_full_flow_snapshot_mode_defers_release_until_archive(self):
        plan_id = self._make_plan(RELEASE_MODE_SNAPSHOT)
        plan = merge_service.get_entry(plan_id)
        assert plan["status"] == "待执行"

        executed = merge_service.execute_merge(plan_id)
        assert executed["ok"] is True
        assert executed["plan"]["status"] == "待核对"
        # 乙口径执行阶段不释放，先保留可恢复快照
        assert executed["plan"]["released_mb"] == 0
        snapshots = store.channel.versions_of("partition_release_snapshots")
        assert snapshots and snapshots[-1].rows[0]["restorable"] is True

        recheck = merge_service.recheck_plan(plan_id)
        assert recheck["ok"] is True
        dimensions = recheck["plan"]["criteria_recheck"]["dimensions"]
        assert set(dimensions) == {"阈值", "口径", "判定", "规则", "上限", "范围", "条件", "标准"}
        assert all(item["通过"] for item in dimensions.values())
        assert recheck["plan"]["status"] == "待跨区核对"

        dispatched = merge_service.dispatch_to_copy(plan_id)
        assert dispatched["ok"] is True
        receipt = dispatched["receipt"]
        assert dispatched["plan"]["status"] == "复制中"

        confirmed = merge_service.receive_receipt(plan_id)
        assert confirmed["ok"] is True
        assert confirmed["plan"]["copy_receipt"]["回执编号"] == receipt["回执编号"]
        # 待复核清单已回写
        checklist = merge_service.review_checklist()
        assert checklist and checklist[-1]["核对结论"] == "完整"

        archived = merge_service.archive_plan(plan_id)
        assert archived["ok"] is True
        assert archived["plan"]["status"] == "已归档"
        # 乙口径在归档时才延迟释放
        assert archived["plan"]["released_mb"] == archived["plan"]["预计释放空间MB"]
        assert store.channel.latest_confirmed_of("merge_consumer_archive") is not None

    def test_immediate_mode_releases_storage_at_merge_execution(self):
        plan_id = self._make_plan(RELEASE_MODE_IMMEDIATE)
        executed = merge_service.execute_merge(plan_id)
        assert executed["plan"]["released_mb"] == executed["plan"]["预计释放空间MB"]
        merge_service.recheck_plan(plan_id)
        merge_service.dispatch_to_copy(plan_id)
        merge_service.receive_receipt(plan_id)
        archived = merge_service.archive_plan(plan_id)
        # 归档阶段没有二次释放
        assert archived["released_mb"] == 0

    def test_retry_is_idempotent_and_keeps_completed_groups(self):
        plan_id = self._make_plan()
        first = merge_service.execute_merge(plan_id)
        first_groups = set(first["plan"]["executed_groups"])
        second = merge_service.execute_merge(plan_id)
        assert second.get("recovered") is True
        assert set(second["plan"]["executed_groups"]) == first_groups
        # 幂等重试不重复执行分组：第二次没有新完成分组
        assert second["finished_groups"] == []
        assert len(second["plan"]["source_partition_list"]) > 0

    def test_rollback_restores_pre_merge_state_with_basis(self):
        plan_id = self._make_plan()
        plan = merge_service.get_entry(plan_id)
        before = plan["partition_count_before"]
        merge_service.execute_merge(plan_id)
        rolled = merge_service.rollback_plan(plan_id)
        assert rolled["ok"] is True
        assert rolled["plan"]["status"] == "已回滚"
        basis = rolled["plan"]["rollback_basis"]
        assert basis["合并前分区数"] == before
        assert basis["源分区清单"] == plan["source_partition_list"]
        assert basis["水位快照"] == plan["快照版本"]
        rollback_version = store.channel.latest_confirmed_of("partition_merge_rollbacks")
        assert rollback_version.rows[0]["restored_partition_count"] == before


class TestCrossZoneReceipt:
    """跨区复制：只接收完整回执，不完整回执退回待复核。"""

    def test_receipt_carries_landing_batch_and_hash_evidence(self):
        partitions = [_partition(f"ds=h-{d}", 40 - d, 30 + d * 7) for d in range(7)]
        partitions.append(_partition("ds=today", 0, 512, today=True))
        result = merge_service.generate_plan(2, copy_task_id=1, partitions=partitions)
        plan = result["plan"]
        merge_service.execute_merge(int(plan["id"]))
        merge_service.recheck_plan(int(plan["id"]))
        dispatched = merge_service.dispatch_to_copy(int(plan["id"]))
        receipt = dispatched["receipt"]
        for field in ("原始落点核对", "确认批次核对", "散列值核对"):
            assert receipt[field] == "一致"
        assert receipt["分区明细"], "回执必须携带数据明细"
        complete, missing = cross_zone_copy_service.validate_receipt_completeness(
            plan, receipt)
        assert complete and missing == []

    def test_incomplete_receipt_is_rejected_and_reviewed(self):
        partitions = [_partition(f"ds=h-{d}", 40 - d, 30 + d * 7) for d in range(7)]
        partitions.append(_partition("ds=today", 0, 512, today=True))
        result = merge_service.generate_plan(2, copy_task_id=1, partitions=partitions)
        plan_id = int(result["plan"]["id"])
        merge_service.execute_merge(plan_id)
        merge_service.recheck_plan(plan_id)
        merge_service.dispatch_to_copy(plan_id)

        bad_receipt = dict(cross_zone_copy_service.latest_receipt(
            result["plan"]["计划编号"]))
        bad_receipt.pop("校验和")
        bad_receipt["散列值核对"] = "不一致"
        confirmed = merge_service.receive_receipt(plan_id, result_receipt=bad_receipt)
        assert confirmed["ok"] is False
        plan = merge_service.get_entry(plan_id)
        assert plan["status"] == "待复核" and plan["abnormal"] is True
        checklist = merge_service.review_checklist()
        assert checklist[-1]["核对结论"] == "不完整"
        # 重新跨区核对可出具完整回执并恢复流程
        merge_service.dispatch_to_copy(plan_id)
        again = merge_service.receive_receipt(plan_id)
        assert again["ok"] is True
        assert again["plan"]["abnormal"] is False

    def test_copy_refuses_plan_without_version_or_snapshot(self):
        assert cross_zone_copy_service.receive_merge_plan(
            1, {"计划编号": "X", "status": "待跨区核对"})["ok"] is False


class TestCrossModuleAndPolicyMasterData:
    """跨模块状态、分区策略改动点与事务一致性。"""

    def test_cross_module_status_records_every_handoff(self):
        partitions = [_partition(f"ds=h-{d}", 40 - d, 30 + d * 7) for d in range(7)]
        partitions.append(_partition("ds=today", 0, 512, today=True))
        result = merge_service.generate_plan(2, copy_task_id=1, partitions=partitions)
        plan_no = result["plan_no"]
        plan_id = int(result["plan"]["id"])
        merge_service.execute_merge(plan_id)
        merge_service.recheck_plan(plan_id)
        merge_service.dispatch_to_copy(plan_id)
        merge_service.receive_receipt(plan_id)
        merge_service.archive_plan(plan_id)
        statuses = [row["跨模块状态"] for row in merge_service.cross_module_status(plan_no)]
        for expected in ("待执行", "待核对", "待跨区核对", "复制中", "待复核", "已归档"):
            assert expected in statuses

    def test_policy_locates_change_points_from_waterline_snapshot(self):
        partitions = [_partition("ds=small-1", 40, 20),
                      _partition("ds=small-2", 30, 24),
                      _partition("ds=today", 0, 512, today=True),
                      _partition("ds=exp", 80, 18, expired=True)]
        merge_service.scan_waterline(2, partitions=partitions)
        points = PartitionPolicyService().locate_change_points(2)
        assert points["ok"] is True
        assert points["是否触顶"] is False  # 4 < 8
        assert points["当天新增分区"] == ["ds=today"]
        assert set(points["历史小分区"]) == {"ds=small-1", "ds=small-2", "ds=exp"}
        assert points["过期分区"] == ["ds=exp"]

    def test_plan_cannot_execute_against_missing_snapshot(self):
        partitions = [_partition(f"ds=h-{d}", 40 - d, 30 + d * 7) for d in range(7)]
        partitions.append(_partition("ds=today", 0, 512, today=True))
        result = merge_service.generate_plan(2, partitions=partitions)
        plan_id = int(result["plan"]["id"])
        # 快照登记本丢失后，禁止跨快照执行
        merge_service._snapshots.clear()
        executed = merge_service.execute_merge(plan_id)
        assert executed["ok"] is False
        assert "快照" in executed["message"]

    def test_transactional_save_leaves_no_half_finished_plan(self):
        before = len(store.rows("partition_merge_plan"))
        queue_before = len(store.rows("merge_pending_queue"))
        # 未触顶场景不会写入任何计划或队列记录
        result = merge_service.generate_plan(
            2, partitions=[_partition("ds=only", 10, 40),
                           _partition("ds=today", 0, 512, today=True)])
        assert result["ok"] is False
        assert len(store.rows("partition_merge_plan")) == before
        assert len(store.rows("merge_pending_queue")) == queue_before
