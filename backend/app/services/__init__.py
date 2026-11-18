"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.lake_zone import LakeZoneService
from app.services.ingest_pipeline import IngestPipelineService
from app.services.schema_registry import SchemaRegistryService
from app.services.partition_policy import PartitionPolicyService
from app.services.partition_merge_plan import PartitionMergePlanService
from app.services.file_format import FileFormatService
from app.services.data_retention import DataRetentionService
from app.services.metadata_catalog import MetadataCatalogService
from app.services.quality_gate import QualityGateService
from app.services.dedup_lake import DedupLakeService
from app.services.lineage_track import LineageTrackService
from app.services.audit_journal import AuditJournalService
from app.services.snapshot_mgr import SnapshotMgrService
from app.services.ttl_sweeper import TtlSweeperService
from app.services.cold_warm_tier import ColdWarmTierService
from app.services.cross_zone_copy import CrossZoneCopyService
from app.services.schema_evolve import SchemaEvolveService
from app.services.ingest_monitor import IngestMonitorService
from app.services.backup_policy import BackupPolicyService
from app.services.data_dictionary import DataDictionaryService
from app.services.access_control import AccessControlService

SERVICES: dict[str, BaseModuleService] = {
    "lake_zone": LakeZoneService(),
    "ingest_pipeline": IngestPipelineService(),
    "schema_registry": SchemaRegistryService(),
    "partition_policy": PartitionPolicyService(),
    "partition_merge_plan": PartitionMergePlanService(),
    "file_format": FileFormatService(),
    "data_retention": DataRetentionService(),
    "metadata_catalog": MetadataCatalogService(),
    "quality_gate": QualityGateService(),
    "dedup_lake": DedupLakeService(),
    "lineage_track": LineageTrackService(),
    "audit_journal": AuditJournalService(),
    "snapshot_mgr": SnapshotMgrService(),
    "ttl_sweeper": TtlSweeperService(),
    "cold_warm_tier": ColdWarmTierService(),
    "cross_zone_copy": CrossZoneCopyService(),
    "schema_evolve": SchemaEvolveService(),
    "ingest_monitor": IngestMonitorService(),
    "backup_policy": BackupPolicyService(),
    "data_dictionary": DataDictionaryService(),
    "access_control": AccessControlService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
