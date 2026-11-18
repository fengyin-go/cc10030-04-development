<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">数据湖入湖治理中心</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向数据湖的多区数据入湖、格式管理、分层治理和生命周期维护的数据湖建设平台</span>
        <span class="head-user">当前值班：数据工程组</span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [{ label: "运营概览", path: "/" }, { label: "湖区分层", path: "/lake_zone" }, { label: "入湖管道", path: "/ingest_pipeline" }, { label: "Schema注册", path: "/schema_registry" }, { label: "分区策略", path: "/partition_policy" }, { label: "分区合并计划", path: "/partition_merge_plan" }, { label: "存储格式", path: "/file_format" }, { label: "留存策略", path: "/data_retention" }, { label: "元数据目录", path: "/metadata_catalog" }, { label: "质量门禁", path: "/quality_gate" }, { label: "湖内去重", path: "/dedup_lake" }, { label: "变更追踪", path: "/lineage_track" }, { label: "操作审计", path: "/audit_journal" }, { label: "快照管理", path: "/snapshot_mgr" }, { label: "过期清理", path: "/ttl_sweeper" }, { label: "冷热分层", path: "/cold_warm_tier" }, { label: "跨区复制", path: "/cross_zone_copy" }, { label: "模式演进", path: "/schema_evolve" }, { label: "入湖监控", path: "/ingest_monitor" }, { label: "灾备策略", path: "/backup_policy" }, { label: "数据字典", path: "/data_dictionary" }, { label: "访问管控", path: "/access_control" }]
</script>
