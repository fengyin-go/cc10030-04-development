/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

export type MergeGroup = {
  分组编号: string
  源分区清单: string[]
  目标分区: string
  源分区数: number
  目标分区数: number
  合并前大小MB: number
  合并后大小MB: number
  释放空间MB: number
  最大年龄天: number
  超生命周期硬上限: boolean
  入复制计划: boolean
}

export type MergePlanRow = EntryRow & {
  计划编号: string
  分区策略: number
  目标数据集: string
  分区键: string
  快照版本: string
  水位时间: string
  当前分区数: number
  最大分区数: number
  命中分区数: number
  合并分组数: number
  预计释放空间MB: number
  释放方式: string
  复制任务: number | string
  计划版本: string
  merge_groups?: MergeGroup[]
  source_partition_list?: string[]
  target_partition_list?: string[]
  partition_count_before?: number
  partition_count_after?: number
  max_partition_limit?: number
  lifecycle_hard_cap_days?: number
  criteria_evidence?: Record<string, string>
  criteria_recheck?: {
    dimensions?: Record<string, { 通过: boolean; 依据: string }>
    全部通过?: boolean
    复核时间?: string
  }
  reconciliation?: Record<string, unknown>
  change_points?: Record<string, unknown>
  rollback_basis?: Record<string, unknown>
  copy_receipt?: Record<string, unknown> | null
  released_mb?: number
}

export type PendingItem = {
  id: number
  计划编号: string
  对象类型: string
  对象标识: string
  入队原因: string
  建议处理: string
  状态: string
  入队时间: string
}

export type ReviewItem = {
  id: number
  计划编号: string
  回执编号: string
  核对结论: string
  核对要点: string
  备注: string
  状态: string
  回写时间: string
}

