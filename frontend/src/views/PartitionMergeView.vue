<template>
  <section class="page" data-module="partition_merge_plan">
    <header class="page-head">
      <div>
        <h2>分区合并计划</h2>
        <p class="page-desc">
          读取分区规则与同源水位，按最大分区数阈值生成分组合并计划，驱动跨区复制按计划版本处理目标分区并核对回执
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="open-generate" type="button" @click="openGenerate">生成合并计划</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">计划总数</span>
        <strong class="stat-value">{{ rows.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">待处理队列</span>
        <strong class="stat-value">{{ pendingItems.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已归档数</span>
        <strong class="stat-value">{{ archivedCount }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">预计/累计释放(MB)</span>
        <strong class="stat-value">{{ totalRelease }}</strong>
      </article>
    </div>

    <p v-if="errorMessage" class="error-text" data-testid="merge-error">{{ errorMessage }}</p>
    <p v-if="infoMessage" class="info-text">{{ infoMessage }}</p>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>阶段操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ displayValue(row, column) }}</td>
          <td>
            <span :class="['status-pill', row.abnormal ? 'abnormal' : '']">{{ row.status }}</span>
          </td>
          <td class="row-actions">
            <button class="link" type="button" data-testid="detail" @click="openDetail(row)">详情核对</button>
            <template v-for="action in availableActions(row)" :key="action">
              <button
                class="link"
                type="button"
                :data-testid="`action-${row.id}-${action}`"
                @click="runStage(action, row)"
              >
                {{ action }}
              </button>
            </template>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无分区合并计划，点击右上角「生成合并计划」
          </td>
        </tr>
      </tbody>
    </table>

    <div class="aux-grid">
      <section class="aux-panel">
        <h3>待处理队列（超上限/超限制对象）</h3>
        <table class="data-table compact">
          <thead>
            <tr><th>计划编号</th><th>对象类型</th><th>入队原因</th><th>建议处理</th></tr>
          </thead>
          <tbody>
            <tr v-for="item in pendingItems" :key="item.id">
              <td>{{ item.计划编号 }}</td>
              <td>{{ item.对象类型 }}</td>
              <td>{{ item.入队原因 }}</td>
              <td>{{ item.建议处理 }}</td>
            </tr>
            <tr v-if="!pendingItems.length"><td colspan="4" class="empty-state">暂无超限对象</td></tr>
          </tbody>
        </table>
      </section>

      <section class="aux-panel">
        <h3>待复核清单（跨区复制回执回写）</h3>
        <table class="data-table compact">
          <thead>
            <tr><th>计划编号</th><th>回执编号</th><th>核对结论</th><th>备注</th></tr>
          </thead>
          <tbody>
            <tr v-for="item in reviewItems" :key="item.id">
              <td>{{ item.计划编号 }}</td>
              <td>{{ item.回执编号 }}</td>
              <td>{{ item.核对结论 }}</td>
              <td>{{ item.备注 }}</td>
            </tr>
            <tr v-if="!reviewItems.length"><td colspan="4" class="empty-state">暂无回执记录</td></tr>
          </tbody>
        </table>
      </section>
    </div>

    <!-- 生成计划弹窗 -->
    <div v-if="showGenerate" class="modal-backdrop" data-testid="generate-modal">
      <form class="modal" @submit.prevent="submitGenerate">
        <h3>生成分区合并计划</h3>
        <label class="create-field">
          <span>分区策略ID（最大分区数阈值来源）</span>
          <input v-model.number="generateForm.policy_id" data-testid="gen-policy" type="number" min="1" required />
        </label>
        <label class="create-field">
          <span>跨区复制任务ID（可空）</span>
          <input v-model.number="generateForm.copy_task_id" data-testid="gen-copy" type="number" min="1" />
        </label>
        <label class="create-field">
          <span>释放方式（甲乙两种口径）</span>
          <select v-model="generateForm.release_mode" data-testid="gen-mode">
            <option value="甲-立即释放">甲-到期后立即释放存储</option>
            <option value="乙-快照延迟释放">乙-先保留可恢复快照，归档时延迟释放</option>
          </select>
        </label>
        <p class="form-hint">计划与分区水位必须来自同一快照，超生命周期硬上限的分组不进入复制计划。</p>
        <div class="page-actions">
          <button class="btn" type="button" @click="showGenerate = false">取消</button>
          <button class="btn primary" data-testid="submit-generate" type="submit" :disabled="submitting">
            {{ submitting ? '生成中…' : '确认生成' }}
          </button>
        </div>
      </form>
    </div>

    <!-- 详情核对抽屉 -->
    <div v-if="detail" class="modal-backdrop" data-testid="detail-modal" @click.self="detail = null">
      <div class="modal detail-modal">
        <h3>计划 {{ detail.计划编号 }} 核对明细</h3>

        <section class="detail-block">
          <h4>阈值与对账证据</h4>
          <ul class="kv-list">
            <li v-for="(value, key) in detail.criteria_evidence" :key="String(key)">
              <strong>{{ key }}</strong>：{{ value }}
            </li>
          </ul>
        </section>

        <section class="detail-block">
          <h4>合并分组与预计释放空间</h4>
          <table class="data-table compact">
            <thead>
              <tr>
                <th>分组</th><th>源分区</th><th>目标分区</th><th>源数</th>
                <th>合并前MB</th><th>合并后MB</th><th>释放MB</th><th>是否入复制计划</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="group in detail.merge_groups" :key="group.分组编号"
                  :class="{ 'row-blocked': !group.入复制计划 }">
                <td>{{ group.分组编号 }}</td>
                <td>{{ group.源分区清单.join('、') }}</td>
                <td>{{ group.目标分区 }}</td>
                <td>{{ group.源分区数 }}</td>
                <td>{{ group.合并前大小MB }}</td>
                <td>{{ group.合并后大小MB }}</td>
                <td>{{ group.释放空间MB }}</td>
                <td>{{ group.入复制计划 ? '是' : '否（超生命周期硬上限）' }}</td>
              </tr>
            </tbody>
          </table>
          <ul class="kv-list">
            <li><strong>合并前分区数</strong>：{{ detail.partition_count_before }}</li>
            <li><strong>合并后分区数</strong>：{{ detail.partition_count_after }}</li>
            <li><strong>最大分区限制</strong>：{{ detail.max_partition_limit }}</li>
            <li><strong>生命周期硬上限(天)</strong>：{{ detail.lifecycle_hard_cap_days }}</li>
          </ul>
        </section>

        <section class="detail-block">
          <h4>八维复核结果</h4>
          <ul v-if="detail.criteria_recheck?.dimensions" class="kv-list">
            <li v-for="(item, key) in detail.criteria_recheck.dimensions" :key="String(key)">
              <strong>{{ key }}</strong>：{{ item.通过 ? '通过' : '未通过' }} — {{ item.依据 }}
            </li>
          </ul>
          <p v-else class="form-hint">尚未执行八维复核（阈值/口径/判定/规则/上限/范围/条件/标准）</p>
        </section>

        <section class="detail-block">
          <h4>跨区复制回执（原始落点/确认批次/散列值）</h4>
          <ul v-if="detail.copy_receipt" class="kv-list">
            <li><strong>回执编号</strong>：{{ receiptField('回执编号') }}</li>
            <li><strong>计划版本</strong>：{{ receiptField('计划版本') }}</li>
            <li><strong>快照版本</strong>：{{ receiptField('快照版本') }}</li>
            <li><strong>原始落点核对</strong>：{{ receiptField('原始落点核对') }}</li>
            <li><strong>确认批次核对</strong>：{{ receiptField('确认批次核对') }}</li>
            <li><strong>散列值核对</strong>：{{ receiptField('散列值核对') }}</li>
            <li><strong>校验和</strong>：{{ receiptField('校验和') }}</li>
          </ul>
          <p v-else class="form-hint">回执待跨区复制模块出具，系统只接收完整回执</p>
        </section>

        <section v-if="detail.rollback_basis && Object.keys(detail.rollback_basis).length" class="detail-block">
          <h4>回滚依据</h4>
          <ul class="kv-list">
            <li><strong>合并前分区数</strong>：{{ detail.rollback_basis['合并前分区数'] }}</li>
            <li><strong>水位快照</strong>：{{ detail.rollback_basis['水位快照'] }}</li>
            <li><strong>源分区清单</strong>：{{ rollbackSources }}</li>
          </ul>
        </section>

        <div class="page-actions">
          <button class="btn primary" type="button" @click="detail = null">关闭</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  generatePlan,
  pendingQueue,
  planDetail,
  reviewChecklist,
  stageAction,
} from '@/api/partition-merge'
import { listEntries } from '@/api/backend-service'
import { moduleMeta } from '@/data/modules'
import type { MergePlanRow, PendingItem, ReviewItem } from '@/data/types'

const meta = moduleMeta('partition_merge_plan')

const rows = ref<MergePlanRow[]>([])
const pendingItems = ref<PendingItem[]>([])
const reviewItems = ref<ReviewItem[]>([])
const errorMessage = ref('')
const infoMessage = ref('')
const showGenerate = ref(false)
const submitting = ref(false)
const detail = ref<MergePlanRow | null>(null)
const generateForm = ref({ policy_id: 2, copy_task_id: 1, release_mode: '乙-快照延迟释放' })

const STAGE_BY_ACTION: Record<string, 'execute' | 'recheck' | 'dispatch' | 'receipt' | 'archive' | 'rollback'> = {
  执行合并: 'execute',
  核对计划: 'recheck',
  跨区核对: 'dispatch',
  确认回执: 'receipt',
  归档计划: 'archive',
  回滚计划: 'rollback',
}
const ACTION_BY_STATUS: Record<string, string> = {
  待执行: '执行合并',
  待核对: '核对计划',
  待跨区核对: '跨区核对',
  复制中: '确认回执',
  待复核: '归档计划',
}

const archivedCount = computed(() => rows.value.filter((row) => row.status === '已归档').length)
const totalRelease = computed(() =>
  rows.value.reduce((sum, row) => sum + Number(row.预计释放空间MB ?? 0), 0))

function displayValue(row: MergePlanRow, column: string): string | number {
  const value = row[column]
  return value === '' || value === null || value === undefined ? '—' : String(value)
}

function availableActions(row: MergePlanRow): string[] {
  const actions: string[] = []
  const stage = ACTION_BY_STATUS[row.status]
  if (stage) actions.push(stage)
  if (['待执行', '待核对', '待跨区核对'].includes(row.status)) actions.push('回滚计划')
  return actions
}

const rollbackSources = computed(() => {
  const sources = detail.value?.rollback_basis?.['源分区清单']
  return Array.isArray(sources) ? sources.join('、') : '—'
})

function receiptField(key: string): string {
  const receipt = detail.value?.copy_receipt as Record<string, unknown> | null
  return receipt ? String(receipt[key] ?? '—') : '—'
}

function openGenerate() {
  errorMessage.value = ''
  showGenerate.value = true
}

async function submitGenerate() {
  submitting.value = true
  errorMessage.value = ''
  try {
    const result = await generatePlan({ ...generateForm.value })
    infoMessage.value = result.message
    showGenerate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '计划生成失败'
  } finally {
    submitting.value = false
  }
}

async function runStage(action: string, row: MergePlanRow) {
  errorMessage.value = ''
  const stage = STAGE_BY_ACTION[action]
  if (!stage) return
  try {
    const result = await stageAction(Number(row.id), stage)
    infoMessage.value = result.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : `${action}失败`
  }
}

async function openDetail(row: MergePlanRow) {
  errorMessage.value = ''
  try {
    detail.value = await planDetail(Number(row.id))
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '详情读取失败'
  }
}

async function reload() {
  try {
    const payload = await listEntries('partition_merge_plan', {})
    rows.value = payload.items as MergePlanRow[]
    const [queue, reviews] = await Promise.all([pendingQueue(), reviewChecklist()])
    pendingItems.value = queue
    reviewItems.value = reviews
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分区合并计划读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.aux-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  margin-top: 20px;
}
.aux-panel {
  background: var(--card-bg, #fff);
  border: 1px solid var(--border-color, #e5e7eb);
  border-radius: 8px;
  padding: 12px 16px;
}
.aux-panel h3 {
  margin: 4px 0 10px;
  font-size: 15px;
}
.data-table.compact {
  font-size: 12px;
}
.status-pill {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  background: #eef2ff;
  color: #3730a3;
  font-size: 12px;
}
.status-pill.abnormal {
  background: #fef2f2;
  color: #b91c1c;
}
.row-blocked {
  background: #fff7ed;
}
.detail-modal {
  max-width: 880px;
  max-height: 86vh;
  overflow-y: auto;
}
.detail-block {
  margin-bottom: 16px;
  text-align: left;
}
.detail-block h4 {
  margin: 0 0 8px;
  font-size: 14px;
  color: #1f2937;
}
.kv-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: grid;
  gap: 4px;
  font-size: 13px;
}
.kv-list strong {
  color: #374151;
}
.form-hint {
  font-size: 12px;
  color: #6b7280;
  margin: 6px 0;
}
.info-text {
  color: #047857;
  font-size: 13px;
}
.create-field select {
  width: 100%;
  padding: 6px 8px;
}
@media (max-width: 960px) {
  .aux-grid {
    grid-template-columns: 1fr;
  }
}
</style>
