/** 分区合并计划阶段接口封装：对应后端 /api/partition-merge 专用路由。 */
import { request } from './client'
import type { MergePlanRow, PendingItem, ReviewItem } from '@/data/types'

const BASE = '/api/partition-merge'

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await request(`${BASE}${path}`, init)
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ?? `接口返回 ${response.status}`)
  }
  return (await response.json()) as T
}

export type GeneratePayload = {
  policy_id: number
  copy_task_id?: number
  release_mode: string
}

export type StageResult = {
  ok: boolean
  message: string
  plan: MergePlanRow
  receipt?: Record<string, unknown>
  receipt_no?: string
  released_mb?: number
  finished_groups?: string[]
  recovered?: boolean
}

export function scanWaterline(policyId: number, smallThresholdMb = 128) {
  return call<Record<string, unknown>>(`/waterline?policy_id=${policyId}&small_threshold_mb=${smallThresholdMb}`)
}

export function changePoints(policyId: number) {
  return call<Record<string, unknown>>(`/change-points?policy_id=${policyId}`)
}

export function generatePlan(payload: GeneratePayload) {
  return call<StageResult>('/plans', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export function planDetail(planId: number) {
  return call<MergePlanRow>(`/plans/${planId}`)
}

export function stageAction(
  planId: number,
  stage: 'execute' | 'recheck' | 'dispatch' | 'receipt' | 'archive' | 'rollback',
) {
  return call<StageResult>(`/plans/${planId}/${stage}`, { method: 'POST' })
}

export async function pendingQueue(): Promise<PendingItem[]> {
  const data = await call<{ items: PendingItem[] }>('/pending-queue')
  return data.items
}

export async function reviewChecklist(): Promise<ReviewItem[]> {
  const data = await call<{ items: ReviewItem[] }>('/review-checklist')
  return data.items
}

export async function crossStatus(planNo?: string) {
  const query = planNo ? `?plan_no=${encodeURIComponent(planNo)}` : ''
  const data = await call<{ items: Record<string, unknown>[] }>(`/cross-status${query}`)
  return data.items
}

export async function snapshots() {
  const data = await call<{ items: Record<string, unknown>[] }>('/snapshots')
  return data.items
}
