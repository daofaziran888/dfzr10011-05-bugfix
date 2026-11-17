<template>
  <section class="page" data-module="merge">
    <header class="page-head">
      <div>
        <h2>结果聚合归并管理</h2>
        <p class="page-desc">聚合任务读取分片执行结果，按重试起点生成待归并进度；下游同步采用修复分片字段丢失清单与分片执行结果。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记聚合任务</button>
        <button class="btn" type="button" @click="exportRows">导出结果聚合归并清单</button>
        <button class="btn" type="button" @click="refreshRepairIssues">刷新修复分片字段丢失清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>任务编号</span>
        <input v-model="filters.keyword" placeholder="按任务编号检索" />
      </label>
      <label class="filter-item">
        <span>任务状态</span>
        <select v-model="filters.status">
          <option value="">全部状态</option>
          <option v-for="status in statuses" :key="status" :value="status">{{ status }}</option>
        </select>
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">
            <button
              v-if="column === '任务编号'"
              class="link"
              type="button"
              @click="openDetail(row)"
            >
              {{ row[column] ?? '—' }}
            </button>
            <template v-else>{{ display(row, column) }}</template>
          </td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无结果聚合归并数据，可先登记聚合任务</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条结果聚合归并记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <section v-if="detail" class="detail-panel" data-testid="merge-detail">
      <header class="detail-head">
        <h3>{{ detail.任务编号 }} 聚合进度</h3>
        <button class="btn ghost" type="button" @click="detail = null">关闭</button>
      </header>
      <p v-if="detailError" class="error-text">{{ detailError }}</p>
      <div class="progress-summary">
        <span>分片总数：{{ detail.聚合进度?.分片总数 }}</span>
        <span>已归并：{{ detail.聚合进度?.已归并分片数 }}</span>
        <span>待归并：{{ detail.聚合进度?.待归并分片数 }}</span>
        <span>重复扫描区间：{{ detail.聚合进度?.重复扫描区间 || '无' }}</span>
        <span>未覆盖区间：{{ detail.聚合进度?.未覆盖区间 || '无' }}</span>
        <span>覆盖缺口：{{ detail.聚合进度?.覆盖缺口 || '无' }}</span>
      </div>
      <table class="data-table">
        <thead>
          <tr>
            <th>分片编号</th>
            <th>分片范围</th>
            <th>分配节点</th>
            <th>重试次数</th>
            <th>重试起点类型</th>
            <th>重试起点</th>
            <th>扫描范围</th>
            <th>失败范围</th>
            <th>重复扫描区间</th>
            <th>未覆盖区间</th>
            <th>补扫清单</th>
            <th>分片状态</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="entry in detail.聚合进度?.分片明细 ?? []" :key="String(entry.分片编号)">
            <td>{{ entry.分片编号 }}</td>
            <td>{{ entry.分片范围 }}</td>
            <td>{{ entry.分配节点 || '—' }}</td>
            <td>{{ entry.重试次数 }}</td>
            <td>{{ entry.重试起点类型 || '—' }}</td>
            <td>{{ entry.重试起点 }}</td>
            <td>{{ entry.扫描范围 || '—' }}</td>
            <td>{{ entry.失败范围 || '无' }}</td>
            <td>{{ entry.重复扫描区间 || '无' }}</td>
            <td>{{ entry.未覆盖区间 || '无' }}</td>
            <td>{{ entry.审批补扫清单 || '—' }}</td>
            <td>{{ entry.分片状态 }}</td>
          </tr>
        </tbody>
      </table>
      <div v-if="(detail.聚合进度?.待归并进度 ?? []).length" class="pending-plan">
        <h4>待归并进度（按重试起点）</h4>
        <ul>
          <li v-for="item in detail.聚合进度?.待归并进度 ?? []" :key="String(item.分片编号)">
            {{ item.分片编号 }} · {{ item.重试起点类型 || '首个起点' }} {{ item.重试起点 }} · 待归并区间 {{ item.待归并区间 }}
          </li>
        </ul>
      </div>
    </section>

    <section class="detail-panel" data-testid="repair-issues">
      <header class="detail-head">
        <h3>修复分片字段丢失待处理清单</h3>
        <span class="muted">共 {{ repairIssues.total }} 条</span>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>分片编号</th>
            <th>采集计划</th>
            <th>分片范围</th>
            <th>分片状态</th>
            <th>分配节点</th>
            <th>重试次数</th>
            <th>缺失字段</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="issue in repairIssues.items" :key="issue.分片编号">
            <td>{{ issue.分片编号 }}</td>
            <td>{{ issue.采集计划 }}</td>
            <td>{{ issue.分片范围 }}</td>
            <td>{{ issue.分片状态 }}</td>
            <td>{{ issue.分配节点 || '—' }}</td>
            <td>{{ issue.重试次数 }}</td>
            <td>{{ (issue.缺失字段 ?? []).join('、') }}</td>
          </tr>
          <tr v-if="!repairIssues.items.length">
            <td colspan="7" class="empty-state">当前分片执行结果字段齐全，没有待处理字段丢失</td>
          </tr>
        </tbody>
      </table>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type ProgressEntry = Record<string, string | number | null>
type Progress = {
  分片总数: number
  已归并分片数: number
  待归并分片数: number
  重复扫描区间: string
  未覆盖区间: string
  覆盖缺口: string
  待归并进度: ProgressEntry[]
  分片明细: ProgressEntry[]
}
type Detail = Row & { 聚合进度?: Progress }
type RepairIssue = { 分片编号: string; 采集计划: string; 分片范围: string; 分片状态: string; 分配节点: string; 重试次数: number; 缺失字段: string[] }

const ENDPOINT = '/api/merge'
const columns = ["任务编号", "采集计划", "分片总数", "已归并数", "归并策略", "输出路径", "数据总量", "任务状态", "失败范围"]
const actions = ["启动归并", "重试归并", "归档任务"]
const statuses = ["待归并", "归并中", "已完成", "归并失败"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref({ keyword: '', status: '' })
const detail = ref<Detail | null>(null)
const detailError = ref('')
const repairIssues = ref<{ total: number; items: RepairIssue[] }>({ total: 0, items: [] })

const stats = ref([
  { label: '聚合任务总数', value: 0 },
  { label: '归并中任务', value: 0 },
  { label: '已完成任务', value: 0 },
])

function display(row: Row, column: string): string {
  const value = row[column]
  if (value === null || value === undefined || value === '') {
    return column === '失败范围' ? '无' : '—'
  }
  return String(value)
}

function resetFilters() {
  filters.value = { keyword: '', status: '' }
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '聚合任务登记入口尚未接入审批流'
}

async function openDetail(row: Row) {
  detailError.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}`)
    if (!response.ok) {
      throw new Error('聚合任务详情读取失败')
    }
    detail.value = await response.json()
  } catch (error) {
    detail.value = null
    detailError.value = error instanceof Error ? error.message : '聚合任务详情读取失败'
  }
}

async function refreshRepairIssues() {
  try {
    const response = await request(`${ENDPOINT}/repair-issues`)
    if (!response.ok) {
      throw new Error('修复分片字段丢失清单读取失败')
    }
    const payload = await response.json()
    repairIssues.value = { total: payload.total ?? 0, items: payload.items ?? [] }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '修复分片字段丢失清单读取失败'
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    const payload = await response.json()
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.detail || payload.message || '结果聚合归并动作未生效')
    }
    await reload()
    await refreshRepairIssues()
    if (detail.value) {
      await openDetail({ id: row.id })
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结果聚合归并操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams()
  if (filters.value.keyword) {
    query.set('keyword', filters.value.keyword)
  }
  if (filters.value.status) {
    query.set('status', filters.value.status)
  }
  try {
    const response = await request(`${ENDPOINT}?${query.toString()}`)
    if (!response.ok) {
      throw new Error('聚合任务列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    stats.value = [
      { label: '聚合任务总数', value: total.value },
      { label: '归并中任务', value: rows.value.filter((item) => item['任务状态'] === '归并中').length },
      { label: '已完成任务', value: rows.value.filter((item) => item['任务状态'] === '已完成').length },
    ]
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结果聚合归并列表读取失败'
  }
}

onMounted(async () => {
  await reload()
  await refreshRepairIssues()
})
</script>

<style scoped>
.detail-panel { margin-top: 16px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px 14px; }
.detail-head { display: flex; justify-content: space-between; align-items: center; }
.detail-head h3 { margin: 0; font-size: 15px; }
.progress-summary { display: flex; flex-wrap: wrap; gap: 14px; margin: 10px 0; font-size: 13px; color: var(--muted); }
.pending-plan h4 { margin: 12px 0 4px; font-size: 14px; }
.pending-plan ul { margin: 0; padding-left: 18px; font-size: 13px; }
.muted { color: var(--muted); font-size: 12px; }
.filter-item select { padding: 4px 6px; }
</style>
