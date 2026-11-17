<template>
  <section class="page" data-module="shard">
    <header class="page-head">
      <div>
        <h2>任务分片调度管理</h2>
        <p class="page-desc">维护任务分片的范围划分、节点分配与重试控制，分配节点、重试次数与失败范围作为一次执行结果整行保存。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记分片任务</button>
        <button class="btn" type="button" @click="exportRows">导出任务分片调度清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">分片任务总数</span>
        <strong class="stat-value">{{ total }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">执行中分片</span>
        <strong class="stat-value">{{ statusCount('执行中') }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已完成分片</span>
        <strong class="stat-value">{{ statusCount('已完成') }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">执行失败分片</span>
        <strong class="stat-value">{{ statusCount('执行失败') }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>分片编号</span>
        <input v-model="filters.keyword" placeholder="按分片编号检索" />
      </label>
      <label class="filter-item">
        <span>分片状态</span>
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
              v-if="column === '分片编号'"
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
          <td :colspan="columns.length + 1" class="empty-state">暂无任务分片调度数据，可先登记分片任务</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条任务分片调度记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <section v-if="detail" class="detail-panel" data-testid="shard-detail">
      <header class="detail-head">
        <h3>{{ detail.分片编号 }} 执行结果</h3>
        <button class="btn ghost" type="button" @click="detail = null">关闭</button>
      </header>
      <p v-if="detailError" class="error-text">{{ detailError }}</p>
      <dl class="detail-grid">
        <template v-for="field in detailFields" :key="field">
          <dt>{{ field }}</dt>
          <dd>{{ display(detail, field) }}</dd>
        </template>
      </dl>
      <div v-if="retryOptions.length" class="retry-compare">
        <h4>重试起点冲突</h4>
        <p class="muted">上次确认位置与最后一次接收位置不在同一采集偏移，两种重试做法如下：</p>
        <table class="data-table">
          <thead>
            <tr>
              <th>重试起点类型</th>
              <th>重试起点</th>
              <th>扫描范围</th>
              <th>重复扫描区间</th>
              <th>未覆盖区间</th>
              <th>尾部补扫清单</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="option in retryOptions" :key="String(option.重试起点类型)">
              <td>{{ option.重试起点类型 }}</td>
              <td>{{ option.重试起点 }}</td>
              <td>{{ option.扫描范围 }}</td>
              <td>{{ option.重复扫描区间 }}</td>
              <td>{{ option.未覆盖区间 }}</td>
              <td>{{ option.补扫清单 }}</td>
            </tr>
          </tbody>
        </table>
        <p class="muted">{{ retryNote }}</p>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/shard'
const columns = ["分片编号", "采集计划", "分片范围", "分配节点", "开始时间", "结束时间", "重试次数", "分片状态", "失败范围"]
const actions = ["分配节点", "重试分片", "标记完成", "标记失败"]
const statuses = ["待分配", "执行中", "已完成", "执行失败"]
const detailFields = [
  "分片编号", "采集计划", "分片范围", "分配节点", "重试次数", "分片状态",
  "失败范围", "上次确认位置", "最后一次接收位置", "重试起点类型", "重试起点",
  "扫描范围", "重复扫描区间", "未覆盖区间", "补扫清单",
]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref({ keyword: '', status: '' })
const detail = ref<Row | null>(null)
const detailError = ref('')
const retryOptions = ref<Row[]>([])
const retryNote = ref('')

function display(row: Row, column: string): string {
  const value = row[column]
  if (value === null || value === undefined || value === '') {
    return column === '失败范围' ? '无' : '—'
  }
  return String(value)
}

function statusCount(status: string): number {
  return rows.value.filter((row) => row['分片状态'] === status).length
}

function resetFilters() {
  filters.value = { keyword: '', status: '' }
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '分片任务登记入口尚未接入审批流'
}

async function loadRetryOptions(entryId: number) {
  retryOptions.value = []
  retryNote.value = ''
  try {
    const response = await request(`${ENDPOINT}/${entryId}/retry-options`)
    if (!response.ok) {
      return
    }
    const payload = await response.json()
    retryOptions.value = payload['两种路径'] ?? []
    retryNote.value = payload['说明'] ?? ''
  } catch {
    retryOptions.value = []
  }
}

async function openDetail(row: Row) {
  detailError.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}`)
    if (!response.ok) {
      throw new Error('分片任务详情读取失败')
    }
    detail.value = await response.json()
    await loadRetryOptions(Number(row.id))
  } catch (error) {
    detail.value = null
    detailError.value = error instanceof Error ? error.message : '分片任务详情读取失败'
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  const body: Record<string, unknown> = {
    action,
    revision: row.revision ?? undefined,
  }
  if (action === '分配节点') {
    const node = window.prompt('请输入分配节点编号，例如 CLUS-0001', String(row['分配节点'] ?? ''))
    if (node === null) {
      return
    }
    body['分配节点'] = node.trim()
    body['重试次数'] = row['重试次数'] ?? 0
    body['失败范围'] = row['失败范围'] ?? ''
  }
  if (action === '重试分片') {
    const basis = window.prompt(
      '请选择重试起点：输入「上次确认位置」或「最后一次接收位置」',
      String(row['重试起点类型'] ?? '上次确认位置'),
    )
    if (basis === null) {
      return
    }
    body['重试起点'] = basis.trim()
  }
  if (action === '标记失败') {
    const failed = window.prompt(
      '请输入失败范围，例如 260-320；多个区间用分号分隔',
      String(row['失败范围'] ?? ''),
    )
    if (failed === null) {
      return
    }
    body['失败范围'] = failed.trim()
  }
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify(body),
    })
    const payload = await response.json()
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.detail || payload.message || '任务分片调度动作未生效')
    }
    await reload()
    if (detail.value) {
      await openDetail({ id: row.id })
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '任务分片调度操作失败'
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
      throw new Error('分片任务列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '任务分片调度列表读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.detail-panel { margin-top: 16px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px 14px; }
.detail-head { display: flex; justify-content: space-between; align-items: center; }
.detail-head h3 { margin: 0; font-size: 15px; }
.detail-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px 12px; margin: 12px 0 0; }
.detail-grid dt { font-size: 12px; color: var(--muted); }
.detail-grid dd { margin: 0; font-size: 13px; }
.retry-compare { margin-top: 14px; }
.retry-compare h4 { margin: 0 0 4px; font-size: 14px; }
.muted { color: var(--muted); font-size: 12px; margin: 0 0 8px; }
.filter-item select { padding: 4px 6px; }
</style>
