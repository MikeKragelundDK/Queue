// Træet hentes lazy — aldrig samlet.

export type QueueLevel = 'GLOBAL' | 'SUBSCRIPTION' | 'ORGANIZER' | 'EVENT' | 'MEMBERSYSTEM'
export type SessionState = 'INITIAL' | 'QUEUED' | 'OPEN' | 'CLOSED'

export interface QueueRow {
  uuid: string
  name: string
  level: QueueLevel
  parentName: string | null
  maxCapacity: number | null
  dirty: boolean
  externalReference: string | null
  archivedAt: string | null
  opensAt: string | null
  salesStartAt: string | null
  drawnAt: string | null
  childCount: number
  initial: number
  queued: number
  open: number
  closed: number
}

export interface SearchHit {
  queue: QueueRow
  ancestors: string[]
}

export type CloseReason = 'COMPLETED' | 'ABANDONED' | 'EXPIRED' | 'ARCHIVED' | 'ADMIN'

export interface SessionRow {
  uuid: string
  queueName: string
  state: SessionState
  sequenceNumber: number
  createdAt: string
  closedAt: string | null
  closeReason: CloseReason | null
}

export interface CreateQueueRequest {
  name: string
  level: QueueLevel
  parentUuid: string | null
  maxCapacity: number | null
  externalReference: string | null
}

/** ORGANIZER har to blad-typer. */
export function childLevelOf(parent: QueueLevel): QueueLevel | 'LEAF_CHOICE' | null {
  switch (parent) {
    case 'GLOBAL':
      return 'SUBSCRIPTION'
    case 'SUBSCRIPTION':
      return 'ORGANIZER'
    case 'ORGANIZER':
      return 'LEAF_CHOICE'
    default:
      return null
  }
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, init)
  if (!response.ok) {
    const body = await response.json().catch(() => null)
    throw new Error(body?.error ?? `${response.status} ${response.statusText}`)
  }
  if (response.status === 204) {
    return undefined as T
  }
  return response.json()
}

export function fetchRoots(includeArchived: boolean): Promise<QueueRow[]> {
  return request(`/api/queues/roots?includeArchived=${includeArchived}`)
}

export function fetchQueue(uuid: string): Promise<QueueRow> {
  return request(`/api/queues/${uuid}`)
}

export function fetchChildren(uuid: string, includeArchived: boolean): Promise<QueueRow[]> {
  return request(`/api/queues/${uuid}/children?includeArchived=${includeArchived}`)
}

export function fetchQueueSessions(uuid: string): Promise<SessionRow[]> {
  return request(`/api/queues/${uuid}/sessions`)
}

export function searchQueues(query: string, includeArchived: boolean): Promise<SearchHit[]> {
  return request(
    `/api/queues/search?query=${encodeURIComponent(query)}&includeArchived=${includeArchived}`,
  )
}

export function fetchRecentSessions(): Promise<SessionRow[]> {
  return request('/api/sessions')
}

export function createQueue(body: CreateQueueRequest): Promise<QueueRow> {
  return request('/api/queues', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  })
}

export function updateQueueCapacity(queueUuid: string, maxCapacity: number | null): Promise<QueueRow> {
  return request(`/api/queues/${queueUuid}/capacity`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ maxCapacity })
  })
}

/** Begge null rydder venteværelset. Afvises af serveren, når køen er lodtrukket. */
export function updateQueueSchedule(
  queueUuid: string,
  opensAt: string | null,
  salesStartAt: string | null
): Promise<QueueRow> {
  return request(`/api/queues/${queueUuid}/schedule`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ opensAt, salesStartAt })
  })
}

export function moveQueue(queueUuid: string, newParentUuid: string): Promise<QueueRow> {
  return request(`/api/queues/${queueUuid}/move`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ newParentUuid })
  })
}

export function enqueue(queueUuid: string): Promise<SessionRow> {
  return request(`/api/queues/${queueUuid}/enqueue`, { method: 'POST' })
}

export function archiveQueue(queueUuid: string): Promise<QueueRow> {
  return request(`/api/queues/${queueUuid}/archive`, { method: 'POST' })
}

export function unarchiveQueue(queueUuid: string): Promise<QueueRow> {
  return request(`/api/queues/${queueUuid}/unarchive`, { method: 'POST' })
}

export function deleteQueue(queueUuid: string): Promise<void> {
  return request(`/api/queues/${queueUuid}`, { method: 'DELETE' })
}

export function closeSession(sessionUuid: string): Promise<SessionRow> {
  return request(`/api/sessions/${sessionUuid}/close`, { method: 'POST' })
}

export interface TimeBucket {
  start: string
  count: number
}

export interface NamedValue {
  label: string
  value: number
  samples: number
}

export interface DurationStats {
  medianMinutes: number
  p95Minutes: number
  samples: number
}

export interface AbandonmentStats {
  completed: number
  abandoned: number
  expired: number
  ratePct: number
  byWaitBucket: NamedValue[]
}

export interface CapacityNode {
  name: string
  level: string
  maxCapacity: number
  minutesAtCap: number
  hits: number
}

export interface SubscriptionLoad {
  name: string
  /** INITIAL + QUEUED — begrænses ikke af maxCapacity. */
  waiting: number
  /** Det tal, maxCapacity er loft for. */
  open: number
  maxCapacity: number | null
}

export interface Stats {
  bucketHours: number
  arrivals: TimeBucket[]
  queueLength: TimeBucket[]
  opens: TimeBucket[]
  completions: TimeBucket[]
  waitTime: DurationStats
  waitByHourOfDay: NamedValue[]
  waitBySequenceBucket: NamedValue[]
  processingTime: DurationStats
  abandonment: AbandonmentStats
  capacityPressure: CapacityNode[]
  truncated: boolean
}

export interface LiveStats {
  waitingNow: number
  openNow: number
  arrivalsLastHour: number
  perSubscription: SubscriptionLoad[]
}

export function fetchStats(scopeUuid: string | null, hours: number): Promise<Stats> {
  const params = new URLSearchParams()
  if (scopeUuid) params.set('queue', scopeUuid)
  params.set('hours', String(hours))
  return request(`/api/stats?${params}`)
}

export function fetchLiveStats(scopeUuid: string | null): Promise<LiveStats> {
  return request(scopeUuid ? `/api/stats/live?queue=${scopeUuid}` : '/api/stats/live')
}

export interface ErrorEntry {
  timestamp: string
  level: string
  logger: string
  message: string
  pod: string
}

export function fetchErrors(): Promise<ErrorEntry[]> {
  return request('/api/errors')
}

// --- Dev-trafiksimulator ---

export type SimMode = 'FAST' | 'SLOW' | 'EXTREME'

export interface SimStatus {
  running: boolean
  mode: SimMode | null
  arrivalsPerSecond: number
  enqueued: number
  completed: number
  waitingNow: number
  openNow: number
  hotQueues: string[]
}

/** null = prod-build — panelet skjules. */
export async function fetchSimStatus(): Promise<SimStatus | null> {
  const response = await fetch('/api/dev/sim')
  if (response.status === 404) {
    return null
  }
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`)
  }
  return response.json()
}

export function startSim(mode: SimMode): Promise<SimStatus> {
  return request(`/api/dev/sim/start?mode=${mode}`, { method: 'POST' })
}

export function stopSim(): Promise<SimStatus> {
  return request('/api/dev/sim/stop', { method: 'POST' })
}
