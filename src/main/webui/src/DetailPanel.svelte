<script lang="ts">
  import { archiveQueue, deleteQueue, enqueue, fetchQueue, fetchQueueSessions, unarchiveQueue, updateQueueCapacity, type QueueRow, type SessionRow } from './api'
  import { formatTimestamp } from './format'
  import CreateQueueForm from './CreateQueueForm.svelte'
  import SessionTable from './SessionTable.svelte'

  let { uuid, onchanged, ondeleted, onerror }: {
    uuid: string
    onchanged: () => void
    ondeleted: () => void
    onerror: (message: string) => void
  } = $props()

  let queue = $state<QueueRow | null>(null)
  let sessions = $state<SessionRow[]>([])
  let showCreateChild = $state(false)
  let editingCapacity = $state(false)
  let capacityInput = $state('')
  let savingCapacity = $state(false)

  async function load() {
    try {
      const [q, s] = await Promise.all([fetchQueue(uuid), fetchQueueSessions(uuid)])
      queue = q
      sessions = s
    } catch (e) {
      onerror(e instanceof Error ? e.message : String(e))
    }
  }

  // Kun den valgte knude polles — aldrig hele træet.
  $effect(() => {
    void uuid
    showCreateChild = false
    editingCapacity = false
    load()
    const timer = setInterval(load, 3000)
    return () => clearInterval(timer)
  })

  function editCapacity() {
    if (!queue) return
    capacityInput = queue.maxCapacity === null ? '' : String(queue.maxCapacity)
    editingCapacity = true
  }

  async function saveCapacity(event: SubmitEvent) {
    event.preventDefault()
    const maxCapacity = capacityInput === '' ? null : Number(capacityInput)
    savingCapacity = true
    try {
      queue = await updateQueueCapacity(uuid, maxCapacity)
      editingCapacity = false
      onchanged()
    } catch (e) {
      onerror(e instanceof Error ? e.message : String(e))
    } finally {
      savingCapacity = false
    }
  }

  async function enqueuePerson() {
    try {
      await enqueue(uuid)
      await load()
      onchanged()
    } catch (e) {
      onerror(e instanceof Error ? e.message : String(e))
    }
  }

  async function archive() {
    if (!queue) return
    if (!confirm(`Arkivér "${queue.name}"? Hele undertræet arkiveres, og åbne sessioner lukkes. Kan genaktiveres.`)) return
    try {
      await archiveQueue(uuid)
      await load()
      onchanged()
    } catch (e) {
      onerror(e instanceof Error ? e.message : String(e))
    }
  }

  async function unarchive() {
    try {
      await unarchiveQueue(uuid)
      await load()
      onchanged()
    } catch (e) {
      onerror(e instanceof Error ? e.message : String(e))
    }
  }

  async function remove() {
    if (!queue) return
    if (!confirm(`Slet "${queue.name}" PERMANENT? Sessioner og statistik-historik slettes uigenkaldeligt.`)) return
    try {
      await deleteQueue(uuid)
      ondeleted()
    } catch (e) {
      onerror(e instanceof Error ? e.message : String(e))
    }
  }
</script>

{#if queue}
  <article class="detail">
    <header class="detail__header">
      <span class="level-square level-{queue.level}" title={queue.level}></span>
      <h2>{queue.name}</h2>
      <span class="detail__meta">{queue.level}{#if queue.parentName}&nbsp;· under {queue.parentName}{/if}</span>
      {#if queue.archivedAt !== null}
        <span class="chip-archived">arkiveret {formatTimestamp(queue.archivedAt)}</span>
      {:else if queue.dirty}
        <span class="dirty"><span class="dirty-dot"></span> skal genberegnes</span>
      {:else}
        <span class="muted">ren</span>
      {/if}
    </header>

    <dl class="detail__facts">
      <div class="capacity-fact">
        <dt>Loft for samtidige indlukninger</dt>
        <dd>
          {#if editingCapacity}
            <form class="capacity-editor" onsubmit={saveCapacity}>
              <input
                bind:value={capacityInput}
                type="number"
                min="0"
                step="1"
                placeholder="∞"
                aria-label="Nyt kapacitetsloft; tomt felt betyder intet loft"
                disabled={savingCapacity}
              />
              <button type="submit" disabled={savingCapacity}>{savingCapacity ? 'Gemmer…' : 'Gem'}</button>
              <button type="button" class="secondary" onclick={() => (editingCapacity = false)} disabled={savingCapacity}>Annullér</button>
            </form>
            <span class="capacity-editor__hint">Tomt felt = intet loft</span>
          {:else}
            <span>{queue.maxCapacity ?? '∞'}</span>
            {#if queue.archivedAt === null}
              <button class="inline-action" onclick={editCapacity}>Redigér</button>
            {/if}
          {/if}
        </dd>
      </div>
      <div><dt>Underkøer</dt><dd>{queue.childCount}</dd></div>
      {#if queue.externalReference}
        <div><dt>Reference</dt><dd>{queue.externalReference}</dd></div>
      {/if}
    </dl>

    <div class="stat-row">
      <div class="stat-tile"><span class="stat-tile__value">{queue.initial}</span><span class="stat-tile__label"><span class="state-dot state-dot--INITIAL"></span>Initial</span></div>
      <div class="stat-tile"><span class="stat-tile__value">{queue.queued}</span><span class="stat-tile__label"><span class="state-dot state-dot--QUEUED"></span>I kø</span></div>
      <div class="stat-tile"><span class="stat-tile__value">{queue.open}</span><span class="stat-tile__label"><span class="state-dot state-dot--OPEN"></span>Lukket ind</span></div>
      <div class="stat-tile"><span class="stat-tile__value">{queue.closed}</span><span class="stat-tile__label"><span class="state-dot state-dot--CLOSED"></span>Færdige</span></div>
    </div>

    <div class="detail__actions">
      {#if queue.archivedAt === null}
        <button onclick={enqueuePerson}>Sæt person i kø</button>
        {#if queue.level !== 'EVENT' && queue.level !== 'MEMBERSYSTEM'}
          <button onclick={() => (showCreateChild = !showCreateChild)}>
            {showCreateChild ? 'Annullér' : '+ Opret underkø'}
          </button>
        {/if}
        <button class="danger" onclick={archive}>Arkivér</button>
      {:else}
        <button onclick={unarchive}>Genaktivér</button>
        <button class="danger" onclick={remove}>Slet permanent</button>
      {/if}
    </div>

    {#if showCreateChild}
      <CreateQueueForm
        parent={queue}
        oncreated={() => {
          showCreateChild = false
          load()
          onchanged()
        }}
        {onerror}
      />
    {/if}

    <h3>Sessioner <span class="muted">(seneste 100 i denne kø)</span></h3>
    <SessionTable {sessions} showQueue={false} onchanged={() => { load(); onchanged() }} {onerror} />
  </article>
{:else}
  <p class="muted">Henter…</p>
{/if}
