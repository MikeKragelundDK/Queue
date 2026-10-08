<script lang="ts">
  import { type QueueRow, type SearchHit } from './api'
  import DetailPanel from './DetailPanel.svelte'
  import Logs from './Logs.svelte'
  import RecentActivity from './RecentActivity.svelte'
  import SearchBox from './SearchBox.svelte'
  import SimulatorPanel from './SimulatorPanel.svelte'
  import Statistics from './Statistics.svelte'
  import Tree from './Tree.svelte'

  type Tab = 'overview' | 'stats' | 'logs'

  let tab = $state<Tab>('overview')
  let selected = $state<QueueRow | null>(null)
  let expandPath = $state<string[]>([])
  let treeVersion = $state(0)
  let showArchived = $state(false)
  let error = $state<string | null>(null)

  function refreshTree() {
    treeVersion += 1
  }

  function onSearchPick(hit: SearchHit) {
    expandPath = hit.ancestors
    selected = hit.queue
  }
</script>

<header class="app-header">
  <h1>Queue System v2</h1>
  <nav class="tabs" aria-label="Hovednavigation">
    <button class:active={tab === 'overview'} onclick={() => (tab = 'overview')}>Overview</button>
    <button class:active={tab === 'stats'} onclick={() => (tab = 'stats')}>Statistik</button>
    <button class:active={tab === 'logs'} onclick={() => (tab = 'logs')}>Logs</button>
  </nav>
  <SimulatorPanel onchanged={refreshTree} onerror={(m) => (error = m)} />
  {#if error}
    <button class="error" onclick={() => (error = null)} title="Klik for at fjerne">{error} ✕</button>
  {/if}
</header>

{#if tab === 'overview'}
  <div class="layout">
    <aside class="tree-panel">
      <SearchBox onpick={onSearchPick} onerror={(m) => (error = m)} includeArchived={showArchived} />
      <Tree
        version={treeVersion}
        includeArchived={showArchived}
        {expandPath}
        selectedUuid={selected?.uuid ?? null}
        onselect={(queue) => (selected = queue)}
        onerror={(m) => (error = m)}
      />
      <label class="tree-panel__toggle muted">
        <input type="checkbox" bind:checked={showArchived} onchange={refreshTree} />
        Vis arkiverede
      </label>
    </aside>

    <main class="detail-panel">
      {#if selected}
        <DetailPanel
          uuid={selected.uuid}
          onchanged={refreshTree}
          ondeleted={() => {
            selected = null
            refreshTree()
          }}
          onerror={(m) => (error = m)}
        />
      {:else}
        <RecentActivity onchanged={refreshTree} onerror={(m) => (error = m)} />
      {/if}
    </main>
  </div>
{:else if tab === 'stats'}
  <Statistics onerror={(m) => (error = m)} />
{:else}
  <Logs onerror={(m) => (error = m)} />
{/if}
