<script lang="ts">
  import { onMount } from 'svelte'
  import { fetchRecentSessions, type SessionRow } from './api'
  import SessionTable from './SessionTable.svelte'

  let { onchanged, onerror }: {
    onchanged: () => void
    onerror: (message: string) => void
  } = $props()

  let sessions = $state<SessionRow[]>([])

  async function load() {
    try {
      sessions = await fetchRecentSessions()
    } catch (e) {
      onerror(e instanceof Error ? e.message : String(e))
    }
  }

  onMount(() => {
    load()
    const timer = setInterval(load, 3000)
    return () => clearInterval(timer)
  })
</script>

<article class="detail">
  <header class="detail__header">
    <h2>Seneste aktivitet</h2>
    <span class="muted">Vælg en kø i træet for detaljer og handlinger</span>
  </header>
  <SessionTable {sessions} showQueue={true} onchanged={() => { load(); onchanged() }} {onerror} />
</article>
