<script lang="ts">
  import { onMount } from 'svelte'
  import { fetchErrors, type ErrorEntry } from './api'
  import { formatTimestamp } from './format'

  let { onerror }: { onerror: (message: string) => void } = $props()

  let entries = $state<ErrorEntry[] | null>(null)

  async function load() {
    try {
      entries = await fetchErrors()
    } catch (e) {
      onerror(e instanceof Error ? e.message : String(e))
    }
  }

  onMount(() => {
    load()
    const timer = setInterval(load, 5000)
    return () => clearInterval(timer)
  })

  function levelClass(level: string): string {
    return level.startsWith('WARN') ? 'lvl-WARN' : 'lvl-ERROR'
  }
</script>

<main class="logs">
  <article class="detail">
    <header class="detail__header">
      <h2>Logs</h2>
      <span class="muted">Seneste WARN/ERROR på tværs af alle pods — persisteret, op til ~5 s forsinket</span>
    </header>

    {#if entries === null}
      <p class="muted">Henter…</p>
    {:else if entries.length === 0}
      <p class="muted">Ingen fejl registreret — som det skal være.</p>
    {:else}
      <table>
        <thead>
          <tr><th>Tidspunkt</th><th>Niveau</th><th>Pod</th><th>Logger</th><th>Besked</th></tr>
        </thead>
        <tbody>
          {#each entries as entry, i (i)}
            <tr>
              <td>{formatTimestamp(entry.timestamp)}</td>
              <td><span class="badge {levelClass(entry.level)}">{entry.level}</span></td>
              <td class="muted mono">{entry.pod}</td>
              <td class="muted mono">{entry.logger}</td>
              <td>{entry.message}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </article>
</main>
