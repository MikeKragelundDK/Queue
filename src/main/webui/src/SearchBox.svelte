<script lang="ts">
  import { searchQueues, type SearchHit } from './api'

  // Følger træets "Vis arkiverede", så søgning og træ viser det samme.
  let { onpick, onerror, includeArchived = false }: {
    onpick: (hit: SearchHit) => void
    onerror: (message: string) => void
    includeArchived?: boolean
  } = $props()

  let query = $state('')
  let hits = $state<SearchHit[]>([])
  let searching = $state(false)
  let debounce: ReturnType<typeof setTimeout> | undefined

  function onInput() {
    clearTimeout(debounce)
    if (query.trim().length < 2) {
      hits = []
      return
    }
    debounce = setTimeout(async () => {
      searching = true
      try {
        hits = await searchQueues(query, includeArchived)
      } catch (e) {
        onerror(e instanceof Error ? e.message : String(e))
      } finally {
        searching = false
      }
    }, 250)
  }

  function pick(hit: SearchHit) {
    onpick(hit)
    query = ''
    hits = []
  }
</script>

<div class="search">
  <input
    type="search"
    placeholder="Søg kø eller reference…"
    bind:value={query}
    oninput={onInput}
    aria-label="Søg i kø-hierarkiet"
  />
  {#if searching}
    <p class="muted">Søger…</p>
  {:else if hits.length > 0}
    <ul class="search__results">
      {#each hits as hit (hit.queue.uuid)}
        <li>
          <button onclick={() => pick(hit)}>
            <span class="level-square level-{hit.queue.level}" title={hit.queue.level}></span>
            {hit.queue.name}
            {#if hit.queue.parentName}
              <span class="muted">— under {hit.queue.parentName}</span>
            {/if}
          </button>
        </li>
      {/each}
    </ul>
  {:else if query.trim().length >= 2}
    <p class="muted">Ingen resultater.</p>
  {/if}
</div>
