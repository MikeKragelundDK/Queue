<script lang="ts">
  import { fetchQueue, fetchRoots, type QueueRow } from './api'
  import CapacityTreeNode from './CapacityTreeNode.svelte'

  // Samme lazy-kontrakt som Overview. Knuder uden loft vises med ∞, ellers får træet huller.
  let { scopeUuid, version, onerror }: {
    scopeUuid: string | null
    version: number
    onerror: (message: string) => void
  } = $props()

  let roots = $state<QueueRow[] | null>(null)

  $effect(() => {
    void version
    const uuid = scopeUuid
    const load = uuid === null ? fetchRoots(false) : fetchQueue(uuid).then((q) => [q])
    load
      .then((r) => (roots = r))
      .catch((e) => onerror(e instanceof Error ? e.message : String(e)))
  })
</script>

<article class="chart-card">
  <h2>Loft-status <span class="muted">(lukket ind / loft lige nu, hele undertræet)</span></h2>
  {#if roots === null}
    <p class="muted">Henter…</p>
  {:else if roots.length === 0}
    <p class="muted">Ingen køer i scope.</p>
  {:else}
    <div class="cap-tree">
      {#each roots as root (root.uuid)}
        <CapacityTreeNode node={root} depth={0} {version} {onerror} />
      {/each}
    </div>
  {/if}
</article>
