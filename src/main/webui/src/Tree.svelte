<script lang="ts">
  import { fetchRoots, type QueueRow } from './api'
  import TreeNode from './TreeNode.svelte'

  let { version, includeArchived, expandPath, selectedUuid, onselect, onerror }: {
    version: number
    includeArchived: boolean
    expandPath: string[]
    selectedUuid: string | null
    onselect: (queue: QueueRow) => void
    onerror: (message: string) => void
  } = $props()

  let roots = $state<QueueRow[] | null>(null)

  $effect(() => {
    void version
    fetchRoots(includeArchived)
      .then((r) => (roots = r))
      .catch((e) => onerror(e instanceof Error ? e.message : String(e)))
  })
</script>

<nav class="tree" aria-label="Kø-hierarki">
  {#if roots === null}
    <p class="muted">Henter…</p>
  {:else if roots.length === 0}
    <p class="muted">Ingen køer endnu — opret en global kø nedenfor.</p>
  {:else}
    {#each roots as root (root.uuid)}
      <TreeNode node={root} depth={0} {version} {includeArchived} {expandPath} {selectedUuid} {onselect} {onerror} />
    {/each}
  {/if}
</nav>
