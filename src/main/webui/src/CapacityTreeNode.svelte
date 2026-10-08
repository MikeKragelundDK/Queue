<script lang="ts">
  import { fetchChildren, type QueueRow } from './api'
  import CapacityTreeNode from './CapacityTreeNode.svelte'

  let { node, depth, version, onerror }: {
    node: QueueRow
    depth: number
    version: number
    onerror: (message: string) => void
  } = $props()

  let expanded = $state(false)
  let children = $state<QueueRow[] | null>(null)
  let loadedVersion = -1

  // Kun udfoldede grene genhentes, så antallet af kald følger det, operatøren har åbnet.
  $effect(() => {
    if (expanded && (children === null || loadedVersion !== version)) {
      loadedVersion = version
      fetchChildren(node.uuid, false)
        .then((c) => (children = c))
        .catch((e) => onerror(e instanceof Error ? e.message : String(e)))
    }
  })

  let atCap = $derived(node.maxCapacity !== null && node.open >= node.maxCapacity)
</script>

<div class="cap-node" style="--depth: {depth}">
  <div class="cap-node__row" class:at-cap={atCap}>
    {#if node.childCount > 0}
      <button
        class="cap-node__chevron"
        class:open={expanded}
        onclick={() => (expanded = !expanded)}
        aria-label={expanded ? 'Fold sammen' : `Fold ud (${node.childCount})`}
      >▸</button>
    {:else}
      <span class="cap-node__chevron-spacer"></span>
    {/if}

    <span class="level-square level-{node.level}" title={node.level}></span>
    <span class="cap-node__name" title="{node.name} ({node.level.toLowerCase()})">{node.name}</span>

    {#if atCap}
      <span class="chip-at-cap" title="Ingen flere kan lukkes ind her, før nogen bliver færdig">
        ⚠ på loftet
      </span>
    {/if}

    <span
      class="cap-node__value mono"
      title="{node.open} lukket ind af loftet {node.maxCapacity ?? '∞'} (hele undertræet)"
    >{node.open}/{node.maxCapacity ?? '∞'}</span>
  </div>

  {#if expanded}
    <div class="cap-node__children">
      {#if children === null}
        <p class="muted cap-node__loading">Henter…</p>
      {:else}
        {#each children as child (child.uuid)}
          <CapacityTreeNode node={child} depth={depth + 1} {version} {onerror} />
        {/each}
      {/if}
    </div>
  {/if}
</div>
