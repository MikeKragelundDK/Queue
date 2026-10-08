<script lang="ts">
  import { fetchChildren, type QueueRow } from './api'
  import TreeNode from './TreeNode.svelte'

  let { node, depth, version, includeArchived, expandPath, selectedUuid, onselect, onerror }: {
    node: QueueRow
    depth: number
    version: number
    includeArchived: boolean
    expandPath: string[]
    selectedUuid: string | null
    onselect: (queue: QueueRow) => void
    onerror: (message: string) => void
  } = $props()

  let expanded = $state(false)
  let children = $state<QueueRow[] | null>(null)
  let loadedVersion = -1

  $effect(() => {
    if (expandPath.includes(node.uuid)) {
      expanded = true
    }
  })

  // Lazy load ved udfoldning. Knuden poller bevidst ikke sig selv: subtree-counts
  // pr. synlig knude pr. tick ville hamre databasen. Opdateres ved version-bump.
  $effect(() => {
    if (expanded && (children === null || loadedVersion !== version)) {
      loadedVersion = version
      fetchChildren(node.uuid, includeArchived)
        .then((c) => (children = c))
        .catch((e) => onerror(e instanceof Error ? e.message : String(e)))
    }
  })
</script>

<div class="tree-node" style="--depth: {depth}">
  <div class="tree-node__row" class:selected={selectedUuid === node.uuid} class:archived={node.archivedAt !== null}>
    {#if node.childCount > 0}
      <button
        class="tree-node__chevron"
        class:open={expanded}
        onclick={() => (expanded = !expanded)}
        aria-label={expanded ? 'Fold sammen' : `Fold ud (${node.childCount})`}
      >▸</button>
    {:else}
      <span class="tree-node__chevron-spacer"></span>
    {/if}

    <button class="tree-node__label" onclick={() => onselect(node)}>
      <span class="level-square level-{node.level}" title={node.level}></span>
      <span class="tree-node__name">{node.name}</span>
      {#if node.archivedAt !== null}
        <span class="chip-archived" title="Arkiveret">arkiveret</span>
      {/if}
      {#if node.dirty}
        <span class="dirty-dot" title="Skal genberegnes (dirty)"></span>
      {/if}
      <span class="tree-node__summary mono" title="{node.initial + node.queued} venter af maks {node.maxCapacity ?? '∞'} (hele undertræet)">
        {node.initial + node.queued}/{node.maxCapacity ?? '∞'}
      </span>
    </button>
  </div>

  {#if expanded}
    <div class="tree-node__children">
      {#if children === null}
        <p class="muted tree-node__loading">Henter…</p>
      {:else}
        {#each children as child (child.uuid)}
          <TreeNode node={child} depth={depth + 1} {version} {includeArchived} {expandPath} {selectedUuid} {onselect} {onerror} />
        {/each}
      {/if}
    </div>
  {/if}
</div>
