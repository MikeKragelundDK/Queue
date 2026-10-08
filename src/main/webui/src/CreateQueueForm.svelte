<script lang="ts">
  import { childLevelOf, createQueue, type QueueLevel, type QueueRow } from './api'

  let { parent, oncreated, onerror }: {
    parent: QueueRow | null
    oncreated: () => void
    onerror: (message: string) => void
  } = $props()

  let name = $state('')
  let leafType = $state<QueueLevel>('EVENT')
  let maxCapacity = $state('')
  let externalReference = $state('')

  // Niveauet afledes af forælderen — man vælger aldrig et frit niveau.
  const derived = $derived(parent === null ? 'GLOBAL' : childLevelOf(parent.level))

  async function submit(event: SubmitEvent) {
    event.preventDefault()
    const level: QueueLevel = derived === 'LEAF_CHOICE' ? leafType : (derived as QueueLevel)
    try {
      await createQueue({
        name,
        level,
        parentUuid: parent?.uuid ?? null,
        maxCapacity: maxCapacity === '' ? null : Number(maxCapacity),
        externalReference: externalReference === '' ? null : externalReference
      })
      name = ''
      maxCapacity = ''
      externalReference = ''
      oncreated()
    } catch (e) {
      onerror(e instanceof Error ? e.message : String(e))
    }
  }
</script>

<form class="create-form" onsubmit={submit}>
  <label>Navn <input bind:value={name} required /></label>
  {#if derived === 'LEAF_CHOICE'}
    <label>Type
      <select bind:value={leafType}>
        <option value="EVENT">EVENT</option>
        <option value="MEMBERSYSTEM">MEMBERSYSTEM</option>
      </select>
    </label>
  {:else}
    <label>Niveau <input value={derived} disabled /></label>
  {/if}
  <label>Loft (tom = intet loft) <input bind:value={maxCapacity} type="number" min="0" /></label>
  <label>Ekstern reference <input bind:value={externalReference} /></label>
  <button type="submit">Opret</button>
</form>
