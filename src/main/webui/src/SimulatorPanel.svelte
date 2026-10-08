<script lang="ts">
  import { onMount } from 'svelte'
  import { fetchSimStatus, startSim, stopSim, type SimMode, type SimStatus } from './api'

  // Vises kun, når /api/dev/sim svarer (dev-buildet).
  let { onchanged, onerror }: {
    onchanged: () => void
    onerror: (message: string) => void
  } = $props()

  let status = $state<SimStatus | null>(null)
  let lastCounters = ''

  async function refresh() {
    try {
      const next = await fetchSimStatus()
      status = next
      if (!next?.running) {
        lastCounters = ''
        return
      }
      // Genindlæs kun, når tallene har flyttet sig — onchanged() henter alle udfoldede grene.
      const counters = `${next.waitingNow}/${next.openNow}/${next.completed}`
      if (counters !== lastCounters) {
        lastCounters = counters
        onchanged()
      }
    } catch {
    }
  }

  async function start(mode: SimMode) {
    try {
      status = await startSim(mode)
      onchanged()
    } catch (e) {
      onerror(e instanceof Error ? e.message : String(e))
    }
  }

  async function stop() {
    try {
      status = await stopSim()
    } catch (e) {
      onerror(e instanceof Error ? e.message : String(e))
    }
  }

  onMount(() => {
    refresh()
    const timer = setInterval(refresh, 3000)
    return () => clearInterval(timer)
  })
</script>

{#if status}
  <div class="simulator">
    <span class="simulator__label">Simulering</span>
    <button
      class:active={status.running && status.mode === 'FAST'}
      onclick={() => start('FAST')}
      title="Komprimeret tempo: højt tryk og korte ekspeditioner — se køen bevæge sig med det samme"
    >Hurtig test</button>
    <button
      class:active={status.running && status.mode === 'SLOW'}
      onclick={() => start('SLOW')}
      title="Realistisk tempo: rolig ankomstrate og rigtige ekspeditionstider — brugbare ventetids-tal"
    >Langsom test</button>
    <button
      class:active={status.running && status.mode === 'EXTREME'}
      onclick={() => start('EXTREME')}
      title="Billetslip: 1000 ankomster/s med korte ekspeditioner. Den faktiske rate bliver det, maskinen og databasen kan følge med til"
    >Ekstremt hurtig test</button>
    {#if status.running}
      <button onclick={stop}>Stop</button>
      <span class="simulator__stats muted" title="Varme events: {status.hotQueues.join(', ')}">
        {status.waitingNow} i kø · {status.openNow} åbne · {status.completed} gennemført
      </span>
    {/if}
  </div>
{/if}
