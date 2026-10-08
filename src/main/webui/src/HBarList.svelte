<script lang="ts">
  export interface HBar {
    label: string
    value: number
    display: string
    tooltip: string
  }

  let { title, subtitle = '', rows, emptyText = 'Ingen data i perioden.', wideValues = false }: {
    title: string
    subtitle?: string
    rows: HBar[]
    emptyText?: string
    wideValues?: boolean
  } = $props()

  const max = $derived(Math.max(1, ...rows.map((r) => r.value)))
</script>

<article class="chart-card">
  <h2>{title} {#if subtitle}<span class="muted">({subtitle})</span>{/if}</h2>
  {#if rows.length === 0}
    <p class="muted">{emptyText}</p>
  {:else}
    <div class="hbar-chart" class:hbar-chart--wide-values={wideValues}>
      {#each rows as row (row.label)}
        <div class="hbar-row" title={row.tooltip}>
          <span class="hbar-row__label">{row.label}</span>
          <div class="hbar-row__track">
            <div class="hbar-row__bar" style="width: {(row.value / max) * 100}%"></div>
          </div>
          <span class="hbar-row__value mono">{row.display}</span>
        </div>
      {/each}
    </div>
  {/if}
</article>
