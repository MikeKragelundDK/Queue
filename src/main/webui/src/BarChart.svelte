<script lang="ts">
  export interface Bar {
    label: string
    value: number
    tooltip: string
  }

  let { title, subtitle = '', bars, labelEvery = 6, emptyText = 'Ingen data i perioden.' }: {
    title: string
    subtitle?: string
    bars: Bar[]
    labelEvery?: number
    emptyText?: string
  } = $props()

  const W = 720
  const H = 150
  const LEFT = 34
  const BOTTOM = 22
  const plotW = W - LEFT
  const plotH = H - BOTTOM

  const max = $derived(Math.max(1, ...bars.map((b) => b.value)))
  const hasData = $derived(bars.some((b) => b.value > 0))
  const barW = $derived(Math.max(1, plotW / Math.max(1, bars.length) - 2))

  function barHeight(value: number): number {
    return (value / max) * (plotH - 10)
  }
</script>

<article class="chart-card">
  <h2>{title} {#if subtitle}<span class="muted">({subtitle})</span>{/if}</h2>
  {#if !hasData}
    <p class="muted">{emptyText}</p>
  {:else}
    <svg viewBox="0 0 {W} {H}" role="img" aria-label={title}>
      <line x1={LEFT} y1={plotH - barHeight(max)} x2={W} y2={plotH - barHeight(max)} class="chart-grid" />
      <text x={LEFT - 6} y={plotH - barHeight(max) + 3} class="chart-axis" text-anchor="end">{max}</text>
      <line x1={LEFT} y1={plotH} x2={W} y2={plotH} class="chart-baseline" />
      {#each bars as bar, i (bar.label + i)}
        <rect
          class="chart-bar"
          x={LEFT + i * (plotW / bars.length) + 1}
          y={plotH - barHeight(bar.value)}
          width={barW}
          height={barHeight(bar.value)}
          rx="2"
        >
          <title>{bar.tooltip}</title>
        </rect>
        {#if i % labelEvery === 0}
          <text x={LEFT + i * (plotW / bars.length) + barW / 2} y={H - 6} class="chart-axis" text-anchor="middle">
            {bar.label}
          </text>
        {/if}
      {/each}
    </svg>
  {/if}
</article>
