<script lang="ts">
  import { fetchLiveStats, fetchStats, type LiveStats, type SearchHit, type Stats, type TimeBucket } from './api'
  import BarChart from './BarChart.svelte'
  import CapacityTree from './CapacityTree.svelte'
  import HBarList from './HBarList.svelte'
  import SearchBox from './SearchBox.svelte'

  let { onerror }: { onerror: (message: string) => void } = $props()

  const INTERVALS = [
    { hours: 24, label: '24 timer' },
    { hours: 168, label: '7 dage' },
    { hours: 720, label: '30 dage' },
    { hours: 2160, label: '90 dage' }
  ]

  let live = $state<LiveStats | null>(null)
  let stats = $state<Stats | null>(null)
  let scope = $state<{ uuid: string; name: string } | null>(null)
  let windowHours = $state(24)

  let liveVersion = $state(0)

  async function loadLive() {
    try {
      live = await fetchLiveStats(scope?.uuid ?? null)
      liveVersion += 1
    } catch (e) {
      onerror(e instanceof Error ? e.message : String(e))
    }
  }

  async function loadHistorical() {
    try {
      stats = await fetchStats(scope?.uuid ?? null, windowHours)
    } catch (e) {
      onerror(e instanceof Error ? e.message : String(e))
    }
  }

  $effect(() => {
    void scope
    loadLive()
    const timer = setInterval(loadLive, 5000)
    return () => clearInterval(timer)
  })

  $effect(() => {
    void scope
    void windowHours
    loadHistorical()
    const timer = setInterval(loadHistorical, 30000)
    return () => clearInterval(timer)
  })

  function onScopePick(hit: SearchHit) {
    scope = { uuid: hit.queue.uuid, name: hit.queue.name }
  }

  function bucketLabel(iso: string, bucketHours: number): string {
    const d = new Date(iso)
    if (bucketHours < 24) {
      return `${String(d.getHours()).padStart(2, '0')}`
    }
    return `${String(d.getDate()).padStart(2, '0')}/${String(d.getMonth() + 1).padStart(2, '0')}`
  }

  function bars(buckets: TimeBucket[], bucketHours: number, unit: string) {
    return buckets.map((b) => ({
      label: bucketLabel(b.start, bucketHours),
      value: b.count,
      tooltip: `${bucketLabel(b.start, bucketHours)}${bucketHours < 24 ? ':00' : ''} (+${bucketHours}t): ${b.count} ${unit}`
    }))
  }

  function labelEvery(buckets: TimeBucket[]): number {
    return Math.max(1, Math.round(buckets.length / 6))
  }
</script>

<main class="stats">
  <div class="stats__scope">
    <SearchBox onpick={onScopePick} {onerror} />
    {#if scope}
      <button class="stats__scope-chip" onclick={() => (scope = null)} title="Fjern scope — vis hele systemet">
        Viser: {scope.name} ✕
      </button>
    {:else}
      <span class="muted">Hele systemet — søg for at scope til et event, en arrangør eller et abonnement.</span>
    {/if}
  </div>

  <!-- ============ LIGE NU ============ -->
  <h2 class="stats__section">Lige nu <span class="muted">(opdateres hvert 5. sekund)</span></h2>
  {#if live === null}
    <p class="muted">Henter…</p>
  {:else}
    <div class="stat-row stat-row--kpi">
      <div class="stat-tile">
        <span class="stat-tile__value">{live.waitingNow}</span>
        <span class="stat-tile__label">Ventende nu</span>
      </div>
      <div class="stat-tile">
        <span class="stat-tile__value">{live.openNow}</span>
        <span class="stat-tile__label">Lukket ind nu</span>
      </div>
      <div class="stat-tile">
        <span class="stat-tile__value">{live.arrivalsLastHour}</span>
        <span class="stat-tile__label">Ankomster seneste time</span>
      </div>
    </div>

    <div class="stats__pair">
      <CapacityTree scopeUuid={scope?.uuid ?? null} version={liveVersion} {onerror} />
      <!-- Bevidst uden nævner: "16/50" ville læses som pladser brugt, men loftet gælder kun de indlukkede. -->
      <HBarList title="Kø-tryk pr. abonnement" subtitle="venter lige nu, hele undertræet"
        wideValues
        rows={live.perSubscription.map((s) => ({
          label: s.name,
          value: s.waiting,
          display: `${s.waiting} i kø · ${s.open}/${s.maxCapacity ?? '∞'} inde`,
          tooltip: `${s.name}: ${s.waiting} venter (ankommet, ikke lukket ind) · `
            + `${s.open} lukket ind af loftet ${s.maxCapacity ?? '∞'}`
        }))}
        emptyText="Ingen abonnements-køer." />
    </div>
  {/if}

  <!-- ============ HISTORISK ============ -->
  <h2 class="stats__section">
    Historisk
    <span class="segmented">
      {#each INTERVALS as interval (interval.hours)}
        <button class:active={windowHours === interval.hours} onclick={() => (windowHours = interval.hours)}>
          {interval.label}
        </button>
      {/each}
    </span>
  </h2>

  {#if stats === null}
    <p class="muted">Henter…</p>
  {:else}
    {#if stats.truncated}
      <p class="error">Datamængden oversteg beregningsgrænsen — tallene er baseret på et udsnit.</p>
    {/if}

    <div class="stat-row stat-row--kpi">
      <div class="stat-tile">
        <span class="stat-tile__value">{stats.waitTime.samples > 0 ? `${stats.waitTime.medianMinutes} min` : '—'}</span>
        <span class="stat-tile__label">Median ventetid</span>
      </div>
      <div class="stat-tile">
        <span class="stat-tile__value">{stats.waitTime.samples > 0 ? `${stats.waitTime.p95Minutes} min` : '—'}</span>
        <span class="stat-tile__label">P95 ventetid</span>
      </div>
      <div class="stat-tile">
        <span class="stat-tile__value">{stats.processingTime.samples > 0 ? `${stats.processingTime.medianMinutes} min` : '—'}</span>
        <span class="stat-tile__label">Median ekspedition</span>
      </div>
      <div class="stat-tile">
        <span class="stat-tile__value">{stats.abandonment.completed + stats.abandonment.abandoned > 0 ? `${stats.abandonment.ratePct} %` : '—'}</span>
        <span class="stat-tile__label">Frafald ({stats.abandonment.abandoned} af {stats.abandonment.abandoned + stats.abandonment.completed})</span>
      </div>
      <div class="stat-tile">
        <span class="stat-tile__value">{stats.abandonment.expired}</span>
        <span class="stat-tile__label">Spildte pladser (udløbet)</span>
      </div>
    </div>

    <BarChart title="Ankomster" subtitle="pr. {stats.bucketHours} time(r)"
      bars={bars(stats.arrivals, stats.bucketHours, 'nye')} labelEvery={labelEvery(stats.arrivals)} />
    <BarChart title="Kølængde over tid" subtitle="ventende ved bucket-slut"
      bars={bars(stats.queueLength, stats.bucketHours, 'ventende')} labelEvery={labelEvery(stats.queueLength)} />

    <div class="stats__pair">
      <BarChart title="Indlukninger" subtitle="pr. {stats.bucketHours} time(r)"
        bars={bars(stats.opens, stats.bucketHours, 'lukket ind')} labelEvery={labelEvery(stats.opens)}
        emptyText="Ingen indlukninger endnu — kræver aktiveringsmotoren." />
      <BarChart title="Gennemførelser" subtitle="pr. {stats.bucketHours} time(r)"
        bars={bars(stats.completions, stats.bucketHours, 'gennemført')} labelEvery={labelEvery(stats.completions)}
        emptyText="Ingen gennemførelser i perioden." />
    </div>

    <BarChart title="Ventetid over døgnet" subtitle="median-minutter pr. klokketime i intervallet" labelEvery={3}
      bars={stats.waitByHourOfDay.map((b) => ({
        label: b.label,
        value: b.value,
        tooltip: `kl. ${b.label}: median ${b.value} min (${b.samples} målinger)`
      }))}
      emptyText="Ingen ventetidsdata endnu — kræver aktiveringsmotoren (QUEUED/OPEN)." />

    <div class="stats__pair">
      <HBarList title="Ventetid pr. kønummer" subtitle="median-minutter pr. positionsinterval"
        rows={stats.waitBySequenceBucket.map((b) => ({
          label: b.label,
          value: b.value,
          display: `${b.value} min`,
          tooltip: `Position ${b.label}: median ${b.value} min (${b.samples} målinger)`
        }))}
        emptyText="Ingen data endnu — kræver aktiveringsmotoren." />
      <HBarList title="Frafald vs. ventetid" subtitle="andel der opgav, pr. ventetids-bucket"
        rows={stats.abandonment.byWaitBucket.map((b) => ({
          label: b.label,
          value: b.value,
          display: `${b.value} %`,
          tooltip: `${b.label} ventetid: ${b.value} % opgav (${b.samples} sessioner)`
        }))}
        emptyText="Ingen data endnu — kræver aktiveringsmotoren." />
    </div>

    <HBarList title="Loft-tryk" subtitle="minutter på loftet pr. kapacitetsknude i intervallet"
      rows={stats.capacityPressure.map((n) => ({
        label: `${n.name} (${n.level.toLowerCase()}, loft ${n.maxCapacity})`,
        value: n.minutesAtCap,
        display: `${n.minutesAtCap} min`,
        tooltip: `${n.name}: på loftet i ${n.minutesAtCap} min fordelt på ${n.hits} perioder`
      }))}
      emptyText="Intet loft har været ramt i intervallet." />
  {/if}
</main>
