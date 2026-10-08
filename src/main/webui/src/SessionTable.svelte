<script lang="ts">
  import { closeSession, type SessionRow } from './api'
  import { formatTimestamp } from './format'

  let { sessions, showQueue = true, onchanged, onerror }: {
    sessions: SessionRow[]
    showQueue?: boolean
    onchanged: () => void
    onerror: (message: string) => void
  } = $props()

  async function close(session: SessionRow) {
    try {
      await closeSession(session.uuid)
      onchanged()
    } catch (e) {
      onerror(e instanceof Error ? e.message : String(e))
    }
  }
</script>

<table>
  <thead>
    <tr>
      <th>UUID</th>
      {#if showQueue}<th>Kø</th>{/if}
      <th>Tilstand</th><th>Seq</th><th>Oprettet</th><th>Lukket</th><th>Årsag</th><th></th>
    </tr>
  </thead>
  <tbody>
    {#each sessions as session (session.uuid)}
      <tr>
        <td class="muted mono">{session.uuid}</td>
        {#if showQueue}<td>{session.queueName}</td>{/if}
        <td><span class="badge s-{session.state}">{session.state}</span></td>
        <td class="mono">{session.sequenceNumber}</td>
        <td>{formatTimestamp(session.createdAt)}</td>
        <td>{formatTimestamp(session.closedAt)}</td>
        <td class="muted">{session.closeReason?.toLowerCase() ?? '—'}</td>
        <td>
          {#if session.state !== 'CLOSED'}
            <button onclick={() => close(session)}>Luk</button>
          {/if}
        </td>
      </tr>
    {/each}
  </tbody>
</table>
