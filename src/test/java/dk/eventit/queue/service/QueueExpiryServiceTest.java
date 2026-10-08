package dk.eventit.queue.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import dk.eventit.queue.entity.CloseReason;
import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueSession;
import dk.eventit.queue.entity.QueueSessionState;
import dk.eventit.queue.entity.QueueSessionStateChange;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

/**
 * Arbejdsmetoderne kaldes direkte under testens transaktion. Asserts er pr.
 * session: continuous testing kan have committede sessioner liggende.
 */
@QuarkusTest
class QueueExpiryServiceTest {

    @Inject
    QueueService queueService;

    @Inject
    QueueExpiryService expiryService;

    @Inject
    EntityManager em;

    @Test
    @TestTransaction
    void givenOpenSessionPastDeadline_whenSweeping_thenClosedAsExpiredAndBranchIsDirty() {
        // Given: en OPEN-session, hvis deadline udløb for 20 minutter siden
        Instant now = Instant.now();
        Fixture f = fixture();
        QueueSession session = session(f.event, QueueSessionState.OPEN, minus(now, 60));
        session.openedAt = minus(now, 50);
        session.expiresAt = minus(now, 20);

        // When: kandidaten findes og lukkes
        assertTrue(expiryService.expiredOpenSessionUuids(now).contains(session.uuid),
                "sessionen er kandidat til udløb");
        int closed = expiryService.closeIfExpired(session.uuid, now);

        // Then
        assertEquals(1, closed);
        assertEquals(QueueSessionState.CLOSED, session.state);
        assertEquals(CloseReason.EXPIRED, session.closeReason);
        assertEquals(1, QueueSessionStateChange.count("session = ?1 and state = ?2",
                session, QueueSessionState.CLOSED), "lukningen er logget i transitionsloggen");
        assertTrue(f.event.dirty && f.organizer.dirty && f.subscription.dirty && f.global.dirty,
                "den frigivne plads markerer hele grenen dirty");
    }

    @Test
    @TestTransaction
    void givenOpenSessionWithoutExpiresAt_whenOlderThanTtl_thenFallbackExpiresIt() {
        // Given: en ældre række uden expiresAt (fra før feltet blev sat ved OPEN)
        Instant now = Instant.now();
        Fixture f = fixture();
        QueueSession session = session(f.event, QueueSessionState.OPEN, minus(now, 90));
        session.openedAt = minus(now, 80);

        // When: openedAt + open-ttl (30 min) er forlængst passeret
        assertTrue(expiryService.expiredOpenSessionUuids(now).contains(session.uuid));
        expiryService.closeIfExpired(session.uuid, now);

        // Then
        assertEquals(QueueSessionState.CLOSED, session.state);
        assertEquals(CloseReason.EXPIRED, session.closeReason);
    }

    @Test
    @TestTransaction
    void givenWaitingSessionsWithoutSignOfLife_whenSweeping_thenClosedAsAbandoned() {
        // Given: én INITIAL og én QUEUED, begge tavse længere end waiting-timeout
        Instant now = Instant.now();
        Instant cutoff = minus(now, 15);
        Fixture f = fixture();
        QueueSession initial = session(f.event, QueueSessionState.INITIAL, minus(now, 40));
        QueueSession queued = session(f.event, QueueSessionState.QUEUED, minus(now, 40));

        // When: begge er kandidater og lukkes
        assertTrue(expiryService.silentWaitingSessionUuids(cutoff)
                .containsAll(List.of(initial.uuid, queued.uuid)));
        expiryService.closeIfAbandoned(initial.uuid, cutoff);
        expiryService.closeIfAbandoned(queued.uuid, cutoff);

        // Then
        assertEquals(QueueSessionState.CLOSED, initial.state);
        assertEquals(CloseReason.ABANDONED, initial.closeReason);
        assertEquals(QueueSessionState.CLOSED, queued.state);
        assertEquals(CloseReason.ABANDONED, queued.closeReason);
    }

    @Test
    @TestTransaction
    void givenUndrawnPreQueue_whenSweeping_thenWaitingSessionsAreLeftAlone() {
        // Given: to tavse ventende i et venteværelse, hvor lodtrækningen ikke har kørt
        Instant now = Instant.now();
        Instant cutoff = minus(now, 15);
        Fixture f = fixture();
        f.event.salesStartAt = now.plusSeconds(7200);
        QueueSession venter = session(f.event, QueueSessionState.INITIAL, minus(now, 600));

        // When/Then: lodtrækningen er sluttidspunktet, så ingen inaktivitetsgrænse imens
        assertFalse(expiryService.silentWaitingSessionUuids(cutoff).contains(venter.uuid),
                "ventende i et utrukket venteværelse må aldrig fejes, uanset hvor længe de har ventet");

        // Og når lodtrækningen har kørt, gælder de almindelige regler igen
        f.event.drawnAt = now;
        assertTrue(expiryService.silentWaitingSessionUuids(cutoff).contains(venter.uuid),
                "efter lodtrækningen er køen en almindelig kø");
    }

    @Test
    @TestTransaction
    void givenRecentPingOrFutureDeadline_whenSweeping_thenSessionsAreUntouched() {
        // Given: en gammel QUEUED med frisk ping og en OPEN med deadline i fremtiden
        Instant now = Instant.now();
        Instant cutoff = minus(now, 15);
        Fixture f = fixture();
        QueueSession pinging = session(f.event, QueueSessionState.QUEUED, minus(now, 40));
        pinging.lastPingAt = minus(now, 1);
        QueueSession active = session(f.event, QueueSessionState.OPEN, minus(now, 40));
        active.openedAt = minus(now, 10);
        active.expiresAt = now.plus(20, ChronoUnit.MINUTES);

        // When/Then: ingen af dem er kandidater, og recheck-vagten lukker dem heller ikke
        assertFalse(expiryService.silentWaitingSessionUuids(cutoff).contains(pinging.uuid),
                "pinget holder den ventende ude af kandidatlisten");
        assertFalse(expiryService.expiredOpenSessionUuids(now).contains(active.uuid),
                "fremtidig deadline holder den åbne ude af kandidatlisten");
        assertEquals(0, expiryService.closeIfAbandoned(pinging.uuid, cutoff));
        assertEquals(0, expiryService.closeIfExpired(active.uuid, now));
        assertEquals(QueueSessionState.QUEUED, pinging.state);
        assertNull(pinging.closeReason);
        assertEquals(QueueSessionState.OPEN, active.state);
        assertNull(active.closeReason);
    }

    @Test
    @TestTransaction
    void givenAlreadySweptSession_whenSweepingAgain_thenNothingChanges() {
        // Given: en udløbet session, der allerede er fejet
        Instant now = Instant.now();
        Fixture f = fixture();
        QueueSession session = session(f.event, QueueSessionState.OPEN, minus(now, 60));
        session.openedAt = minus(now, 50);
        session.expiresAt = minus(now, 20);
        expiryService.closeIfExpired(session.uuid, now);
        // DATETIME(6) runder til mikrosekunder, så tidspunktet læses tilbage først.
        em.flush();
        em.refresh(session);
        Instant firstClosedAt = session.closedAt;

        // When: recheck-vagten ser CLOSED og rører ingenting
        int closedAgain = expiryService.closeIfExpired(session.uuid, now);

        // Then: første lukning vinder — hverken tidspunkt, årsag eller log ændres
        assertEquals(0, closedAgain);
        assertEquals(firstClosedAt, session.closedAt);
        assertEquals(CloseReason.EXPIRED, session.closeReason);
        assertEquals(1, QueueSessionStateChange.count("session = ?1 and state = ?2",
                session, QueueSessionState.CLOSED));
    }

    @Test
    @TestTransaction
    void givenEngineOpensSession_whenTransitioning_thenExpiresAtDeadlineIsSet() {
        // Given: en ventende session
        Instant now = Instant.now();
        Fixture f = fixture();
        QueueSession session = session(f.event, QueueSessionState.QUEUED, minus(now, 5));

        // When: motoren lukker den ind
        queueService.transitionToOpen(session, now);

        // Then: deadline = nu + open-ttl (30 min), så expiry-jobbet kan frigive pladsen
        assertEquals(now.plus(30, ChronoUnit.MINUTES), session.expiresAt);
        assertEquals(now, session.openedAt);
    }

    /** Eneste dækning af lås-rækken (id 2) og én transaktion pr. lukning. */
    @Test
    void givenExpiredAndAbandonedSessions_whenSweepRuns_thenBothAreClosedAndRunIsReportedComplete() {
        // Given: én OPEN forbi deadline og én ventende uden livstegn — committet
        Instant now = Instant.now();
        Committed committed = QuarkusTransaction.requiringNew().call(() -> {
            Fixture f = fixture();
            QueueSession expired = session(f.event, QueueSessionState.OPEN, minus(now, 60));
            expired.openedAt = minus(now, 50);
            expired.expiresAt = minus(now, 20);

            // Et døgn, ikke lige forbi grænsen — ellers rød, hver gang waiting-timeout hæves.
            QueueSession abandoned = session(f.event, QueueSessionState.QUEUED, minus(now, 60 * 25));
            abandoned.lastPingAt = minus(now, 60 * 24);
            return new Committed(f.global.uuid, expired.uuid, abandoned.uuid);
        });

        try {
            // When
            QueueExpiryService.ExpiryResult result = expiryService.sweep();

            // Then: begge lukket med hver sin årsag, og kørslen nåede igennem
            assertTrue(result.lockAcquired(), "fejningen skal have fået fler-pod-låsen");
            assertTrue(result.completed(), "låsen blev holdt hele vejen igennem");
            QuarkusTransaction.requiringNew().run(() -> {
                QueueSession expired = QueueSession.findByUuid(committed.expiredUuid());
                QueueSession abandoned = QueueSession.findByUuid(committed.abandonedUuid());
                assertEquals(QueueSessionState.CLOSED, expired.state);
                assertEquals(CloseReason.EXPIRED, expired.closeReason);
                assertEquals(QueueSessionState.CLOSED, abandoned.state);
                assertEquals(CloseReason.ABANDONED, abandoned.closeReason);
            });
        } finally {
            deleteCommittedTree(committed.rootUuid());
        }
    }

    // === Fixture-hjælpere ===

    private record Committed(UUID rootUuid, UUID expiredUuid, UUID abandonedUuid) {
    }

    /** Går uden om {@code deleteQueue}, som kræver arkivering først. */
    private void deleteCommittedTree(UUID rootUuid) {
        QuarkusTransaction.requiringNew().run(() -> {
            Queue root = Queue.findByUuid(rootUuid);
            if (root != null) {
                deleteSubtree(root);
            }
        });
    }

    private void deleteSubtree(Queue node) {
        for (Queue child : Queue.<Queue>list("parent", node)) {
            deleteSubtree(child);
        }
        QueueSessionStateChange.delete(
                "session.id in (select s.id from QueueSession s where s.queue = ?1)", node);
        QueueSession.delete("queue", node);
        node.delete();
    }

    private record Fixture(Queue global, Queue subscription, Queue organizer, Queue event) {
    }

    private Fixture fixture() {
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        Queue subscription = queueService.createQueue("abonnement", QueueLevel.SUBSCRIPTION, global.uuid, 50, null);
        Queue organizer = queueService.createQueue("arrangør-a", QueueLevel.ORGANIZER, subscription.uuid, null, null);
        Queue event = queueService.createQueue("event-x", QueueLevel.EVENT, organizer.uuid, null, null);
        return new Fixture(global, subscription, organizer, event);
    }

    private QueueSession session(Queue queue, QueueSessionState state, Instant createdAt) {
        QueueSession session = new QueueSession();
        session.uuid = UUID.randomUUID();
        session.queue = queue;
        session.createdAt = createdAt;
        session.state = state;
        session.persist();
        session.sequenceNumber = session.id;
        return session;
    }

    private static Instant minus(Instant now, long minutes) {
        return now.minus(minutes, ChronoUnit.MINUTES);
    }
}
