package dk.eventit.queue.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

/** Efter batches er persistence context clearet — asserts genindlæser via uuid. */
@QuarkusTest
class QueueRetentionServiceTest {

    private static final Instant CUTOFF = Instant.now().minus(365, ChronoUnit.DAYS);

    @Inject
    QueueService queueService;

    @Inject
    QueueRetentionService retentionService;

    @Test
    @TestTransaction
    void givenOldAndRecentClosedSessions_whenDeletingBatch_thenOnlyOldOnesGoAndQueueStays() {
        // Given: en gammel lukket (13 mdr.), en frisk lukket og en gammel ventende
        Fixture f = fixture();
        QueueSession old = closedSession(f.event, Instant.now().minus(400, ChronoUnit.DAYS));
        QueueSession recent = closedSession(f.event, Instant.now().minus(1, ChronoUnit.HOURS));
        QueueSession waiting = session(f.event, QueueSessionState.QUEUED,
                Instant.now().minus(400, ChronoUnit.DAYS));

        // When
        int deleted = retentionService.deleteClosedSessionsBatch(CUTOFF, 100);

        // Then: kun den gamle lukkede er væk, inkl. transitionslog; køen består
        assertEquals(1, deleted);
        assertNull(QueueSession.findByUuid(old.uuid));
        assertEquals(0, QueueSessionStateChange.count(
                "session.id = ?1", old.id), "transitionsloggen fulgte med");
        assertNotNull(QueueSession.findByUuid(recent.uuid), "friske lukkede består");
        assertNotNull(QueueSession.findByUuid(waiting.uuid), "kun CLOSED røres — aldrig ventende");
        assertNotNull(Queue.findByUuid(f.event.uuid), "køen består");
    }

    @Test
    @TestTransaction
    void givenMoreOldSessionsThanBatchLimit_whenDeleting_thenBatchesAreBounded() {
        // Given: tre gamle lukkede sessioner
        Fixture f = fixture();
        Instant closedAt = Instant.now().minus(400, ChronoUnit.DAYS);
        closedSession(f.event, closedAt);
        closedSession(f.event, closedAt);
        closedSession(f.event, closedAt);

        // When/Then: batchgrænsen respekteres, og løkken tømmer over flere kald
        assertEquals(2, retentionService.deleteClosedSessionsBatch(CUTOFF, 2));
        assertEquals(1, retentionService.deleteClosedSessionsBatch(CUTOFF, 2));
        assertEquals(0, retentionService.deleteClosedSessionsBatch(CUTOFF, 2));
    }

    @Test
    @TestTransaction
    void givenArchivedTreePastRetention_whenDeleting_thenTreeGoesBottomUpWithSessions() {
        // Given: arrangør + event arkiveret for 13 måneder siden, med en session på bladet
        Fixture f = fixture();
        QueueSession session = closedSession(f.event, Instant.now().minus(400, ChronoUnit.DAYS));
        queueService.archiveQueue(f.organizer.uuid);
        Instant longAgo = Instant.now().minus(400, ChronoUnit.DAYS);
        f.organizer.archivedAt = longAgo;
        f.event.archivedAt = longAgo;

        // When: første omgang tager det barnløse blad, anden den nu barnløse arrangør
        assertEquals(1, deleteArchivedRound(CUTOFF));
        assertEquals(1, deleteArchivedRound(CUTOFF));
        assertEquals(0, deleteArchivedRound(CUTOFF));

        // Then: hele grenen inkl. sessionen er væk — abonnementet (aktivt) består
        assertNull(Queue.findByUuid(f.event.uuid));
        assertNull(Queue.findByUuid(f.organizer.uuid));
        assertNull(QueueSession.findByUuid(session.uuid));
        assertNotNull(Queue.findByUuid(f.subscription.uuid));
    }

    @Test
    @TestTransaction
    void givenRecentlyArchivedAndActiveQueues_whenDeleting_thenTheyAreUntouched() {
        // Given: et blad arkiveret for nylig — og resten af træet aktivt
        Fixture f = fixture();
        queueService.archiveQueue(f.event.uuid);

        // When
        int deleted = deleteArchivedRound(CUTOFF);

        // Then: nyligt arkiverede venter på deres 12 måneder; aktive røres aldrig
        assertEquals(0, deleted);
        assertNotNull(Queue.findByUuid(f.event.uuid));
        assertNotNull(Queue.findByUuid(f.organizer.uuid));
    }

    /** Eneste dækning af lås-række id 3 og completed-rapporteringen; fixturen committes. */
    @Test
    void givenOldSessionsAndArchivedTree_whenRunExecutes_thenBothRulesApplyAndRunIsReportedComplete() {
        // Given: en gammel lukket session og en gren arkiveret for 13 måneder siden
        Committed committed = QuarkusTransaction.requiringNew().call(() -> {
            Fixture f = fixture();
            QueueSession old = closedSession(f.event, Instant.now().minus(400, ChronoUnit.DAYS));
            queueService.archiveQueue(f.organizer.uuid);
            Instant longAgo = Instant.now().minus(400, ChronoUnit.DAYS);
            f.organizer.archivedAt = longAgo;
            f.event.archivedAt = longAgo;
            return new Committed(f.global.uuid, f.organizer.uuid, f.event.uuid, old.uuid);
        });

        try {
            // When
            QueueRetentionService.RetentionResult result = retentionService.run();

            // Then: begge regler ramte, og kørslen nåede hele vejen igennem
            assertTrue(result.lockAcquired(), "kørslen skal have fået fler-pod-låsen");
            assertTrue(result.completed(), "låsen blev holdt hele vejen igennem");
            assertTrue(result.deletedSessions() >= 1);
            assertTrue(result.deletedQueues() >= 2, "både bladet og den nu barnløse arrangør");
            QuarkusTransaction.requiringNew().run(() -> {
                assertNull(QueueSession.findByUuid(committed.sessionUuid()));
                assertNull(Queue.findByUuid(committed.eventUuid()));
                assertNull(Queue.findByUuid(committed.organizerUuid()));
                assertNotNull(Queue.findByUuid(committed.rootUuid()), "aktive knuder røres ikke");
            });
        } finally {
            deleteCommittedTree(committed.rootUuid());
        }
    }

    // === Fixture-hjælpere ===

    private record Committed(UUID rootUuid, UUID organizerUuid, UUID eventUuid, UUID sessionUuid) {
    }

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

    private int deleteArchivedRound(Instant cutoff) {
        List<UUID> victims = retentionService.expiredArchivedQueueUuids(cutoff);
        for (UUID uuid : victims) {
            queueService.deleteQueue(uuid);
        }
        return victims.size();
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

    private QueueSession closedSession(Queue queue, Instant closedAt) {
        QueueSession session = session(queue, QueueSessionState.CLOSED, closedAt.minus(1, ChronoUnit.HOURS));
        session.closedAt = closedAt;
        session.closeReason = CloseReason.COMPLETED;

        QueueSessionStateChange change = new QueueSessionStateChange();
        change.session = session;
        change.state = QueueSessionState.CLOSED;
        change.reason = CloseReason.COMPLETED;
        change.createdAt = closedAt;
        change.persist();
        return session;
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
}
