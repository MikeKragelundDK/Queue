package dk.eventit.queue.service;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import dk.eventit.queue.entity.CloseReason;
import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueSession;
import dk.eventit.queue.entity.QueueSessionStateChange;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class QueueVisitorEnqueueTest {

    @Inject
    QueueService queueService;

    private UUID rootUuid;

    @AfterEach
    void cleanUp() {
        if (rootUuid == null) {
            return;
        }
        QuarkusTransaction.requiringNew().run(() -> {
            Queue root = Queue.findByUuid(rootUuid);
            if (root != null) {
                deleteSubtree(root);
            }
        });
        rootUuid = null;
    }

    @Test
    void givenSameVisitorKeyTenTimes_whenEnqueueing_thenOnlyOneSessionIsCreated() {
        // Given
        UUID eventUuid = commitTree();
        String key = "sameKey";
        String eventKey = "event a";

        // When
        for (int i = 0; i < 10; i++) {
            queueService.enqueue(eventUuid, key, eventKey);
        }

        // Then
        assertEquals(1, countLive(eventUuid), "ti faner må kun give ét lod");
    }

    @Test
    void givenReopenedTab_whenEnqueueingWithSameKey_thenSamePlaceAndNumberComeBack() {
        // Given
        UUID eventUuid = commitTree();
        String key = "someKey";
        String eventKey = "event a";

        // When
        QueueSession firstSession = queueService.enqueue(eventUuid, key, eventKey);
        QueueSession reopenedSession = queueService.enqueue(eventUuid, key, eventKey);

        // Then
        assertEquals(firstSession.uuid, reopenedSession.uuid, "samme session skal komme retur");
        assertEquals(firstSession.sequenceNumber, reopenedSession.sequenceNumber,
                "pladsen i køen skal være bevaret");
        assertEquals(1, countLive(eventUuid));

        // Et efterladt mellemrum må ikke give to besøgende
        assertEquals(firstSession.uuid, queueService.enqueue(eventUuid, "  " + key + "  ", eventKey).uuid,
                "nøglen trimmes, så samme browser forbliver én besøgende");
        assertEquals(1, countLive(eventUuid));
    }

    @Test
    void givenNoVisitorKey_whenEnqueueingTwice_thenTwoSessionsAreCreated() {
        // Given
        UUID eventUuid = commitTree();
        String eventKey = "event a";

        // When
        QueueSession sessionOne = queueService.enqueue(eventUuid, null, eventKey);
        QueueSession sessionTwo = queueService.enqueue(eventUuid, null, eventKey);

        // Then
        assertNotEquals(sessionOne.uuid, sessionTwo.uuid, "uden nøgle dedupliceres der ikke");
        assertNotEquals(sessionOne.sequenceNumber, sessionTwo.sequenceNumber);
        assertEquals(2, countLive(eventUuid), "admin og simulator er upåvirkede");
    }

    @Test
    void givenMultipleConcurrentTabsWithSameKey_whenEnqueueing_thenOneSessionPerVisitorAndNoError() throws Exception {
        // Given
        UUID eventUuid = commitTree();
        String eventKey = "event a";
        int rounds = 10;

        // When
        for (int i = 0; i < rounds; i++) {
            String key = "someKey-" + i;
            List<QueueSession> results = runConcurrently(
                    () -> queueService.enqueue(eventUuid, key, eventKey),
                    () -> queueService.enqueue(eventUuid, key, eventKey));
            // Then
            assertEquals(results.get(0).uuid, results.get(1).uuid, "taberen skal have vinderens session");
        }
        assertEquals(rounds, countLive(eventUuid), "ti kapløb skal give ti sessioner, aldrig tyve");

    }

    @Test
    void givenClosedSession_whenEnqueueingWithSameKeyAgain_thenANewSessionIsCreated() {
        // Given
        UUID eventUuid = commitTree();
        String key = "someKey";
        String eventKey = "event a";
        QueueSession session = queueService.enqueue(eventUuid, key , eventKey);

        // When
        queueService.closeSession(session.uuid, CloseReason.ADMIN);

        QueueSession requeuedSession = queueService.enqueue(eventUuid, key, eventKey);

        // Then
        assertNotEquals(session.uuid, requeuedSession.uuid, "en lukket session spærrer ikke nøglen");
    }

    /** Slap en tom nøgle igennem, fik næste besøgende en fremmeds adgangstoken. */
    @Test
    void givenBlankVisitorKey_whenEnqueueing_thenItIsRejectedAndNoSessionIsCreated() {
        // Given
        UUID eventUuid = commitTree();
        String emptyKey = "";
        String emptyKeySpace = " ";
        String eventKey = "event a";

        // When/then
        assertThrows(IllegalArgumentException.class, () -> queueService.enqueue(eventUuid, emptyKey, eventKey));
        assertThrows(IllegalArgumentException.class, () -> queueService.enqueue(eventUuid, emptyKeySpace, eventKey));

        assertEquals(0, countLive(eventUuid));
    }

    /** Uden tjekket når MySQL's trunkeringsfejl kalderen som en 500. */
    @Test
    void givenTooLongVisitorKey_whenEnqueueing_thenItIsRejectedAndNoSessionIsCreated() {
        // Given
        UUID eventUuid = commitTree();
        String veryLongKey = "x".repeat(65);
        String exactTopKey = "x".repeat(64);
        String eventKey = "event a";
        // When/Then
        assertThrows(IllegalArgumentException.class, () -> queueService.enqueue(eventUuid, veryLongKey, eventKey));

        assertDoesNotThrow(() -> queueService.enqueue(eventUuid, exactTopKey, eventKey));
    }

    // === Hjælpere ===

    private UUID commitTree() {
        Leaf leaf = QuarkusTransaction.requiringNew().call(() -> {
            Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
            Queue subscription = queueService.createQueue("abonnement", QueueLevel.SUBSCRIPTION, global.uuid, 50, null);
            Queue organizer = queueService.createQueue("arrangør-a", QueueLevel.ORGANIZER, subscription.uuid, null, null);
            Queue event = queueService.createQueue("event-x", QueueLevel.EVENT, organizer.uuid, null, null);
            return new Leaf(global.uuid, event.uuid);
        });
        rootUuid = leaf.rootUuid();
        return leaf.eventUuid();
    }

    private long countLive(UUID queueUuid) {
        return QuarkusTransaction.requiringNew().call(
                () -> QueueSession.count("queue.uuid = ?1 and closedAt is null", queueUuid));
    }

    /** Barrieren gør kollisionen sandsynlig, ikke garanteret. */
    private <T> List<T> runConcurrently(Callable<T> first, Callable<T> second) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<T> a = pool.submit(() -> {
                barrier.await();
                return first.call();
            });
            Future<T> b = pool.submit(() -> {
                barrier.await();
                return second.call();
            });
            return Arrays.asList(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
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

    private record Leaf(UUID rootUuid, UUID eventUuid) {
    }
}
