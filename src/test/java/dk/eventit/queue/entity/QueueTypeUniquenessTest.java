package dk.eventit.queue.entity;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import dk.eventit.queue.service.SqlErrors;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;

/** Flusher eksplicit — ellers slår unique-bruddet først ud ved commit, som {@code @TestTransaction} aldrig når. */
@QuarkusTest
class QueueTypeUniquenessTest {

    @Test
    @TestTransaction
    void givenTwoSubscriptionsOfSameType_whenPersisting_thenTheSecondIsRejected() {
        // Given
        Queue global = persist(node(QueueLevel.GLOBAL, "global", null));
        persist(subscription(global, "normal", QueueType.NORMAL));

        // When
        Queue duplicate = subscription(global, "normal igen", QueueType.NORMAL);

        // Then
        RuntimeException error = assertThrows(RuntimeException.class, () -> persist(duplicate));
        assertTrue(SqlErrors.isDuplicateKey(error), "afvisningen skal komme fra unique-indekset: " + error);
    }

    @Test
    @TestTransaction
    void givenOneOfEachType_whenPersisting_thenBothAreAllowed() {
        // Given/When
        Queue global = persist(node(QueueLevel.GLOBAL, "global", null));
        persist(subscription(global, "normal", QueueType.NORMAL));

        // Then
        assertDoesNotThrow(() -> persist(subscription(global, "priority", QueueType.PRIORITY)));
    }

    @Test
    @TestTransaction
    void givenArchivedSubscription_whenPersistingSameTypeAgain_thenItIsAllowed() {
        // Given: en arkiveret normal-kø
        Queue global = persist(node(QueueLevel.GLOBAL, "global", null));
        Queue archived = subscription(global, "gammel normal", QueueType.NORMAL);
        archived.archivedAt = Instant.now();
        persist(archived);

        // When/Then
        assertDoesNotThrow(() -> persist(subscription(global, "ny normal", QueueType.NORMAL)));
    }

    @Test
    @TestTransaction
    void givenManyNodesWithoutType_whenPersisting_thenTheyDoNotCollide() {
        // Given: typen er null uden for abonnementslaget
        Queue global = persist(node(QueueLevel.GLOBAL, "global", null));
        Queue subscription = persist(subscription(global, "normal", QueueType.NORMAL));
        Queue organizerA = persist(node(QueueLevel.ORGANIZER, "arrangør a", subscription));

        // When
        Queue organizerB = persist(node(QueueLevel.ORGANIZER, "arrangør b", subscription));

        // Then: NULL er indbyrdes forskellige i et unikt indeks
        assertNull(organizerA.queueType);
        assertNull(organizerB.queueType);
        assertNotNull(organizerB.id);
    }

    // === Hjælpere ===

    private static Queue node(QueueLevel level, String name, Queue parent) {
        Queue queue = new Queue();
        queue.uuid = UUID.randomUUID();
        queue.name = name;
        queue.level = level;
        queue.parent = parent;
        return queue;
    }

    private static Queue subscription(Queue global, String name, QueueType type) {
        Queue queue = node(QueueLevel.SUBSCRIPTION, name, global);
        queue.queueType = type;
        return queue;
    }

    private static Queue persist(Queue queue) {
        queue.persist();
        Queue.getEntityManager().flush();
        return queue;
    }
}
