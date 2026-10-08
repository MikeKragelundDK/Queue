package dk.eventit.queue.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueType;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;

@QuarkusTest
class QueueMoveTest {

    @Inject
    QueueService queueService;

    @Test
    @TestTransaction
    void givenGenericOrganizer_whenMovingOutOfSharedQueue_thenItIsRejected() {
        // Given: den generiske arrangør, som al normal trafik routes til
        Tree t = tree();
        Queue generic = queueService.createQueue("generisk arrangør", QueueLevel.ORGANIZER, t.normal.uuid, null, null);
        queueService.createQueue("generisk event-kø", QueueLevel.EVENT, generic.uuid, 25, null);

        // When/Then: uden den har normale tilmeldinger ingenting at blive routet til
        assertThrows(IllegalArgumentException.class,
                () -> queueService.moveQueue(generic.uuid, t.priority.uuid));
        assertEquals(t.normal.id, generic.parent.id, "fælleskøens struktur bliver, hvor den er");
    }

    @Test
    @TestTransaction
    void givenPriorityOrganizer_whenMovingIntoSharedQueue_thenItIsRejected() {
        // Given: en priority-arrangør med egne lofter på både arrangør og blad
        Tree t = tree();

        // When/Then: en nedgradering dræner i stedet for at flytte (KPM-88)
        assertThrows(IllegalArgumentException.class,
                () -> queueService.moveQueue(t.organizer.uuid, t.normal.uuid));
        assertEquals(t.priority.id, t.organizer.parent.id, "grenen bliver, hvor den er");
    }
    @Test
    @TestTransaction
    void givenInvalidTargets_whenMoving_thenLevelInvariantAndArchiveRulesAreEnforced() {
        // Given
        Tree t = tree();

        // When/Then: forkert niveau, rod-flyt og arkiveret mål afvises
        assertThrows(IllegalArgumentException.class,
                () -> queueService.moveQueue(t.event.uuid, t.priority.uuid),
                "EVENT kan ikke hænge direkte under SUBSCRIPTION");
        assertThrows(IllegalArgumentException.class,
                () -> queueService.moveQueue(t.global.uuid, t.priority.uuid),
                "GLOBAL er roden og kan ikke flyttes");

        queueService.archiveQueue(t.normal.uuid);
        assertThrows(IllegalArgumentException.class,
                () -> queueService.moveQueue(t.organizer.uuid, t.normal.uuid),
                "kan ikke flytte ind under en arkiveret kø");
    }

    @Test
    @TestTransaction
    void givenSameParent_whenMoving_thenNothingHappens() {
        // Given
        Tree t = tree();

        // When: "flyt" til den nuværende forælder
        queueService.moveQueue(t.organizer.uuid, t.priority.uuid);

        // Then: no-op. Tjekkes pr. knude, så en delvis lækage ikke slipper igennem
        assertEquals(t.priority.id, t.organizer.parent.id);
        assertFalse(t.organizer.dirty, "den flyttede knude må ikke markeres dirty");
        assertFalse(t.priority.dirty, "forælderen må ikke markeres dirty");
        assertFalse(t.global.dirty, "roden må ikke markeres dirty");
    }

    // === Fixture ===

    private record Tree(Queue global, Queue normal, Queue priority, Queue organizer, Queue event) {
    }

    private Tree tree() {
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        Queue normal = subscription("fælleskø", global, 50, QueueType.NORMAL);
        Queue priority = subscription("priority", global, null, QueueType.PRIORITY);
        Queue organizer = queueService.createQueue("arrangør-a", QueueLevel.ORGANIZER, priority.uuid, 40, null);
        Queue event = queueService.createQueue("event-x", QueueLevel.EVENT, organizer.uuid, 20, null);
        return new Tree(global, normal, priority, organizer, event);
    }

    private Queue subscription(String name, Queue global, Integer maxCapacity, QueueType type) {
        Queue queue = queueService.createQueue(name, QueueLevel.SUBSCRIPTION, global.uuid, maxCapacity, null);
        queue.queueType = type;
        Queue.getEntityManager().flush();
        return queue;
    }
}
