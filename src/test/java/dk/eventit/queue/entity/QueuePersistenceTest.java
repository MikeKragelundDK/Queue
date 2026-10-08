package dk.eventit.queue.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class QueuePersistenceTest {

    @Test
    @TestTransaction
    void givenSharedCapacityTree_whenWalkingFromLeafToRoot_thenLevelsAndCapacitiesMatch() {
        // Given
        Queue event = persistSharedCapacityTree();

        // When
        Queue found = Queue.findByUuid(event.uuid);

        // Then
        assertNotNull(found);
        assertEquals(QueueLevel.EVENT, found.level);
        assertNull(found.maxCapacity, "eventet har intet eget loft");

        Queue organizer = found.parent;
        Queue subscription = organizer.parent;
        Queue global = subscription.parent;

        assertEquals(QueueLevel.ORGANIZER, organizer.level);
        assertNull(organizer.maxCapacity, "arrangøren deler abonnementets loft");
        assertEquals(QueueLevel.SUBSCRIPTION, subscription.level);
        assertEquals(50, subscription.maxCapacity);
        assertEquals(QueueLevel.GLOBAL, global.level);
        assertEquals(1000, global.maxCapacity);
        assertNull(global.parent, "roden har ingen parent");
    }

    @Test
    @TestTransaction
    void givenNewSessionInLeafQueue_whenPersisted_thenItStartsInInitialState() {
        // Given
        Queue event = persistSharedCapacityTree();
        QueueSession session = new QueueSession();
        session.uuid = UUID.randomUUID();
        session.queue = event;
        session.sequenceNumber = 1;

        // When
        session.persist();

        // Then
        QueueSession found = QueueSession.findByUuid(session.uuid);
        assertNotNull(found);
        assertEquals(QueueSessionState.INITIAL, found.state);
        assertEquals(1, found.sequenceNumber);
        assertNotNull(found.createdAt);
        assertEquals(event.uuid, found.queue.uuid);
    }

    @Test
    @TestTransaction
    void givenFullBusinessHierarchy_whenPersisted_thenWholeTreeWithCapacitiesCanBeReadBack() {
        // Given: ingen queueType — ux_queue_active_type ville gøre testen afhængig af andre klassers fixturer
        Queue global = node("global", QueueLevel.GLOBAL, null, 1000);

        // Delt loft
        Queue sharedCap = node("abonnement-delt-loft", QueueLevel.SUBSCRIPTION, global, 50);
        Queue sharedOrg = node("delt-arrangør", QueueLevel.ORGANIZER, sharedCap, null);
        node("delt-event", QueueLevel.EVENT, sharedOrg, null);
        Queue sharedMemberSystem = node("delt-medlemssystem", QueueLevel.MEMBERSYSTEM, sharedOrg, null);

        // Eget loft; abonnementet står uden
        Queue ownCap = node("abonnement-eget-loft", QueueLevel.SUBSCRIPTION, global, null);
        Queue ownOrg = node("egen-arrangør", QueueLevel.ORGANIZER, ownCap, 400);
        Queue ownEvent = node("egen-event", QueueLevel.EVENT, ownOrg, 100);

        // When
        Queue root = Queue.findByUuid(global.uuid);
        List<Queue> subscriptions = Queue.list("parent", root);
        List<Queue> sharedOrgLeaves = Queue.list("parent", sharedOrg);
        Queue deepestLeaf = Queue.findByUuid(ownEvent.uuid);
        Queue memberSystemLeaf = Queue.findByUuid(sharedMemberSystem.uuid);

        // Then: scopet til dette træ — databasen genbruges på tværs af kørsler
        assertEquals(2, subscriptions.size(), "roden har begge abonnementer som børn");
        assertEquals(2, sharedOrgLeaves.size(), "arrangøren har både event og medlemssystem");
        assertEquals(1, Queue.count("parent", ownOrg), "arrangøren med eget loft har sit event");

        assertEquals(100, deepestLeaf.maxCapacity);
        assertEquals(400, deepestLeaf.parent.maxCapacity);
        assertNull(deepestLeaf.parent.parent.maxCapacity, "abonnementet bærer selv intet loft her");
        assertEquals(1000, deepestLeaf.parent.parent.parent.maxCapacity);
        assertEquals(QueueLevel.GLOBAL, deepestLeaf.parent.parent.parent.level);

        assertEquals(QueueLevel.MEMBERSYSTEM, memberSystemLeaf.level);
        assertEquals(QueueLevel.ORGANIZER, memberSystemLeaf.parent.level);
        assertNull(memberSystemLeaf.maxCapacity);
    }

    /** GLOBAL(1000) → abonnement(50) → arrangør(null) → event(null); returnerer bladet. */
    private Queue persistSharedCapacityTree() {
        Queue global = node("global", QueueLevel.GLOBAL, null, 1000);
        Queue subscription = node("abonnement-delt-loft", QueueLevel.SUBSCRIPTION, global, 50);
        Queue organizer = node("arrangør-a", QueueLevel.ORGANIZER, subscription, null);
        organizer.externalReference = "OrganizerKey: demo-a";
        Queue event = node("event-x", QueueLevel.EVENT, organizer, null);
        event.externalReference = "EventKey: demo-x";
        return event;
    }

    private Queue node(String name, QueueLevel level, Queue parent, Integer maxCapacity) {
        Queue queue = new Queue();
        queue.uuid = UUID.randomUUID();
        queue.name = name;
        queue.level = level;
        queue.parent = parent;
        queue.maxCapacity = maxCapacity;
        queue.persist();
        return queue;
    }
}
