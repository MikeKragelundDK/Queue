package dk.eventit.queue.service;

import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import dk.eventit.queue.entity.ClientType;
import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueSession;
import dk.eventit.queue.entity.QueueSessionState;
import dk.eventit.queue.entity.QueueSessionStateChange;
import dk.eventit.queue.entity.QueueType;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;

/** Ukendt nøgle betyder fælleskøen, ikke en fejl — normale kunder har ingen egne knuder. */
@QuarkusTest
class QueueKeyRoutingTest {

    @Inject
    QueueService queueService;

    @Inject
    QueueActivationService activationService;

    private UUID committedRootUuid;

    @AfterEach
    void cleanUp() {
        if (committedRootUuid == null) {
            return;
        }
        QuarkusTransaction.requiringNew().run(() -> {
            Queue root = Queue.findByUuid(committedRootUuid);
            if (root != null) {
                deleteSubtree(root);
            }
        });
        committedRootUuid = null;
    }

    @Test
    @TestTransaction
    void givenUnknownEventKey_whenResolving_thenGenericEventQueueIsChosen() {
        // Given: nøglen tilhører en normal kunde, som ingen egne knuder har.
        Tree tree = tree();

        // When
        Queue target = queueService.resolveQueueForKey(ClientType.EVENT_BOOKING, "ukendt-event");

        // Then
        assertEquals(tree.genericEvent().id, target.id, "ukendt nøgle hører til i fælleskøen");
    }

    @Test
    @TestTransaction
    void givenUnknownKeys_whenResolvingBothClientTypes_thenQueuesAreSeparate() {
        // Given
        Tree tree = tree();

        // When
        Queue booking = queueService.resolveQueueForKey(ClientType.EVENT_BOOKING, "ukendt-event");
        Queue signup = queueService.resolveQueueForKey(ClientType.MEMBER_SIGNUP, "ukendt-medlemssystem");

        // Then: hver sin kø, så et udsolgt arrangement ikke spærrer for medlemsoprettelser
        assertEquals(tree.genericEvent().id, booking.id);
        assertEquals(tree.genericMember().id, signup.id);
        assertNotEquals(booking.id, signup.id);
    }

    @Test
    @TestTransaction
    void givenKnownPriorityKey_whenResolving_thenCustomersOwnQueueIsChosen() {
        // Given
        Tree tree = tree();

        // When
        Queue target = queueService.resolveQueueForKey(ClientType.EVENT_BOOKING, "evt-1");

        // Then: fallbacket må ikke ramme dem, der har deres egen knude.
        assertEquals(tree.customerEvent().id, target.id);
    }

    @Test
    @TestTransaction
    void givenArchivedLeaf_whenResolving_thenGenericQueueIsChosen() {
        // Given: kundens event er arkiveret, men platformen sender stadig nøglen.
        Tree tree = tree();
        queueService.archiveQueue(tree.customerEvent().uuid);

        // When
        Queue target = queueService.resolveQueueForKey(ClientType.EVENT_BOOKING, "evt-1");

        // Then: arkiveret regnes som ukendt
        assertEquals(tree.genericEvent().id, target.id);
    }

    @Test
    @TestTransaction
    void givenFullGenericEventQueue_whenActivating_thenMemberSignupIsUnaffected() {
        // Given: event-bladet er fuldt, medlems-bladet har ét slot; eventet ankom først
        Tree tree = tree();
        queueService.updateMaxCapacity(tree.genericEvent().uuid, 0);
        queueService.updateMaxCapacity(tree.genericMember().uuid, 1);

        QueueSession booking = queueService.enqueue(
                queueService.resolveQueueForKey(ClientType.EVENT_BOOKING, "ukendt-event").uuid);
        QueueSession signup = queueService.enqueue(
                queueService.resolveQueueForKey(ClientType.MEMBER_SIGNUP, "ukendt-medlemssystem").uuid);
        assertTrue(booking.sequenceNumber < signup.sequenceNumber);

        // When
        activationService.activateNow();

        // Then: klienttyperne deler ikke loft
        assertEquals(QueueSessionState.OPEN, signup.state, "medlemsoprettelsen har sit eget loft");
        assertEquals(QueueSessionState.QUEUED, booking.state, "event-køen er på sit loft");
    }

    @Test
    void givenUnknownEventKey_whenEnqueueingByKey_thenSessionLandsInGenericQueue() {
        // Given: committet træ, fordi tilmeldingsstien kører i sin egen transaktion.
        UUID genericEventUuid = commitTree();

        // When
        QueueSession session = queueService.enqueueByKey(ClientType.EVENT_BOOKING, "ukendt-event", "besøgende-1");

        // Then
        UUID landedIn = QuarkusTransaction.requiringNew()
                .call(() -> QueueSession.<QueueSession>findById(session.id).queue.uuid);
        assertEquals(genericEventUuid, landedIn, "tilmeldingen fejler ikke — den havner i fælleskøen");
    }

    @Test
    @TestTransaction
    void givenKnownMemberSystemKey_whenResolving_thenCustomersOwnQueueIsChosen() {
        // Given: kunden har også et medlemssystem med sin egen nøgle.
        Tree tree = tree();
        Queue customerOrganizer = tree.customerEvent().parent;
        Queue customerMemberSystem = queueService.createQueue("kunde-medlemssystem", QueueLevel.MEMBERSYSTEM,
                customerOrganizer.uuid, 20, Queue.leafReference(QueueLevel.MEMBERSYSTEM, "mem-1"));

        // When
        Queue target = queueService.resolveQueueForKey(ClientType.MEMBER_SIGNUP, "mem-1");

        // Then: klienttypen vælger bladniveauet
        assertEquals(customerMemberSystem.id, target.id);
    }

    @Test
    @TestTransaction
    void givenForeignBranchUnderSharedQueue_whenResolvingUnknownKey_thenGenericQueueStillWins() {
        // Given: kundens gren bygges først (laveste id) og hænges under fælleskøen uden om servicens værn
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        Queue shared = queueService.createQueue("fælleskø", QueueLevel.SUBSCRIPTION, global.uuid, 50, null,
                QueueType.NORMAL);
        Queue priority = queueService.createQueue("prioritet", QueueLevel.SUBSCRIPTION, global.uuid, null, null,
                QueueType.PRIORITY);

        Queue customer = queueService.createQueue("kunde", QueueLevel.ORGANIZER, priority.uuid, 60,
                Queue.organizerReference("org-1"));
        Queue customerEvent = queueService.createQueue("kunde-event", QueueLevel.EVENT, customer.uuid, 20,
                Queue.leafReference(QueueLevel.EVENT, "evt-1"));

        Queue genericOrganizer = queueService.createQueue("generisk arrangør", QueueLevel.ORGANIZER, shared.uuid,
                null, null);
        Queue genericEvent = queueService.createQueue("generisk event-kø", QueueLevel.EVENT,
                genericOrganizer.uuid, 25, null);
        assertTrue(customerEvent.id < genericEvent.id, "kundens blad skal have lavest id, ellers beviser testen intet");

        customer.parent = shared;
        Queue.getEntityManager().flush();

        // When
        Queue target = queueService.resolveQueueForKey(ClientType.EVENT_BOOKING, "ukendt-event");

        // Then: de generiske knuder kendes på manglende nøgle, ikke på at stå først
        assertEquals(genericEvent.id, target.id);
    }
    @Test
    @TestTransaction
    void givenDrainingCustomerLeaf_whenResolvingItsKey_thenGenericQueueIsChosen() {
        // Given: kunden er nedgraderet, så grenen er sat til at dræne
        Tree tree = tree();
        queueService.drainQueue(tree.customerEvent().parent.uuid);

        // When: en ny ankomst med kundens egen nøgle
        Queue target = queueService.resolveQueueForKey(ClientType.EVENT_BOOKING, "evt-1");

        // Then: nye ankomster hører til i fælleskøen, men grenen rives ikke væk.
        assertEquals(tree.genericEvent().id, target.id);
        assertNull(tree.customerEvent().archivedAt, "grenen består, mens den dræner");
    }

    @Test
    void givenSameVisitorAtDifferentEvents_whenEnqueuing_thenSessionsAreSeparate(){
        // Given
        commitTree();

        // When
        QueueSession a = queueService.enqueueByKey(
                ClientType.EVENT_BOOKING, "ukendt-event-a", "samme-browser"
        );

        QueueSession b = queueService.enqueueByKey(
                ClientType.EVENT_BOOKING, "ukendt-event-b", "samme-browser"
        );

        // Then
        assertNotEquals(a.uuid, b.uuid);
    }

    // === Hjælpere ===

    private Tree tree() {
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        Queue shared = queueService.createQueue("fælleskø", QueueLevel.SUBSCRIPTION, global.uuid, 50, null,
                QueueType.NORMAL);
        Queue priority = queueService.createQueue("prioritet", QueueLevel.SUBSCRIPTION, global.uuid, null, null,
                QueueType.PRIORITY);

        Queue genericOrganizer = queueService.createQueue("generisk arrangør", QueueLevel.ORGANIZER, shared.uuid,
                null, null);
        Queue genericEvent = queueService.createQueue("generisk event-kø", QueueLevel.EVENT, genericOrganizer.uuid,
                25, null);
        Queue genericMember = queueService.createQueue("generisk medlems-kø", QueueLevel.MEMBERSYSTEM,
                genericOrganizer.uuid, 25, null);

        Queue customer = queueService.createQueue("kunde", QueueLevel.ORGANIZER, priority.uuid, 60,
                Queue.organizerReference("org-1"));
        Queue customerEvent = queueService.createQueue("kunde-event", QueueLevel.EVENT, customer.uuid, 20,
                Queue.leafReference(QueueLevel.EVENT, "evt-1"));

        return new Tree(global, genericEvent, genericMember, customerEvent);
    }

    private UUID commitTree() {
        Committed committed = QuarkusTransaction.requiringNew().call(() -> {
            Tree tree = tree();
            return new Committed(tree.global().uuid, tree.genericEvent().uuid);
        });
        committedRootUuid = committed.rootUuid();
        return committed.genericEventUuid();
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

    private record Tree(Queue global, Queue genericEvent, Queue genericMember, Queue customerEvent) {
    }

    private record Committed(UUID rootUuid, UUID genericEventUuid) {
    }
}
