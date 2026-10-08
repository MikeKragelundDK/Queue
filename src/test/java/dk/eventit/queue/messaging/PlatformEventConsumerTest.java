package dk.eventit.queue.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueType;
import dk.eventit.queue.service.QueueService;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.json.JsonObject;
import jakarta.inject.Inject;

/** Consumeren kaldes direkte med fabrikerede payloads — ingen broker. */
@QuarkusTest
class PlatformEventConsumerTest {

    @Inject
    QueueService queueService;

    @Inject
    PlatformEventConsumer consumer;

    @Test
    @TestTransaction
    void givenOrganizerCreatedEvent_whenConsumedTwice_thenOneOrganizerExistsUnderSubscription() {
        // Given
        Fixture f = fixture();
        String key = "org-" + UUID.randomUUID();
        JsonObject event = new JsonObject()
                .put("type", "organizer.created")
                .put("organizerKey", key)
                .put("name", "Nordhavn Live")
                .put("subscription", "PRIORITY");

        // When: samme besked leveres to gange (at-least-once)
        consumer.on(event);
        consumer.on(event);

        // Then: én aktiv arrangør, uden eget loft, under priority-knuden
        assertEquals(1, Queue.count("externalReference", "OrganizerKey: " + key));
        Queue organizer = Queue.<Queue>find("externalReference", "OrganizerKey: " + key).firstResult();
        assertEquals(QueueLevel.ORGANIZER, organizer.level);
        assertEquals(f.priority.id, organizer.parent.id);
        assertNull(organizer.maxCapacity);
    }

    @Test
    @TestTransaction
    void givenSubscriptionChangedToNormal_whenConsumed_thenBranchDrainsAndStaysPut() {
        // Given: priority-arrangør oprettet via event
        Fixture f = fixture();
        String key = "org-" + UUID.randomUUID();
        consumer.on(new JsonObject().put("type", "organizer.created")
                .put("organizerKey", key).put("name", "Aarhus Kulturhus").put("subscription", "PRIORITY"));
        Queue organizer = Queue.<Queue>find("externalReference", "OrganizerKey: " + key).firstResult();

        // When: nedgradering til fælleskøen
        consumer.on(new JsonObject().put("type", "organizer.subscription-changed")
                .put("organizerKey", key).put("subscription", "NORMAL"));

        // Then: grenen dræner under priority i stedet for at flytte (KPM-88)
        assertEquals(f.priority.id, organizer.parent.id, "grenen bliver, hvor den er");
        assertNotNull(organizer.drainingAt, "grenen skal dræne");
    }
    @Test
    @TestTransaction
    void givenEventLifecycle_whenCreatedAndArchived_thenLeafFollowsOrganizer() {
        // Given: arrangør + event-blad oprettet via events
        Fixture f = fixture();
        String orgKey = "org-" + UUID.randomUUID();
        String eventKey = "evt-" + UUID.randomUUID();
        consumer.on(new JsonObject().put("type", "organizer.created")
                .put("organizerKey", orgKey).put("name", "Odense Sport").put("subscription", "PRIORITY"));
        consumer.on(new JsonObject().put("type", "event.created")
                .put("eventKey", eventKey).put("organizerKey", orgKey).put("name", "Forårskoncert"));

        Queue leaf = Queue.<Queue>find("externalReference", "EventKey: " + eventKey).firstResult();
        assertNotNull(leaf);
        assertEquals(QueueLevel.EVENT, leaf.level);
        assertEquals("OrganizerKey: " + orgKey, leaf.parent.externalReference);

        // When: eventet arkiveres — og beskeden gensendes (idempotent)
        consumer.on(new JsonObject().put("type", "event.archived").put("eventKey", eventKey));
        consumer.on(new JsonObject().put("type", "event.archived").put("eventKey", eventKey));

        // Then: bladet er arkiveret, arrangøren stadig aktiv
        assertNotNull(leaf.archivedAt);
        assertNull(leaf.parent.archivedAt);
    }

    @Test
    @TestTransaction
    void givenOrganizerArchivedEvent_whenConsumed_thenWholeSubtreeIsArchived() {
        // Given: arrangør med et blad
        Fixture f = fixture();
        String orgKey = "org-" + UUID.randomUUID();
        consumer.on(new JsonObject().put("type", "organizer.created")
                .put("organizerKey", orgKey).put("name", "Roskilde Events").put("subscription", "PRIORITY"));
        consumer.on(new JsonObject().put("type", "membersystem.created")
                .put("memberSystemKey", "ms-" + UUID.randomUUID())
                .put("organizerKey", orgKey).put("name", "Medlemsklub"));

        // When
        consumer.on(new JsonObject().put("type", "organizer.archived").put("organizerKey", orgKey));

        // Then: arkiveringen cascader til bladet
        Queue organizer = Queue.<Queue>find("externalReference", "OrganizerKey: " + orgKey).firstResult();
        assertNotNull(organizer.archivedAt);
        Queue leaf = Queue.<Queue>find("parent", organizer).firstResult();
        assertNotNull(leaf.archivedAt);
    }

    @Test
    @TestTransaction
    void givenMalformedOrUnknownEvents_whenConsumed_thenTheyAreRejected() {
        // Given
        Fixture f = fixture();

        // When/Then: ukendt type, manglende felt og ukendt nøgle kaster (→ DLQ i drift)
        assertThrows(IllegalArgumentException.class,
                () -> consumer.on(new JsonObject().put("type", "noget.helt.andet")));
        assertThrows(IllegalArgumentException.class,
                () -> consumer.on(new JsonObject().put("type", "organizer.created").put("name", "Uden nøgle")));
        assertThrows(IllegalArgumentException.class,
                () -> consumer.on(new JsonObject().put("type", "organizer.subscription-changed")
                        .put("organizerKey", "findes-ikke-" + UUID.randomUUID())
                        .put("subscription", "PRIORITY")));
        assertThrows(IllegalArgumentException.class,
                () -> consumer.on(new JsonObject().put("type", "organizer.created")
                        .put("organizerKey", "org-" + UUID.randomUUID())
                        .put("name", "Ukendt abo").put("subscription", "platin")));
    }

    @Test
    @TestTransaction
    void givenSubscriptionChangedToNormal_whenConsumed_thenSubtreeDrainsAndKeepsItsCapacities() {
        // Given: priority-arrangør med eget loft og et event-blad med loft
        Fixture f = fixture();
        String orgKey = "org-" + UUID.randomUUID();
        String eventKey = "evt-" + UUID.randomUUID();
        consumer.on(new JsonObject().put("type", "organizer.created")
                .put("organizerKey", orgKey).put("name", "Aalborg Teater").put("subscription", "PRIORITY"));
        consumer.on(new JsonObject().put("type", "event.created")
                .put("eventKey", eventKey).put("organizerKey", orgKey).put("name", "Premiere"));
        Queue organizer = Queue.<Queue>find("externalReference", "OrganizerKey: " + orgKey).firstResult();
        Queue leaf = Queue.<Queue>find("externalReference", "EventKey: " + eventKey).firstResult();
        organizer.maxCapacity = 40;
        leaf.maxCapacity = 20;

        // When: kunden nedgraderes
        consumer.on(new JsonObject().put("type", "organizer.subscription-changed")
                .put("organizerKey", orgKey).put("subscription", "normal"));

        // Then: dræningen cascader til bladet; lofterne bliver stående
        assertEquals(f.priority.id, organizer.parent.id);
        assertNotNull(organizer.drainingAt);
        assertNotNull(leaf.drainingAt, "dræningen cascader nedad");
        assertEquals(40, organizer.maxCapacity, "grenen beholder sine lofter, mens den dræner");
        assertEquals(20, leaf.maxCapacity);
    }
    @Test
    @TestTransaction
    void givenNormalCustomer_whenOrganizerAndEventCreated_thenNoNodesAreBuilt() {
        // Given: en normal kunde deler de generiske køer og får ingen egne knuder
        fixture();
        String orgKey = "org-" + UUID.randomUUID();
        String eventKey = "evt-" + UUID.randomUUID();

        // When: platformen sender de samme events som for enhver anden kunde
        consumer.on(new JsonObject().put("type", "organizer.created")
                .put("organizerKey", orgKey).put("name", "Lille Forening").put("subscription", "NORMAL"));
        consumer.on(new JsonObject().put("type", "event.created")
                .put("eventKey", eventKey).put("organizerKey", orgKey).put("name", "Sommerfest"));

        // Then: no-op, ikke en fejl
        assertEquals(0, Queue.count("externalReference", "OrganizerKey: " + orgKey));
        assertEquals(0, Queue.count("externalReference", "EventKey: " + eventKey));
    }

    // === Fixture ===

    private record Fixture(Queue global, Queue normal, Queue priority) {
    }

    private Fixture fixture() {
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        Queue normal = subscription("fælleskø", global, 50, QueueType.NORMAL);
        Queue priority = subscription("priority", global, null, QueueType.PRIORITY);
        return new Fixture(global, normal, priority);
    }

    private Queue subscription(String name, Queue global, Integer maxCapacity, QueueType type) {
        Queue queue = queueService.createQueue(name, QueueLevel.SUBSCRIPTION, global.uuid, maxCapacity, null);
        queue.queueType = type;
        Queue.getEntityManager().flush();
        return queue;
    }
}
