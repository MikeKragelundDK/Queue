package dk.eventit.queue.service;

import static org.junit.jupiter.api.Assertions.*;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import dk.eventit.queue.entity.ClientType;
import dk.eventit.queue.entity.CloseReason;
import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueSession;
import dk.eventit.queue.entity.QueueSessionState;
import dk.eventit.queue.entity.QueueSessionStateChange;
import dk.eventit.queue.entity.QueueType;
import dk.eventit.queue.messaging.PlatformEventConsumer;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.json.JsonObject;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;

/** Egne committede træer gør transaktions-overlap deterministiske uden sleeps. */
@QuarkusTest
class QueueDrainingAdmissionTest {

    @Inject QueueService queueService;
    @Inject QueueActivationService activationService;
    @Inject QueueDrainingService drainingService;
    @Inject PlatformEventConsumer consumer;
    @Inject EntityManager em;

    private UUID rootUuid;

    @AfterEach
    void cleanUp() {
        if (rootUuid != null) {
            QuarkusTransaction.requiringNew().run(() -> deleteSubtree(Queue.findByUuid(rootUuid)));
        }
    }

    @Test
    void givenDrainingQueue_whenNewVisitorEnqueuesByUuid_thenNoSessionOrTransitionIsCreated() {
        // Given
        Fixture f = fixture();
        downgrade(f);

        // When/Then: både besøgende-indgangen og admin/simulator skal afvises.
        assertThrows(IllegalArgumentException.class,
                () -> queueService.enqueue(f.event(), "ny", context(f)));
        assertThrows(IllegalArgumentException.class, () -> queueService.enqueue(f.event()));
        assertThrows(IllegalArgumentException.class,
                () -> queueService.enqueue(f.event(), null, context(f)));
        assertCounts(f.event(), 0, 0);
    }

    @Test
    void givenExistingSessionInDrainingQueue_whenEnqueueingAgainByUuid_thenSameSessionIsReturned() {
        // Given
        Fixture f = fixture();
        QueueSession first = queueService.enqueue(f.event(), "samme-browser", context(f));
        downgrade(f);

        // When
        QueueSession again = queueService.enqueue(f.event(), "samme-browser", context(f));

        // Then: genopslag er ikke en ny ankomst.
        assertEquals(first.uuid, again.uuid);
        assertEquals(first.sequenceNumber, again.sequenceNumber);
        assertCounts(f.event(), 1, 1);
    }

    @Test
    void givenExistingSessionInDrainingQueue_whenEnqueueingAgainByKey_thenPlaceIsNotMovedToNormal() {
        // Given
        Fixture f = fixture();
        QueueSession first = queueService.enqueueByKey(ClientType.EVENT_BOOKING, f.eventKey(), "samme-browser");
        downgrade(f);

        // When
        QueueSession again = queueService.enqueueByKey(ClientType.EVENT_BOOKING, f.eventKey(), "samme-browser");

        // Then
        assertEquals(first.uuid, again.uuid);
        assertEquals(first.sequenceNumber, again.sequenceNumber);
        assertQueue(again, f.event());
        assertCounts(f.genericEvent(), 0, 0);
    }

    @Test
    void givenDowngradedOrganizer_whenNewVisitorsArrive_thenNormalIsUsedAndOldSessionsFinish() {
        // Given: en eksisterende session venter endnu i prioritetsgrenen.
        Fixture f = fixture();
        QueueSession old = queueService.enqueueByKey(ClientType.EVENT_BOOKING, f.eventKey(), "gammel");
        downgrade(f);

        // When: nye besøgende går til NORMAL, motoren fortsætter med den gamle session.
        QueueSession fresh = queueService.enqueueByKey(ClientType.EVENT_BOOKING, f.eventKey(), "ny");
        activationService.activateNow();

        // Then
        assertQueue(fresh, f.genericEvent());
        QuarkusTransaction.requiringNew().run(() -> {
            assertEquals(QueueSessionState.OPEN, QueueSession.findByUuid(old.uuid).state);
            assertEquals(0, drainingService.archiveIfDrained(f.organizer()), "levende sessioner holder grenen i live");
        });
        queueService.closeSession(old.uuid, CloseReason.COMPLETED);
        QuarkusTransaction.requiringNew().run(() -> {
            assertEquals(1, drainingService.archiveIfDrained(f.organizer()));
            assertNotNull(Queue.findByUuid(f.event()).archivedAt);
            assertEquals(CloseReason.COMPLETED, QueueSession.findByUuid(old.uuid).closeReason);
            assertEquals(QueueSessionState.OPEN, QueueSession.findByUuid(fresh.uuid).state,
                    "arkivering af den gamle gren rører ikke sessioner i NORMAL");
        });
    }

    @Test
    void givenDowngradedOrganizer_whenNewPlatformLeavesArrive_thenNoOwnQueuesAreCreated() {
        // Given
        Fixture f = fixture();
        downgrade(f);
        String eventKey = "nyt-event-" + UUID.randomUUID();
        String memberKey = "nyt-medlemssystem-" + UUID.randomUUID();

        // When: samme kontrakt for begge klienttyper, også ved gensendelse.
        for (int i = 0; i < 2; i++) {
            consumer.on(new JsonObject().put("type", "event.created").put("eventKey", eventKey)
                    .put("organizerKey", f.organizerKey()).put("name", "Nyt event"));
            consumer.on(new JsonObject().put("type", "membersystem.created").put("memberSystemKey", memberKey)
                    .put("organizerKey", f.organizerKey()).put("name", "Nyt medlemssystem"));
        }

        // Then: ingen tomme, drænende prioritetskøer bygges for nye events.
        QuarkusTransaction.requiringNew().run(() -> {
            assertNull(Queue.findActiveByReference(QueueLevel.EVENT, Queue.leafReference(QueueLevel.EVENT, eventKey)));
            assertNull(Queue.findActiveByReference(QueueLevel.MEMBERSYSTEM,
                    Queue.leafReference(QueueLevel.MEMBERSYSTEM, memberKey)));
        });
        assertQueue(queueService.enqueueByKey(ClientType.EVENT_BOOKING, eventKey, "browser"), f.genericEvent());
        assertQueue(queueService.enqueueByKey(ClientType.MEMBER_SIGNUP, memberKey, "browser"), f.genericMember());
    }

    @Test
    void givenDrainingParent_whenQueueIsCreatedDirectly_thenCreationIsRejected() {
        // Given
        Fixture f = fixture();
        downgrade(f);

        // When/Then: admin/service må ikke kunne omgå platform-handlerens no-op.
        assertThrows(IllegalArgumentException.class,
                () -> queueService.createQueue("Nyt event", QueueLevel.EVENT, f.organizer(), null, "forbudt"));
        QuarkusTransaction.requiringNew().run(() ->
                assertEquals(1, Queue.count("parent.uuid", f.organizer())));
    }

    @Test
    void givenStaleQueue_whenDowngradeCommitsBeforeEnqueue_thenSessionAndLogAreRolledBack() {
        // Given
        Fixture f = fixture();

        // When: B beholder en gammel managed kø; A nedgraderer og committer.
        assertThrows(IllegalArgumentException.class, () -> QuarkusTransaction.requiringNew().run(() -> {
            Queue stale = Queue.findByUuid(f.event());
            assertNull(stale.drainingAt);
            downgrade(f);
            assertNull(stale.drainingAt, "testen skal ramme et reelt forældet opslag");
            queueService.enqueue(f.event());
        }));

        // Then: valideringen efter INSERT må ikke efterlade en session eller log.
        assertCounts(f.event(), 0, 0);
        QuarkusTransaction.requiringNew().run(() -> {
            assertNotNull(Queue.findByUuid(f.event()).drainingAt);
            assertFalse(Queue.findByUuid(f.event()).dirty, "også dirty-markeringen skal rulles tilbage");
        });
    }

    @Test
    void givenPriorityRoute_whenDowngradeWinsBeforeInsert_thenFreshAttemptUsesNormal() {
        // Given: placér nedgraderingen præcist efter routing, uden tråde eller sleeps.
        Fixture f = fixture();
        AtomicInteger attempts = new AtomicInteger();
        QueueService controlled = new QueueService() {
            @Override
            protected QueueSession doEnqueue(Queue queue, String visitorKey, String eventContext) {
                if (attempts.incrementAndGet() == 1) {
                    assertEquals(f.event(), queue.uuid);
                    downgrade(f);
                    assertNull(queue.drainingAt, "det første forsøg har stadig sit gamle opslag");
                }
                return super.doEnqueue(queue, visitorKey, eventContext);
            }
        };

        // When
        QueueSession result = controlled.enqueueByKey(ClientType.EVENT_BOOKING, f.eventKey(), "ny");

        // Then: genrouting sker efter rollback, ikke med en halvfærdig session.
        assertEquals(2, attempts.get());
        assertQueue(result, f.genericEvent());
        assertCounts(f.event(), 0, 0);
        assertCounts(f.genericEvent(), 1, 1);
    }

    @Test
    void givenStaleOrganizer_whenPlatformCreatesLeafAfterDowngrade_thenItIsStillANoOp() {
        // Given
        Fixture f = fixture();
        String key = "nyt-event-" + UUID.randomUUID();

        // When: handleren ser en gammel arrangør; servicen skal genbekræfte uden at kaste (DLQ)
        QuarkusTransaction.requiringNew().run(() -> {
            Queue stale = Queue.findByUuid(f.organizer());
            downgrade(f);
            assertNull(stale.drainingAt);
            consumer.on(new JsonObject().put("type", "event.created").put("eventKey", key)
                    .put("organizerKey", f.organizerKey()).put("name", "Nyt event"));
        });

        // Then
        QuarkusTransaction.requiringNew().run(() ->
                assertNull(Queue.findActiveByReference(QueueLevel.EVENT, Queue.leafReference(QueueLevel.EVENT, key))));
    }

    @Test
    void givenOldTreeSnapshot_whenLeafCreationCommitsBeforeDraining_thenNewLeafIsAlsoDrained() {
        // Given
        Fixture f = fixture();

        // When: A har et gammelt snapshot, B tilføjer et blad; A skal bruge det aktuelle træ
        UUID added = QuarkusTransaction.requiringNew().call(() -> {
            Queue organizer = Queue.findByUuid(f.organizer());
            assertEquals(1, Queue.count("parent", organizer));
            UUID leaf = QuarkusTransaction.requiringNew().call(() -> queueService.createQueue(
                    "Ekstra event", QueueLevel.EVENT, f.organizer(), null, "ekstra-" + UUID.randomUUID()).uuid);
            queueService.drainQueue(f.organizer());
            return leaf;
        });

        // Then
        QuarkusTransaction.requiringNew().run(() -> {
            Queue organizer = Queue.findByUuid(f.organizer());
            assertNotNull(organizer.drainingAt);
            assertEquals(organizer.drainingAt, Queue.findByUuid(added).drainingAt);
            assertEquals(organizer.drainingAt, Queue.findByUuid(f.event()).drainingAt);
        });
    }

    @Test
    void givenDirtyQueue_whenTwoEnqueuesOverlap_thenSharedAdmissionLocksDoNotSerializeThem() {
        // Given: en varm kø har allerede dirty=true langs hele stien.
        Fixture f = fixture();
        queueService.enqueue(f.event());

        // When: B skal kunne committe, mens A holder sin FK-lås — en X-lås på stien ville blokere
        QuarkusTransaction.requiringNew().run(() -> {
            queueService.enqueue(f.event());
            QuarkusTransaction.requiringNew().run(() -> queueService.enqueue(f.event()));
        });

        // Then
        assertCounts(f.event(), 3, 3);
    }

    @Test
    void givenLockedUnrelatedBranch_whenDraining_thenTheDrainDoesNotWaitForIt() throws Exception {
        // Given: en anden kundes event holdes eksklusivt låst.
        Fixture f = fixture();
        UUID unrelatedEvent = secondBranch(f);
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Void> holder = CompletableFuture.runAsync(() ->
                QuarkusTransaction.requiringNew().run(() -> {
                    em.refresh(Queue.findByUuid(unrelatedEvent), LockModeType.PESSIMISTIC_WRITE);
                    locked.countDown();
                    awaitQuietly(release);
                }));
        assertTrue(locked.await(30, TimeUnit.SECONDS), "den urelaterede gren blev aldrig låst");

        // When
        CompletableFuture<Void> drain = CompletableFuture.runAsync(() -> queueService.drainQueue(f.organizer()));

        // Then: en scannende dræning ville vente her til lock wait-timeout (50 s i prod)
        try {
            drain.get(15, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            fail("dræningen ventede på en lås i en urelateret gren — den låser bredere end sit eget undertræ");
        } finally {
            release.countDown();
            holder.join();
            drain.handle((ok, error) -> null).join();
        }
        QuarkusTransaction.requiringNew().run(() -> {
            assertNotNull(Queue.findByUuid(f.event()).drainingAt);
            assertNull(Queue.findByUuid(unrelatedEvent).drainingAt, "kun den nedgraderede gren dræner");
        });
    }

    @Test
    void givenDrainingBranch_whenMovingIntoOrOutOfIt_thenBothAreRejected() {
        // Given
        Fixture f = fixture();
        UUID unrelatedEvent = secondBranch(f);
        downgrade(f);

        // When/Then
        IllegalArgumentException ind = assertThrows(IllegalArgumentException.class,
                () -> queueService.moveQueue(unrelatedEvent, f.organizer()));
        assertTrue(ind.getMessage().contains("drænende"), ind.getMessage());

        IllegalArgumentException ud = assertThrows(IllegalArgumentException.class,
                () -> queueService.moveQueue(f.organizer(), f.priority()));
        assertTrue(ud.getMessage().contains("dræner"), ud.getMessage());

        QuarkusTransaction.requiringNew().run(() ->
                assertEquals(f.priority(), Queue.findByUuid(unrelatedEvent).parent.parent.uuid,
                        "intet blev flyttet"));
    }

    private Fixture fixture() {
        Fixture f = QuarkusTransaction.requiringNew().call(() -> {
            String suffix = UUID.randomUUID().toString();
            Queue root = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
            Queue normal = queueService.createQueue("normal", QueueLevel.SUBSCRIPTION, root.uuid, 100, null, QueueType.NORMAL);
            Queue generic = queueService.createQueue("generisk", QueueLevel.ORGANIZER, normal.uuid, null, null);
            Queue event = queueService.createQueue("generisk event", QueueLevel.EVENT, generic.uuid, 100, null);
            Queue member = queueService.createQueue("generisk medlemskø", QueueLevel.MEMBERSYSTEM, generic.uuid, 100, null);
            Queue priority = queueService.createQueue("priority", QueueLevel.SUBSCRIPTION, root.uuid, 100, null, QueueType.PRIORITY);
            String organizerKey = "org-" + suffix;
            String eventKey = "evt-" + suffix;
            Queue organizer = queueService.createQueue("kunde", QueueLevel.ORGANIZER, priority.uuid, 50,
                    Queue.organizerReference(organizerKey));
            Queue customerEvent = queueService.createQueue("kunde-event", QueueLevel.EVENT, organizer.uuid, 50,
                    Queue.leafReference(QueueLevel.EVENT, eventKey));
            return new Fixture(root.uuid, priority.uuid, organizer.uuid, customerEvent.uuid, event.uuid, member.uuid,
                    organizerKey, eventKey);
        });
        rootUuid = f.root();
        return f;
    }

    private void downgrade(Fixture f) {
        QuarkusTransaction.requiringNew().run(() -> consumer.on(new JsonObject()
                .put("type", "organizer.subscription-changed").put("organizerKey", f.organizerKey())
                .put("subscription", "NORMAL")));
    }

    private static String context(Fixture f) {
        return ClientType.EVENT_BOOKING.name() + ":" + f.eventKey();
    }

    private static void assertQueue(QueueSession session, UUID queueUuid) {
        QuarkusTransaction.requiringNew().run(() ->
                assertEquals(queueUuid, QueueSession.findByUuid(session.uuid).queue.uuid));
    }

    private static void assertCounts(UUID queueUuid, long sessions, long transitions) {
        QuarkusTransaction.requiringNew().run(() -> {
            assertEquals(sessions, QueueSession.count("queue.uuid", queueUuid));
            assertEquals(transitions, QueueSessionStateChange.count("session.queue.uuid", queueUuid));
        });
    }

    private static void deleteSubtree(Queue node) {
        for (Queue child : Queue.<Queue>list("parent", node)) {
            deleteSubtree(child);
        }
        QueueSessionStateChange.delete("session.id in (select s.id from QueueSession s where s.queue = ?1)", node);
        QueueSession.delete("queue", node);
        node.delete();
    }

    private record Fixture(UUID root, UUID priority, UUID organizer, UUID event, UUID genericEvent, UUID genericMember,
                           String organizerKey, String eventKey) {
    }

    /** En anden priority-kunde ved siden af fixturens; returnerer dens event. */
    private UUID secondBranch(Fixture f) {
        return QuarkusTransaction.requiringNew().call(() -> {
            String suffix = UUID.randomUUID().toString();
            Queue other = queueService.createQueue("anden kunde", QueueLevel.ORGANIZER, f.priority(), 50,
                    Queue.organizerReference("anden-" + suffix));
            return queueService.createQueue("andet event", QueueLevel.EVENT, other.uuid, 50,
                    Queue.leafReference(QueueLevel.EVENT, "andet-" + suffix)).uuid;
        });
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
