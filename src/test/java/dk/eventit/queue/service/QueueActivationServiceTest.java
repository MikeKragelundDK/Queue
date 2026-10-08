package dk.eventit.queue.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueSession;
import dk.eventit.queue.entity.QueueSessionState;
import dk.eventit.queue.entity.QueueSessionStateChange;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.inject.Inject;

@QuarkusTest
class QueueActivationServiceTest {

    @Inject
    QueueService queueService;

    @Inject
    QueueActivationService activationService;

    /** Scheduleren er slukket i tests — uden denne er claim-trinnet udækket. */
    @Test
    @TestTransaction
    void givenWaitingSession_whenClaimingWork_thenWorkIsClaimedAndDirtyFlagsAreCleared() {
        // Given: en ventende session, hvis enqueue har markeret hele grenen dirty
        Fixture fixture = fixture(10, 10);
        queueService.enqueue(fixture.ownCapEvent.uuid);
        assertTrue(fixture.global.dirty, "enqueue cascader dirty op til roden");

        // When
        QueueActivationService.WorkClaim claim = activationService.claimDirtyWork();

        // Then: der er nogen at kigge på, og flagene er ryddet i samme greb
        assertEquals(QueueActivationService.WorkClaim.CLAIMED, claim);
        assertEquals(0, Queue.count("dirty = true"), "dirty-flagene ryddes ved claim");
    }

    @Test
    @TestTransaction
    void givenOneGlobalSlot_whenTwoSessionsArrive_thenOldestOpensAndSecondQueues() {
        // Given
        Fixture fixture = fixture(1, 10);
        QueueSession oldest = queueService.enqueue(fixture.sharedCapEvent.uuid);
        QueueSession newest = queueService.enqueue(fixture.ownCapEvent.uuid);

        // When
        activationService.activateNow();

        // Then
        assertEquals(QueueSessionState.OPEN, oldest.state, "ældste session vinder det globale slot");
        assertEquals(QueueSessionState.QUEUED, newest.state);
        assertNotNull(oldest.openedAt);
        assertEquals(List.of(QueueSessionState.INITIAL, QueueSessionState.QUEUED, QueueSessionState.OPEN),
                states(oldest),
                "QUEUED skrives altid — også ved ledig kapacitet — så ventetiden (OPEN − QUEUED) "
                        + "også findes for de hurtige indlukninger");
        assertEquals(List.of(QueueSessionState.INITIAL, QueueSessionState.QUEUED), states(newest));
    }

    @Test
    @TestTransaction
    void givenFullBranchButFreeGlobalCapacity_whenNewerSessionArrivesElsewhere_thenBlockedBranchDoesNotHoldItBack() {
        // Given: grenen med delt loft har ét slot, global har ti.
        Fixture fixture = fixture(10, 1);
        QueueSession sharedCapOccupant = queueService.enqueue(fixture.sharedCapEvent.uuid);
        activationService.activateNow();
        assertEquals(QueueSessionState.OPEN, sharedCapOccupant.state);

        QueueSession olderBlockedShared = queueService.enqueue(fixture.sharedCapEvent.uuid);
        QueueSession newerOwnCap = queueService.enqueue(fixture.ownCapEvent.uuid);
        assertTrue(olderBlockedShared.sequenceNumber < newerOwnCap.sequenceNumber);

        // When
        activationService.activateNow();

        // Then: global FIFO må ikke skabe head-of-line blocking på tværs af isolerede grene.
        assertEquals(QueueSessionState.QUEUED, olderBlockedShared.state);
        assertEquals(QueueSessionState.OPEN, newerOwnCap.state);
    }

    @Test
    @TestTransaction
    void givenMoreThanOnePageOfBlockedSessions_whenOtherBranchArrives_thenBlockedBranchIsNotScanned() {
        // Given
        Fixture fixture = fixture(1_000, 1);
        queueService.enqueue(fixture.sharedCapEvent.uuid);
        activationService.activateNow();
        for (int i = 0; i < 600; i++) {
            queueService.enqueue(fixture.sharedCapEvent.uuid);
        }
        QueueSession ownCap = queueService.enqueue(fixture.ownCapEvent.uuid);

        // When
        QueueActivationService.ActivationResult result = activationService.activateNow();

        // Then
        assertEquals(QueueSessionState.OPEN, ownCap.state);
        assertEquals(1, result.scanned(), "den fyldte gren filtreres fra kandidat-queryen");
    }

    @Test
    @TestTransaction
    void givenMoreAdmissionsThanOneBatch_whenActivationRunsTwice_thenWorkContinuesInNextTransaction() {
        // Given
        Fixture fixture = fixture(1_000, 1_000);
        for (int i = 0; i < 501; i++) {
            queueService.enqueue(fixture.ownCapEvent.uuid);
        }

        // When
        QueueActivationService.ActivationResult first = activationService.activateNow();
        QueueActivationService.ActivationResult second = activationService.activateNow();

        // Then
        assertEquals(500, first.opened());
        assertTrue(first.batchFull());
        assertEquals(1, second.opened());
        assertEquals(501, QueueSession.count("queue = ?1 and state = ?2",
                fixture.ownCapEvent, QueueSessionState.OPEN));
    }

    @Test
    @TestTransaction
    void givenWaitingSessions_whenBacklogIsMeasured_thenCurrentCountAndAgeAreReturned() {
        // Given
        long waitingBefore = activationService.backlogSnapshot().waitingSessions();
        Fixture fixture = fixture(10, 10);
        queueService.enqueue(fixture.sharedCapEvent.uuid);
        queueService.enqueue(fixture.ownCapEvent.uuid);

        // When
        QueueActivationService.BacklogSnapshot snapshot = activationService.backlogSnapshot();

        // Then
        assertEquals(waitingBefore + 2, snapshot.waitingSessions());
        assertTrue(snapshot.oldestWaitSeconds() >= 0);
    }

    @Test
    @TestTransaction
    void givenQueuedSession_whenSharedCapacityIsRaised_thenItOpensOnNextRun() {
        // Given
        Fixture fixture = fixture(10, 1);
        QueueSession occupant = queueService.enqueue(fixture.sharedCapEvent.uuid);
        QueueSession waiting = queueService.enqueue(fixture.sharedCapEvent.uuid);
        activationService.activateNow();
        assertEquals(QueueSessionState.OPEN, occupant.state);
        assertEquals(QueueSessionState.QUEUED, waiting.state);

        // When
        queueService.updateMaxCapacity(fixture.sharedCap.uuid, 2);
        activationService.activateNow();

        // Then
        assertEquals(QueueSessionState.OPEN, waiting.state);
        assertNotNull(waiting.openedAt);
    }

    @Test
    @TestTransaction
    void givenOpenSessions_whenCapacityIsLowered_thenExistingSessionsStayOpenAndNoNewSessionOpens() {
        // Given
        Fixture fixture = fixture(10, 2);
        QueueSession first = queueService.enqueue(fixture.sharedCapEvent.uuid);
        QueueSession second = queueService.enqueue(fixture.sharedCapEvent.uuid);
        activationService.activateNow();
        assertEquals(QueueSessionState.OPEN, first.state);
        assertEquals(QueueSessionState.OPEN, second.state);

        // When
        queueService.updateMaxCapacity(fixture.sharedCap.uuid, 1);
        QueueSession third = queueService.enqueue(fixture.sharedCapEvent.uuid);
        activationService.activateNow();

        // Then: et lavere loft evikter ikke brugere; det bremser kun nye indlukninger.
        assertEquals(QueueSessionState.OPEN, first.state);
        assertEquals(QueueSessionState.OPEN, second.state);
        assertEquals(QueueSessionState.QUEUED, third.state);
    }

    /** Stier kortere end fire niveauer giver null i forfader-projektionen. */
    @Test
    @TestTransaction
    void givenSessionOnSubscriptionQueue_whenActivationRuns_thenShortPathCountsAgainstSharedCapacity() {
        // Given: en session direkte på abonnementet — stien er kun to knuder lang
        Fixture fixture = fixture(10, 1);
        QueueSession onSubscription = queueService.enqueue(fixture.sharedCap.uuid);
        activationService.activateNow();
        assertEquals(QueueSessionState.OPEN, onSubscription.state);

        // When: en session på et blad under samme abonnement, hvis loft nu er brugt
        QueueSession onEvent = queueService.enqueue(fixture.sharedCapEvent.uuid);
        activationService.activateNow();

        // Then: den korte sti talte med mod abonnementets loft
        assertEquals(QueueSessionState.QUEUED, onEvent.state);
    }

    @Test
    @TestTransaction
    void givenClosedOccupant_whenActivationRunsAgain_thenOldestQueuedSessionTakesTheSlot() {
        // Given
        Fixture fixture = fixture(1, 10);
        QueueSession occupant = queueService.enqueue(fixture.ownCapEvent.uuid);
        QueueSession waiting = queueService.enqueue(fixture.ownCapEvent.uuid);
        activationService.activateNow();
        assertEquals(QueueSessionState.OPEN, occupant.state);
        assertEquals(QueueSessionState.QUEUED, waiting.state);

        // When
        queueService.closeSession(occupant.uuid, dk.eventit.queue.entity.CloseReason.COMPLETED);
        activationService.activateNow();

        // Then
        assertEquals(QueueSessionState.CLOSED, occupant.state);
        assertEquals(QueueSessionState.OPEN, waiting.state);
    }

    @Test
    @TestTransaction
    void givenNoNewChanges_whenActivationRunsTwice_thenRepeatedTickIsIdempotent() {
        // Given
        Fixture fixture = fixture(2, 10);
        QueueSession first = queueService.enqueue(fixture.ownCapEvent.uuid);
        QueueSession second = queueService.enqueue(fixture.ownCapEvent.uuid);
        QueueSession third = queueService.enqueue(fixture.ownCapEvent.uuid);

        // When
        activationService.activateNow();
        activationService.activateNow();

        // Then
        assertEquals(QueueSessionState.OPEN, first.state);
        assertEquals(QueueSessionState.OPEN, second.state);
        assertEquals(QueueSessionState.QUEUED, third.state);
        assertEquals(2, QueueSession.count("queue = ?1 and state = ?2",
                fixture.ownCapEvent, QueueSessionState.OPEN));
    }

    /** Motoren filtrerer utrukne køer to steder — derfor både venteværelse og almindelig kø. */
    @Test
    @TestTransaction
    void givenUndrawnPreQueueBesideOrdinaryQueue_whenActivationRuns_thenOnlyTheOrdinaryQueueAdvances() {
        // Given: utrukket venteværelse; den anden gren har loft 0 og skal gennem den globale scanning
        Fixture fixture = fixture(10, 0);
        fixture.ownCapEvent.salesStartAt = Instant.now().plusSeconds(1000);

        QueueSession blockedOrdinary = queueService.enqueue(fixture.sharedCapEvent.uuid);
        QueueSession inWaitingRoom = queueService.enqueue(fixture.ownCapEvent.uuid);

        // When
        activationService.activateNow();

        // Then: venteværelset røres ikke, den almindelige kø fungerer som før
        assertEquals(QueueSessionState.INITIAL, inWaitingRoom.state,
                "utrukket venteværelse må hverken lukkes ind eller flyttes til QUEUED");
        assertEquals(QueueSessionState.QUEUED, blockedOrdinary.state,
                "en blokeret session i en almindelig kø skal stadig nå QUEUED");
    }

    @Test
    @TestTransaction
    void givenDrawnPreQueue_whenActivationRuns_thenItBehavesLikeAnyQueue() {
        // Given: salget er åbnet, og lodtrækningen har kørt
        Fixture fixture = fixture(10, 10);
        Instant inThePast = Instant.now().minusSeconds(1000);
        fixture.ownCapEvent.salesStartAt = inThePast;
        fixture.ownCapEvent.drawnAt = inThePast;

        QueueSession session = queueService.enqueue(fixture.ownCapEvent.uuid);

        // When
        activationService.activateNow();

        // Then
        assertEquals(QueueSessionState.OPEN, session.state, "en trukket kø lukker ind som enhver anden");
        assertEquals(List.of(QueueSessionState.INITIAL, QueueSessionState.QUEUED, QueueSessionState.OPEN),
                states(session),
                "QUEUED skrives også her, så ventetiden findes for lodtrækningens indlukninger");
    }

    /**
     * Overdragelsen er {@code drawnAt}, ikke {@code salesStartAt <= now}: ellers
     * lukker motoren ind i ankomstorden, før numrene er blandet.
     */
    @Test
    @TestTransaction
    void givenPreQueueAfterSaleStartButBeforeDraw_whenActivationRuns_thenSessionsStayUntouched() {
        // Given: salgsstart er passeret, men lodtrækningen har ikke kørt endnu
        Fixture fixture = fixture(10, 0);
        fixture.ownCapEvent.salesStartAt = Instant.now().minusSeconds(1000);
        fixture.ownCapEvent.drawnAt = null;

        QueueSession session = queueService.enqueue(fixture.ownCapEvent.uuid);

        // When
        activationService.activateNow();

        // Then
        assertEquals(QueueSessionState.INITIAL, session.state,
                "en passeret salgsstart giver ikke adgang — kun en gennemført lodtrækning gør");
    }

    @Test
    void givenTwoConcurrentActivationTransactions_whenCapacityIsTwo_thenTheyNeverOpenThreeSessions()
            throws Exception {
        // Committet, så begge tråde ser samme snapshot — derfor oprydning bagefter.
        Fixture committed = QuarkusTransaction.requiringNew().call(() -> fixture(2, 10));
        UUID rootUuid = committed.global().uuid;
        UUID eventUuid = committed.ownCapEvent().uuid;
        try {
            List<UUID> sessionUuids = QuarkusTransaction.requiringNew().call(() -> List.of(
                    queueService.enqueue(eventUuid).uuid,
                    queueService.enqueue(eventUuid).uuid,
                    queueService.enqueue(eventUuid).uuid));

            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                Future<?> firstPod = executor.submit(() -> runActivationTogether(ready, start));
                Future<?> secondPod = executor.submit(() -> runActivationTogether(ready, start));
                assertTrue(ready.await(5, TimeUnit.SECONDS), "begge simulerede pods skal være klar");

                // When
                start.countDown();
                firstPod.get(10, TimeUnit.SECONDS);
                secondPod.get(10, TimeUnit.SECONDS);
            } finally {
                executor.shutdownNow();
            }

            // Then: singleton-rækkelåsen serialiserer snapshots og transitions.
            QuarkusTransaction.requiringNew().run(() -> {
                long open = sessionUuids.stream()
                        .map(QueueSession::findByUuid)
                        .filter(session -> session.state == QueueSessionState.OPEN)
                        .count();
                long queued = sessionUuids.stream()
                        .map(QueueSession::findByUuid)
                        .filter(session -> session.state == QueueSessionState.QUEUED)
                        .count();
                assertEquals(2, open);
                assertEquals(1, queued);
            });
        } finally {
            deleteCommittedTree(rootUuid);
        }
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

    private void runActivationTogether(CountDownLatch ready, CountDownLatch start) {
        try {
            ready.countDown();
            if (!start.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Samtidighedstestens start-signal udeblev");
            }
            activationService.activateNow();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Samtidighedstesten blev afbrudt", e);
        }
    }

    private List<QueueSessionState> states(QueueSession session) {
        return QueueSessionStateChange.<QueueSessionStateChange>list("session = ?1 order by id", session)
                .stream().map(change -> change.state).toList();
    }

    private Fixture fixture(int globalCapacity, int subscriptionCapacity) {
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, globalCapacity, null);
        Queue sharedCap = queueService.createQueue("abonnement-delt-loft", QueueLevel.SUBSCRIPTION, global.uuid, subscriptionCapacity, null);
        Queue sharedCapOrganizer = queueService.createQueue("delt-arrangør", QueueLevel.ORGANIZER, sharedCap.uuid,
                null, null);
        Queue sharedCapEvent = queueService.createQueue("delt-event", QueueLevel.EVENT, sharedCapOrganizer.uuid,
                null, null);

        Queue ownCap = queueService.createQueue("abonnement-eget-loft", QueueLevel.SUBSCRIPTION, global.uuid, null, null);
        Queue ownCapOrganizer = queueService.createQueue("egen-arrangør", QueueLevel.ORGANIZER, ownCap.uuid,
                null, null);
        Queue ownCapEvent = queueService.createQueue("egen-event", QueueLevel.EVENT, ownCapOrganizer.uuid,
                null, null);
        return new Fixture(global, sharedCap, sharedCapEvent, ownCap, ownCapEvent);
    }

    private record Fixture(Queue global, Queue sharedCap, Queue sharedCapEvent, Queue ownCap, Queue ownCapEvent) {
    }
}
