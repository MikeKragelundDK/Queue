package dk.eventit.queue.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import dk.eventit.queue.entity.QueueType;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;

@QuarkusTest
class QueueServiceTest {

    @Inject
    QueueService queueService;

    @Test
    @TestTransaction
    void givenLeafQueue_whenEnqueueing_thenSessionIsInitialWithIdAsSequenceAndBranchIsDirty() {
        // Given
        Tree tree = persistTree();

        // When
        QueueSession session = queueService.enqueue(tree.event.uuid);

        // Then
        assertEquals(QueueSessionState.INITIAL, session.state);
        assertEquals(session.id, session.sequenceNumber, "FIFO-nummeret afledes af id");
        assertNotNull(session.uuid);
        assertEquals(tree.event.uuid, session.queue.uuid);

        // Dirty opad hele grenen ...
        assertTrue(tree.event.dirty, "bladet skal være dirty");
        assertTrue(tree.organizer.dirty, "arrangøren skal være dirty");
        assertTrue(tree.subscription.dirty, "abonnementet skal være dirty");
        assertTrue(tree.global.dirty, "roden skal være dirty");
        // ... men aldrig til søskende
        assertFalse(tree.siblingSubscription.dirty, "søskende-abonnement er upåvirket");
    }

    @Test
    @TestTransaction
    void givenSessionsInQueue_whenEnqueueingMore_thenSequenceNumbersAreStrictlyIncreasing() {
        // Given
        Tree tree = persistTree();

        // When
        QueueSession first = queueService.enqueue(tree.event.uuid);
        QueueSession second = queueService.enqueue(tree.event.uuid);
        QueueSession third = queueService.enqueue(tree.event.uuid);

        // Then: monoton, huller er tilladt
        assertEquals(first.id, first.sequenceNumber);
        assertEquals(second.id, second.sequenceNumber);
        assertEquals(third.id, third.sequenceNumber);
        assertTrue(first.sequenceNumber < second.sequenceNumber, "rækkefølgen skal være monoton");
        assertTrue(second.sequenceNumber < third.sequenceNumber, "rækkefølgen skal være monoton");
    }

    @Test
    @TestTransaction
    void givenInitialSession_whenClosing_thenStateIsClosedWithTimestampAndBranchIsDirtyAgain() {
        // Given
        Tree tree = persistTree();
        QueueSession session = queueService.enqueue(tree.event.uuid);
        markWholeTreeClean(tree);

        // When
        QueueSession closed = queueService.closeSession(session.uuid, CloseReason.COMPLETED);

        // Then
        assertEquals(QueueSessionState.CLOSED, closed.state);
        assertNotNull(closed.closedAt);
        assertEquals(CloseReason.COMPLETED, closed.closeReason);
        assertTrue(tree.event.dirty, "close skal markere grenen dirty igen");
        assertTrue(tree.global.dirty, "close skal cascade helt op til roden");
        assertFalse(tree.siblingSubscription.dirty, "søskende-abonnement er upåvirket");
    }

    @Test
    @TestTransaction
    void givenClosedSession_whenClosingAgain_thenNothingChanges() {
        // Given
        Tree tree = persistTree();
        QueueSession session = queueService.enqueue(tree.event.uuid);
        queueService.closeSession(session.uuid, CloseReason.COMPLETED);
        // Databasen kan afrunde mikrosekunder
        QueueSession.getEntityManager().refresh(session);
        Instant firstClosedAt = session.closedAt;
        markWholeTreeClean(tree);

        // When
        QueueSession closedAgain = queueService.closeSession(session.uuid, CloseReason.ABANDONED);

        // Then: idempotent
        assertEquals(QueueSessionState.CLOSED, closedAgain.state);
        assertEquals(firstClosedAt, closedAgain.closedAt);
        assertEquals(CloseReason.COMPLETED, closedAgain.closeReason, "første årsag vinder");
        assertFalse(tree.event.dirty, "gen-luk må ikke markere dirty igen");
    }

    @Test
    void givenStaleOpenSession_whenAnotherTransactionClosesFirst_thenFirstClosureWins() {
        // Given
        Tree tree = QuarkusTransaction.requiringNew().call(this::persistTree);
        try {
            UUID sessionUuid = QuarkusTransaction.requiringNew().call(() -> {
                QueueSession session = queueService.enqueue(tree.event.uuid);
                Instant now = Instant.now();
                queueService.transitionToQueued(session, now);
                queueService.transitionToOpen(session, now);
                return session.uuid;
            });

            // When
            Instant firstClosedAt = QuarkusTransaction.requiringNew().call(() -> {
                QueueSession staleSession = QueueSession.findByUuid(sessionUuid);
                assertEquals(QueueSessionState.OPEN, staleSession.state);

                Instant committedClosedAt = QuarkusTransaction.requiringNew().call(() -> {
                    QueueSession closed = queueService.closeSession(sessionUuid, CloseReason.COMPLETED);
                    QueueSession.getEntityManager().refresh(closed);
                    return closed.closedAt;
                });

                assertEquals(QueueSessionState.OPEN, staleSession.state,
                        "B skal stadig have det forældede opslag, når den forsøger at lukke");
                QueueSession closedAgain = queueService.closeSession(sessionUuid, CloseReason.ADMIN);

                // Then
                assertEquals(QueueSessionState.CLOSED, closedAgain.state);
                assertEquals(CloseReason.COMPLETED, closedAgain.closeReason, "første årsag vinder");
                assertEquals(committedClosedAt, closedAgain.closedAt, "første lukketidspunkt bevares");
                return committedClosedAt;
            });

            QuarkusTransaction.requiringNew().run(() -> {
                QueueSession persisted = QueueSession.findByUuid(sessionUuid);
                assertEquals(QueueSessionState.CLOSED, persisted.state);
                assertEquals(CloseReason.COMPLETED, persisted.closeReason);
                assertEquals(firstClosedAt, persisted.closedAt);

                List<QueueSessionStateChange> closures = QueueSessionStateChange.list(
                        "session = ?1 and state = ?2", persisted, QueueSessionState.CLOSED);
                assertEquals(1, closures.size(), "overlappende lukninger må kun logge én CLOSED-transition");
                assertEquals(CloseReason.COMPLETED, closures.getFirst().reason);
            });
        } finally {
            deleteCommittedTree(tree.global.uuid);
        }
    }

    @Test
    @TestTransaction
    void givenNoParent_whenCreatingSubscription_thenCreationFails() {
        assertThrows(IllegalArgumentException.class,
                () -> queueService.createQueue("abonnement", QueueLevel.SUBSCRIPTION, null, 50, null));
    }

    @Test
    @TestTransaction
    void givenWrongParentLevel_whenCreatingEvent_thenCreationFails() {
        // Given
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        Queue subscription = queueService.createQueue("abonnement", QueueLevel.SUBSCRIPTION, global.uuid, 50, null);

        // When/Then
        assertThrows(IllegalArgumentException.class,
                () -> queueService.createQueue("event", QueueLevel.EVENT, subscription.uuid, null, null));
    }

    @Test
    @TestTransaction
    void givenGlobalRoot_whenCreatingFullBranch_thenAllNodesArePersistedWithCorrectStructure() {
        // When
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        Queue subscription = queueService.createQueue("abonnement-uden-loft", QueueLevel.SUBSCRIPTION, global.uuid, null, null);
        Queue organizer = queueService.createQueue("arrangør", QueueLevel.ORGANIZER, subscription.uuid, 400,
                "OrganizerKey: demo");
        Queue event = queueService.createQueue("event", QueueLevel.EVENT, organizer.uuid, 100, "EventKey: demo");

        // Then
        assertEquals(organizer.id, event.parent.id);
        assertEquals(400, event.parent.maxCapacity);
        assertEquals("OrganizerKey: demo", event.parent.externalReference);
        assertNull(global.parent);
        assertFalse(event.dirty, "en nyoprettet kø uden sessioner er ikke dirty");
    }

    @Test
    @TestTransaction
    void givenUnknownQueue_whenEnqueueing_thenItFails() {
        assertThrows(IllegalArgumentException.class, () -> queueService.enqueue(UUID.randomUUID()));
    }

    @Test
    @TestTransaction
    void givenActiveQueue_whenUpdatingCapacity_thenNewLimitIsStoredAndBranchIsDirty() {
        // Given
        Tree tree = persistTree();
        markWholeTreeClean(tree);

        // When
        Queue updated = queueService.updateMaxCapacity(tree.event.uuid, 17);

        // Then
        assertEquals(17, updated.maxCapacity);
        assertTrue(tree.event.dirty, "det ændrede blad skal genberegnes");
        assertTrue(tree.global.dirty, "kapacitetsændringen skal markere hele grenen dirty");
        assertFalse(tree.siblingSubscription.dirty, "søskende-grene er upåvirkede");
    }

    @Test
    @TestTransaction
    void givenQueue_whenUpdatingCapacityToUnlimitedOrNegative_thenNullIsAllowedAndNegativeIsRejected() {
        // Given
        Tree tree = persistTree();

        // When/Then
        assertNull(queueService.updateMaxCapacity(tree.subscription.uuid, null).maxCapacity,
                "null betyder intet loft på dette niveau");
        assertThrows(IllegalArgumentException.class,
                () -> queueService.updateMaxCapacity(tree.subscription.uuid, -1));
    }

    @Test
    @TestTransaction
    void givenSession_whenEnqueuedAndClosed_thenStateChangesAreLoggedInSameTransaction() {
        // Given
        Tree tree = persistTree();

        // When
        QueueSession session = queueService.enqueue(tree.event.uuid);
        queueService.closeSession(session.uuid, CloseReason.COMPLETED);
        queueService.closeSession(session.uuid, CloseReason.ADMIN); // idempotent — må ikke logge dobbelt

        // Then
        List<QueueSessionStateChange> changes = QueueSessionStateChange
                .list("session = ?1 order by id", session);
        assertEquals(2, changes.size());
        assertEquals(QueueSessionState.INITIAL, changes.get(0).state);
        assertNull(changes.get(0).reason, "kun CLOSED har en årsag");
        assertEquals(QueueSessionState.CLOSED, changes.get(1).state);
        assertEquals(CloseReason.COMPLETED, changes.get(1).reason);
        assertEquals(CloseReason.COMPLETED, session.closeReason, "første lukning vinder — ADMIN overskriver ikke");
        assertNotNull(changes.get(0).createdAt, "createdAt er tidspunktet, tilstanden indtrådte");
    }

    @Test
    @TestTransaction
    void givenBranchWithOpenSessions_whenArchivingOrganizer_thenSubtreeIsArchivedAndSessionsClosed() {
        // Given
        Tree tree = persistTree();
        QueueSession session = queueService.enqueue(tree.event.uuid);
        markWholeTreeClean(tree);

        // When
        queueService.archiveQueue(tree.organizer.uuid);

        // Then
        assertNotNull(tree.organizer.archivedAt, "arrangøren skal være arkiveret");
        assertNotNull(tree.event.archivedAt, "arkivering cascader nedad til eventet");
        assertNull(tree.subscription.archivedAt, "arkivering cascader aldrig opad");
        assertEquals(QueueSessionState.CLOSED, session.state, "åbne sessioner lukkes ved arkivering");
        assertEquals(CloseReason.ARCHIVED, session.closeReason, "arkivering registreres som årsag");
        assertNotNull(session.closedAt);
        assertTrue(tree.global.dirty, "frigivet kapacitet skal genberegnes");
        assertNotNull(QueueSession.findByUuid(session.uuid), "sessionen bevares til statistik");
    }

    @Test
    @TestTransaction
    void givenArchivedQueue_whenEnqueueingOrCreatingChild_thenBothAreRejected() {
        // Given
        Tree tree = persistTree();
        queueService.archiveQueue(tree.organizer.uuid);

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> queueService.enqueue(tree.event.uuid),
                "arkiverede køer modtager ikke sessioner");
        assertThrows(IllegalArgumentException.class,
                () -> queueService.createQueue("nyt-event", QueueLevel.EVENT, tree.organizer.uuid, null, null),
                "der kan ikke oprettes køer under en arkiveret forælder");
    }

    @Test
    @TestTransaction
    void givenSeparatelyArchivedEvent_whenUnarchivingOrganizer_thenEventStaysArchived() {
        // Given: eventet arkiveres særskilt før arrangøren
        Tree tree = persistTree();
        queueService.archiveQueue(tree.event.uuid);
        queueService.archiveQueue(tree.organizer.uuid);

        // When
        queueService.unarchiveQueue(tree.organizer.uuid);

        // Then: kun samme ombæring genaktiveres
        assertNull(tree.organizer.archivedAt, "arrangøren er genaktiveret");
        assertNotNull(tree.event.archivedAt, "det særskilt arkiverede event forbliver arkiveret");
    }

    @Test
    @TestTransaction
    void givenActiveQueue_whenDeleting_thenDeletionIsRejected() {
        // Given
        Tree tree = persistTree();

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> queueService.deleteQueue(tree.event.uuid));
        assertNotNull(Queue.findByUuid(tree.event.uuid));
    }

    @Test
    @TestTransaction
    void givenArchivedLeafWithSessions_whenDeleting_thenQueueSessionsAndHistoryAreGone() {
        // Given
        Tree tree = persistTree();
        QueueSession session = queueService.enqueue(tree.event.uuid);
        queueService.archiveQueue(tree.event.uuid);
        markWholeTreeClean(tree);

        // When
        queueService.deleteQueue(tree.event.uuid);

        // Then: genindlæs — deleteQueue clearer persistence context
        assertNull(Queue.findByUuid(tree.event.uuid), "køen skal være slettet");
        assertNull(QueueSession.findByUuid(session.uuid), "køens sessioner skal slettes med");
        assertEquals(0, QueueSessionStateChange.count("session.id = ?1", session.id),
                "transitionsloggen skal slettes med");
        assertTrue(Queue.findByUuid(tree.organizer.uuid).dirty, "grenen skal være dirty");
    }

    @Test
    @TestTransaction
    void givenQueueWithChildren_whenDeleting_thenDeletionFails() {
        // Given
        Tree tree = persistTree();
        queueService.archiveQueue(tree.organizer.uuid);

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> queueService.deleteQueue(tree.organizer.uuid));
        assertNotNull(Queue.findByUuid(tree.organizer.uuid), "køen må ikke være slettet");
    }

    @Test
    @TestTransaction
    void givenLeafQueue_whenSettingSalesStart_thenItBecomesAPreQueueAndPastTimesAreAllowed() {
        // Given
        Tree tree = persistTree();
        Instant salgsstart = Instant.now().plusSeconds(7200);

        // When
        Queue updated = queueService.updateSchedule(tree.event.uuid, null, salgsstart);

        // Then
        assertEquals(salgsstart, updated.salesStartAt);
        assertNull(updated.drawnAt, "en salgsstart alene trækker ingenting");

        // When/Then: fortiden er tilladt — lodtrækningen tager den ved næste tick
        Instant iForvejenPasseret = Instant.now().minusSeconds(300);
        assertEquals(iForvejenPasseret,
                queueService.updateSchedule(tree.event.uuid, null, iForvejenPasseret).salesStartAt);
    }

    @Test
    @TestTransaction
    void givenNonLeafQueue_whenSettingSalesStart_thenItIsRejected() {
        // Given
        Tree tree = persistTree();
        Instant salgsstart = Instant.now().plusSeconds(3600);

        // When/Then
        assertThrows(IllegalArgumentException.class,
                () -> queueService.updateSchedule(tree.subscription.uuid, null, salgsstart));
        assertThrows(IllegalArgumentException.class,
                () -> queueService.updateSchedule(tree.organizer.uuid, null, salgsstart));
        assertNull(tree.organizer.salesStartAt, "afvisningen må ikke have sat feltet undervejs");
    }

    @Test
    @TestTransaction
    void givenDrawnQueue_whenChangingSalesStart_thenItIsRejectedAndTheOldValueStands() {
        // Given
        Tree tree = persistTree();
        // DATETIME(6) bevarer mikrosekunder ved servicekaldets genlæsning.
        Instant salgsstart = Instant.now().minusSeconds(600).truncatedTo(ChronoUnit.MICROS);
        queueService.updateSchedule(tree.event.uuid, null, salgsstart);
        tree.event.drawnAt = Instant.now();
        Queue.getEntityManager().flush();

        // When/Then: numrene er delt ud
        assertThrows(IllegalArgumentException.class,
                () -> queueService.updateSchedule(tree.event.uuid, null, Instant.now().plusSeconds(3600)));
        assertThrows(IllegalArgumentException.class,
                () -> queueService.updateSchedule(tree.event.uuid, null, null));
        assertEquals(salgsstart, tree.event.salesStartAt, "salgsstarten står, som den gjorde");
    }

    @Test
    @TestTransaction
    void givenPreQueueWithoutClaimedDrawSeed_whenClearingSchedule_thenItBecomesAnOrdinaryQueue() {
        // Given
        Tree tree = persistTree();
        Instant salgsstart = Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.MICROS);
        queueService.updateSchedule(tree.event.uuid, salgsstart.minusSeconds(600), salgsstart);
        Queue.getEntityManager().flush();
        assertNull(tree.event.drawSeed, "lodtrækningen er endnu ikke startet");

        // When
        Queue cleared = queueService.updateSchedule(tree.event.uuid, null, null);
        Queue.getEntityManager().flush();
        Queue.getEntityManager().refresh(cleared);

        // Then
        assertNull(cleared.opensAt);
        assertNull(cleared.salesStartAt);
        assertNull(cleared.drawSeed);
        assertNull(cleared.drawnAt);
    }

    @Test
    @TestTransaction
    void givenPreQueueWithClaimedDrawSeed_whenClearingSchedule_thenItIsRejectedAndScheduleIsPreserved() {
        // Given: seed claimet, trækningen ufærdig — en rydning ville slippe motoren løs
        Tree tree = persistTree();
        Instant salgsstart = Instant.now().minusSeconds(60).truncatedTo(ChronoUnit.MICROS);
        Instant aabner = salgsstart.minusSeconds(600);
        String seed = "ab".repeat(32);
        queueService.updateSchedule(tree.event.uuid, aabner, salgsstart);
        tree.event.drawSeed = seed;
        Queue.getEntityManager().flush();

        // When
        IllegalArgumentException fejl = assertThrows(IllegalArgumentException.class,
                () -> queueService.updateSchedule(tree.event.uuid, null, null));
        Queue.getEntityManager().refresh(tree.event);

        // Then: grundlaget for at genoptage er bevaret
        assertTrue(fejl.getMessage().contains("er startet"));
        assertEquals(aabner, tree.event.opensAt);
        assertEquals(salgsstart, tree.event.salesStartAt);
        assertEquals(seed, tree.event.drawSeed);
        assertNull(tree.event.drawnAt, "testen handler om en igangværende, ikke en færdig lodtrækning");
    }

    @Test
    @TestTransaction
    void givenQueueWithAdmittedSessions_whenSettingSalesStart_thenItIsRejectedUntilTheyAreGone() {
        // Given: én venter, én er lukket ind
        Tree tree = persistTree();
        queueService.enqueue(tree.event.uuid);
        QueueSession admitted = queueService.enqueue(tree.event.uuid);
        admitted.state = QueueSessionState.OPEN;
        Instant salgsstart = Instant.now().plusSeconds(3600);

        // When/Then: trækningen ville genbruge de indlukkedes numre
        assertThrows(IllegalArgumentException.class,
                () -> queueService.updateSchedule(tree.event.uuid, null, salgsstart));
        assertNull(tree.event.salesStartAt);

        // When/Then
        admitted.state = QueueSessionState.CLOSED;
        assertEquals(salgsstart, queueService.updateSchedule(tree.event.uuid, null, salgsstart).salesStartAt);
    }

    @Test
    @TestTransaction
    void givenArchivedQueue_whenSettingSalesStart_thenItIsRejected() {
        // Given
        Tree tree = persistTree();
        queueService.archiveQueue(tree.event.uuid);

        // When/Then
        assertThrows(IllegalArgumentException.class,
                () -> queueService.updateSchedule(tree.event.uuid, null, Instant.now().plusSeconds(3600)));
        assertNull(tree.event.salesStartAt);
    }

    @Test
    @TestTransaction
    void givenSalesStart_whenSettingWindowThatOpensBefore_thenBothTimesAreStored() {
        // Given
        Tree tree = persistTree();
        Instant salgsstart = Instant.now().plusSeconds(7200);
        Instant aabner = salgsstart.minusSeconds(3600);

        // When
        Queue updated = queueService.updateSchedule(tree.event.uuid, aabner, salgsstart);

        // Then
        assertEquals(aabner, updated.opensAt);
        assertEquals(salgsstart, updated.salesStartAt);
    }

    @Test
    @TestTransaction
    void givenNoSalesStart_whenSettingWindowAlone_thenItIsRejected() {
        // Given
        Tree tree = persistTree();

        // When/Then
        assertThrows(IllegalArgumentException.class,
                () -> queueService.updateSchedule(tree.event.uuid, Instant.now().plusSeconds(60), null));
        assertNull(tree.event.opensAt);
    }

    @Test
    @TestTransaction
    void givenWindowAtOrAfterSalesStart_whenSettingSchedule_thenItIsRejected() {
        // Given
        Tree tree = persistTree();
        Instant salgsstart = Instant.now().plusSeconds(3600);

        // When/Then
        assertThrows(IllegalArgumentException.class,
                () -> queueService.updateSchedule(tree.event.uuid, salgsstart.plusSeconds(1), salgsstart));
        assertThrows(IllegalArgumentException.class,
                () -> queueService.updateSchedule(tree.event.uuid, salgsstart, salgsstart));
        assertNull(tree.event.opensAt);
        assertNull(tree.event.salesStartAt);
    }

    @Test
    @TestTransaction
    void givenWindowNotYetOpen_whenEnqueueing_thenItIsRejectedUntilTheWindowOpens() {
        // Given
        Tree tree = persistTree();
        Instant salgsstart = Instant.now().plusSeconds(7200);
        queueService.updateSchedule(tree.event.uuid, salgsstart.minusSeconds(3600), salgsstart);

        // When/Then
        IllegalArgumentException fejl = assertThrows(IllegalArgumentException.class,
                () -> queueService.enqueue(tree.event.uuid));
        assertTrue(fejl.getMessage().contains("åbner"), "beskeden skal sige, hvornår der åbnes: " + fejl.getMessage());
        assertEquals(0, QueueSession.count("queue", tree.event));

        queueService.updateSchedule(tree.event.uuid, Instant.now().minusSeconds(60), salgsstart);
        assertNotNull(queueService.enqueue(tree.event.uuid).uuid);
    }

    @Test
    @TestTransaction
    void givenNormalSubscription_whenCreatingASecondOrganizer_thenItIsRejected() {
        // Given
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        Queue normal = normalSubscription(global);
        queueService.createQueue("generisk arrangør", QueueLevel.ORGANIZER, normal.uuid, null, null);

        // When/Then
        IllegalArgumentException fejl = assertThrows(IllegalArgumentException.class,
                () -> queueService.createQueue("egen arrangør", QueueLevel.ORGANIZER, normal.uuid, null, null));
        assertTrue(fejl.getMessage().contains("generiske"), "beskeden skal forklare hvorfor: " + fejl.getMessage());
        assertEquals(1, Queue.count("parent = ?1 and archivedAt is null", normal));
    }

    @Test
    @TestTransaction
    void givenGenericOrganizer_whenSettingACapacity_thenItIsRejected() {
        // Given
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        Queue normal = normalSubscription(global);
        Queue generic = queueService.createQueue("generisk arrangør", QueueLevel.ORGANIZER, normal.uuid, null, null);

        // When/Then
        assertThrows(IllegalArgumentException.class,
                () -> queueService.updateMaxCapacity(generic.uuid, 30));
        assertNull(generic.maxCapacity);

        Queue leaf = queueService.createQueue("generisk event-kø", QueueLevel.EVENT, generic.uuid, null, null);
        assertEquals(25, queueService.updateMaxCapacity(leaf.uuid, 25).maxCapacity);
    }

    @Test
    @TestTransaction
    void givenSubscriptionLevel_whenCreatingWithType_thenTypeIsPersisted() {
        // Given
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);

        // When
        Queue normal = queueService.createQueue("fælleskø", QueueLevel.SUBSCRIPTION, global.uuid, 50, null,
                QueueType.NORMAL);

        // Then: sat efter persist, så det er UPDATE'et, der skal nå databasen
        assertEquals(QueueType.NORMAL, normal.queueType);
        assertEquals(1, Queue.count("queueType = ?1", QueueType.NORMAL));
    }

    @Test
    @TestTransaction
    void givenOrganizerLevel_whenCreatingWithType_thenCreationFails() {
        // Given
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        Queue subscription = queueService.createQueue("prioritet", QueueLevel.SUBSCRIPTION, global.uuid, null, null,
                QueueType.PRIORITY);

        // When/Then
        assertThrows(IllegalArgumentException.class,
                () -> queueService.createQueue("arrangør", QueueLevel.ORGANIZER, subscription.uuid, 60, null,
                        QueueType.NORMAL));
    }

    @Test
    @TestTransaction
    void givenExistingNormalSubscription_whenCreatingAnotherThroughService_thenDatabaseRejectsIt() {
        // Given
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        queueService.createQueue("fælleskø", QueueLevel.SUBSCRIPTION, global.uuid, 50, null, QueueType.NORMAL);

        // When/Then: indekset afviser, ikke servicelaget
        assertThrows(RuntimeException.class, () -> {
            queueService.createQueue("endnu en fælleskø", QueueLevel.SUBSCRIPTION, global.uuid, 50, null,
                    QueueType.NORMAL);
            Queue.getEntityManager().flush();
        });
    }

    @Test
    @TestTransaction
    void givenGenericLeaf_whenSettingSalesStart_thenItIsRejected() {
        // Given
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        Queue shared = queueService.createQueue("fælleskø", QueueLevel.SUBSCRIPTION, global.uuid, 50, null,
                QueueType.NORMAL);
        Queue genericOrganizer = queueService.createQueue("generisk arrangør", QueueLevel.ORGANIZER, shared.uuid,
                null, null);
        Queue genericEvent = queueService.createQueue("generisk event-kø", QueueLevel.EVENT,
                genericOrganizer.uuid, 25, null);
        Instant salesStart = Instant.now().plusSeconds(3600);

        // When/Then
        assertThrows(IllegalArgumentException.class,
                () -> queueService.updateSchedule(genericEvent.uuid, salesStart.minusSeconds(600), salesStart));

        // Rydning er tilladt, så en fejltilstand kan rettes
        assertDoesNotThrow(() -> queueService.updateSchedule(genericEvent.uuid, null, null));
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

    private Queue normalSubscription(Queue global) {
        Queue normal = queueService.createQueue("fælleskø", QueueLevel.SUBSCRIPTION, global.uuid, 50, null);
        normal.queueType = QueueType.NORMAL;
        Queue.getEntityManager().flush();
        return normal;
    }

    /** GLOBAL(1000) → abonnement(50) → arrangør(null) → event(null) + søskende-abonnement. */
    private Tree persistTree() {
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        Queue subscription = queueService.createQueue("abonnement", QueueLevel.SUBSCRIPTION, global.uuid, 50, null);
        Queue siblingSubscription = queueService.createQueue("abonnement-b", QueueLevel.SUBSCRIPTION, global.uuid, 200, null);
        Queue organizer = queueService.createQueue("arrangør-a", QueueLevel.ORGANIZER, subscription.uuid, null, null);
        Queue event = queueService.createQueue("event-x", QueueLevel.EVENT, organizer.uuid, null, null);
        return new Tree(global, subscription, siblingSubscription, organizer, event);
    }

    private void markWholeTreeClean(Tree tree) {
        tree.global.dirty = false;
        tree.subscription.dirty = false;
        tree.siblingSubscription.dirty = false;
        tree.organizer.dirty = false;
        tree.event.dirty = false;
    }

    private record Tree(Queue global, Queue subscription, Queue siblingSubscription, Queue organizer, Queue event) {
    }
}
