package dk.eventit.queue.service;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueSession;
import dk.eventit.queue.entity.QueueSessionState;
import dk.eventit.queue.entity.QueueSessionStateChange;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code run()} styrer egne transaktioner og ser kun committede fixtures, som
 * {@code @AfterEach} rydder. Asserts er pr. kø/session, aldrig på totaler.
 */
@QuarkusTest
class QueueDrawServiceTest {

    @Inject
    QueueService queueService;

    @Inject
    QueueDrawService drawService;

    @Inject
    QueueActivationService activationService;

    @Inject
    EntityManager em;

    private final List<UUID> createdRoots = new ArrayList<>();

    @AfterEach
    void deleteCreatedTrees() {
        createdRoots.forEach(this::deleteCommittedTree);
    }

    // === Kandidatopslaget ===

    @Test
    @TestTransaction
    void givenQueuesWithAndWithoutPassedOpensAt_whenFindingCandidates_thenOnlyPassedOnesAreReturned() {
        // Given: tre venteværelser — ét med salgsstart om en halv time, to passerede
        Instant now = Instant.now();
        Fixture queues = fixture(plus(now, 30));
        Queue passedRecently = queueService.createQueue("event-lige-åbnet", QueueLevel.EVENT,
                queues.organizer.uuid, null, null);
        passedRecently.salesStartAt = minus(now, 5);
        Queue passedLongAgo = queueService.createQueue("event-åbnet-for-længst", QueueLevel.EVENT,
                queues.organizer.uuid, null, null);
        passedLongAgo.salesStartAt = minus(now, 60);
        em.flush();

        // When
        List<UUID> candidates = drawService.findDrawCandidates();

        // Then
        assertTrue(candidates.contains(passedRecently.uuid), "salgsstarten er passeret — køen skal trækkes");
        assertTrue(candidates.contains(passedLongAgo.uuid), "salgsstarten er passeret — køen skal trækkes");
        assertFalse(candidates.contains(queues.event.uuid),
                "salgsstarten er ikke nået endnu — der er intet at trække");
        assertTrue(candidates.indexOf(passedLongAgo.uuid) < candidates.indexOf(passedRecently.uuid),
                "ældste salgsstart først — med et bounded batch ville en kø, der blev hængende, ellers kunne udsultes");
    }

    @Test
    @TestTransaction
    void givenAlreadyDrawnOrArchivedQueue_whenFindingCandidates_thenItIsNotReturned() {
        // Given: tre venteværelser med passeret salgsstart — urørt, trukket og arkiveret
        Instant now = Instant.now();
        Fixture queues = fixture(minus(now, 5));
        Queue alreadyDrawn = queueService.createQueue("event-trukket", QueueLevel.EVENT,
                queues.organizer.uuid, null, null);
        alreadyDrawn.salesStartAt = minus(now, 5);
        alreadyDrawn.drawnAt = minus(now, 4);
        Queue archived = queueService.createQueue("event-arkiveret", QueueLevel.EVENT,
                queues.organizer.uuid, null, null);
        archived.salesStartAt = minus(now, 5);
        queueService.archiveQueue(archived.uuid);
        em.flush();

        // When
        List<UUID> candidates = drawService.findDrawCandidates();

        // Then
        assertTrue(candidates.contains(queues.event.uuid),
                "kontrollen: en aktiv, utrukket kø med passeret salgsstart er kandidat");
        assertFalse(candidates.contains(alreadyDrawn.uuid),
                "der trækkes én gang — drawnAt er overdragelsen til motoren, ikke et flag der kan sættes igen");
        assertFalse(candidates.contains(archived.uuid), "en arkiveret kø har ingen kø at blande");
    }

    // === Tidsændringer under lodtrækning ===

    @Test
    void givenDrawClaimed_whenSalesStartIsPostponed_thenChangeIsRejectedAndDrawCompletes() {
        // Given: claim committet, ingen batch skrevet — permute placerer admin i fejlvinduet
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Drawn drawn = preQueue(now, 5);
        Instant originalSalesStart = minus(now, 1);
        Instant postponedSalesStart = plus(now, 60);
        QueueDrawService controlledDraw = configuredDraw(new QueueDrawService() {
            @Override
            protected void permute(List<Long> pool, String seedHex) {
                super.permute(pool, seedHex);

                IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                        () -> queueService.updateSchedule(drawn.queueUuid(), null, postponedSalesStart));
                assertTrue(failure.getMessage().contains("er startet"));
                QuarkusTransaction.requiringNew().run(() -> {
                    Queue queue = Queue.findByUuid(drawn.queueUuid());
                    assertEquals(originalSalesStart, queue.salesStartAt);
                    assertEquals(seedHex, queue.drawSeed, "claimet må ikke nulstilles af admin-kaldet");
                    assertNull(queue.drawnAt, "afvisningen skal ske før lodtrækningen er færdig");
                });
            }
        });

        // When
        QueueDrawService.DrawResult result = controlledDraw.run();

        // Then: admin fik afslag, men lodtrækningen kan fortsætte med sit faste grundlag.
        assertTrue(result.completed());
        QuarkusTransaction.requiringNew().run(() -> {
            Queue queue = Queue.findByUuid(drawn.queueUuid());
            assertEquals(originalSalesStart, queue.salesStartAt);
            assertNotNull(queue.drawnAt);
            for (UUID uuid : drawn.sessionUuids()) {
                assertEquals(QueueSessionState.QUEUED, QueueSession.findByUuid(uuid).state);
            }
        });
    }

    @Test
    void givenDrawCandidate_whenSalesStartIsPostponedBeforeClaim_thenNoDrawOrAdmissionOccurs() {
        // Given: admin committer en udskydelse, efter køen er fundet, men før claim
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Drawn drawn = preQueue(now, 5);
        Instant postponedSalesStart = plus(now, 60);
        QueueDrawService controlledDraw = configuredDraw(new QueueDrawService() {
            @Override
            List<UUID> findDrawCandidates() {
                List<UUID> candidates = super.findDrawCandidates();
                assertTrue(candidates.contains(drawn.queueUuid()), "det gamle opslag skal have fundet køen");
                QuarkusTransaction.requiringNew().run(
                        () -> queueService.updateSchedule(drawn.queueUuid(), null, postponedSalesStart));
                return candidates;
            }
        });

        // When: den forældede kandidatliste må ikke være tilstrækkelig til at trække.
        controlledDraw.run();
        activationService.activateNow();

        // Then: den nye salgsstart gælder, ingen numre er blandet, og ingen er lukket ind.
        QuarkusTransaction.requiringNew().run(() -> {
            Queue queue = Queue.findByUuid(drawn.queueUuid());
            assertEquals(postponedSalesStart, queue.salesStartAt);
            assertNull(queue.drawSeed);
            assertNull(queue.drawnAt);
            for (UUID uuid : drawn.sessionUuids()) {
                QueueSession session = QueueSession.findByUuid(uuid);
                assertEquals(QueueSessionState.INITIAL, session.state);
                assertEquals(session.id.longValue(), session.sequenceNumber);
                assertEquals(0, QueueSessionStateChange.count("session = ?1", session),
                        "hverken lodtrækningen eller motoren må have flyttet den fabrikerede session");
            }
        });
    }

    @Test
    void givenAdminReadBeforeClaim_whenScheduleIsUpdatedAfterClaim_thenFreshSeedRejectsTheChange() {
        // Given: admin har læst seed = null; en anden transaktion claimer og afbrydes før første batch
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Drawn drawn = preQueue(now, 5);
        QueueDrawService interruptedDraw = configuredDraw(new QueueDrawService() {
            @Override
            protected void permute(List<Long> pool, String seedHex) {
                throw new DrawInterruptedAfterClaim();
            }
        });

        // When: de indre transaktioner suspenderer admins, så det gamle snapshot bevares
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> QuarkusTransaction.requiringNew().run(() -> {
                    Queue earlierRead = Queue.findByUuid(drawn.queueUuid());
                    assertNull(earlierRead.drawSeed);
                    assertThrows(DrawInterruptedAfterClaim.class, interruptedDraw::run);
                    assertNull(earlierRead.drawSeed, "admin holder stadig sin gamle entity");

                    queueService.updateSchedule(drawn.queueUuid(), null, plus(now, 60));
                }));

        // Then: servicekaldet genlæste seed under låsen og afviste udskydelsen.
        assertTrue(failure.getMessage().contains("er startet"));
        QuarkusTransaction.requiringNew().run(() -> {
            Queue queue = Queue.findByUuid(drawn.queueUuid());
            assertEquals(minus(now, 1), queue.salesStartAt);
            assertNotNull(queue.drawSeed, "det committede claim overlever admins rollback");
            assertNull(queue.drawnAt, "lodtrækningen er afbrudt og skal stadig kunne genoptages");
        });
    }

    // === Kohorten ===

    @Test
    @TestTransaction
    void givenSessionsArrivedBeforeAndAfterOpensAt_whenFindingCohort_thenOnlyEarlyArrivalsParticipate() {
        // Given
        Instant now = Instant.now();
        Fixture fixture = fixture(minus(now, 1));

        QueueSession earlyArrival = session(fixture.event, QueueSessionState.INITIAL, minus(now, 30));
        QueueSession earlyArrivalTwo = session(fixture.event, QueueSessionState.INITIAL, minus(now, 30));
        QueueSession lateArrival = session(fixture.event, QueueSessionState.INITIAL, now);
        em.flush();

        // When
        List<Long> cohort = drawService.findWaitingSessionIds(fixture.event.uuid);

        // Then
        assertTrue(cohort.contains(earlyArrival.id));
        assertTrue(cohort.contains(earlyArrivalTwo.id));
        assertFalse(cohort.contains(lateArrival.id));
    }

    @Test
    @TestTransaction
    void givenClosedSessionThatArrivedBeforeOpensAt_whenFindingCohort_thenItStillCountsAsParticipant() {
        // Given:
        Instant now = Instant.now();
        Fixture fixture = fixture(minus(now, 1));

        QueueSession earlyArrival = session(fixture.event, QueueSessionState.INITIAL, minus(now, 30));
        QueueSession earlyArrivalTwo = session(fixture.event, QueueSessionState.INITIAL, minus(now, 30));
        earlyArrivalTwo.state = QueueSessionState.CLOSED;
        QueueSession earlyArrivalThree = session(fixture.event, QueueSessionState.INITIAL, minus(now, 30));
        earlyArrivalThree.state = QueueSessionState.QUEUED;
        em.flush();

        // When
        List<Long> cohort = drawService.findWaitingSessionIds(fixture.event.uuid);

        // Then
        assertTrue(cohort.contains(earlyArrival.id));
        assertTrue(cohort.contains(earlyArrivalTwo.id));
        assertTrue(cohort.contains(earlyArrivalThree.id));

    }

    // === Selve lodtrækningen (committede fixtures) ===

    @Test
    void givenPreQueueAtSalesStart_whenDrawRuns_thenNumbersArePermutedAndQueueIsMarkedDrawn() {
        // Given: 25 deltagere — at permutationen rammer ankomstordenen er praktisk umuligt (1/25!)
        Instant now = Instant.now();
        Drawn drawn = preQueue(now, 25);
        Map<UUID, Long> beforeDraw = numbersOf(drawn.sessionUuids());

        // When
        drawService.run();

        // Then
        Map<UUID, Long> afterDraw = numbersOf(drawn.sessionUuids());
        assertEquals(new HashSet<>(beforeDraw.values()), new HashSet<>(afterDraw.values()),
                "en permutation opfinder ingen numre og taber ingen — det er de samme numre i ny rækkefølge");
        assertEquals(afterDraw.size(), new HashSet<>(afterDraw.values()).size(),
                "ingen dubletter — to deltagere må aldrig dele kønummer");
        assertNotEquals(beforeDraw, afterDraw,
                "rækkefølgen er faktisk blandet og ikke bare skrevet tilbage som den lå");

        QuarkusTransaction.requiringNew().run(() -> {
            Queue event = Queue.findByUuid(drawn.queueUuid());
            assertNotNull(event.drawSeed, "seed'en er gemt, så rækkefølgen kan regnes igen");
            assertNotNull(event.drawnAt, "drawnAt er overdragelsen — først nu må motoren røre køen");
            assertTrue(event.drawnAt.isAfter(event.salesStartAt), "der trækkes ved salgsstart, ikke før");
        });
    }

    @Test
    void givenPreQueue_whenDrawRuns_thenEveryParticipantIsQueuedWithOneTransitionRow() {
        // Given: committet fixture; kun uuid'er bæres ud, da entiteterne er detached
        Instant now = Instant.now();
        Drawn drawn = preQueue(now, 2);

        // When
        QueueDrawService.DrawResult result = drawService.run();

        // Then
        assertTrue(result.lockAcquired(), "lodtrækningen skal have fået fler-pod-låsen");
        assertTrue(result.completed(), "låsen blev holdt hele vejen igennem");

        QuarkusTransaction.requiringNew().run(() -> {
            for (UUID sessionUuid : drawn.sessionUuids()) {
                QueueSession session = QueueSession.findByUuid(sessionUuid);
                assertEquals(QueueSessionState.QUEUED, session.state,
                        "deltageren er sat i kø af lodtrækningen");
                assertEquals(1, QueueSessionStateChange.count("session = ?1 and state = ?2",
                        session, QueueSessionState.QUEUED),
                        "præcis én QUEUED-række — en genkørsel må ikke skrive dubletter i transitionsloggen");
            }
        });
    }

    @Test
    void givenDrawnQueue_whenDrawRunsAgain_thenNothingIsRedrawn() {
        // Given: et venteværelse, der allerede er trukket
        Instant now = Instant.now();
        Drawn drawn = preQueue(now, 5);

        drawService.run();
        Map<UUID, Long> afterFirstDraw = numbersOf(drawn.sessionUuids());
        Instant firstDrawnAt = QuarkusTransaction.requiringNew()
                .call(() -> Queue.findByUuid(drawn.queueUuid()).drawnAt);

        // When
        drawService.run();

        // Then
        assertEquals(afterFirstDraw, numbersOf(drawn.sessionUuids()),
                "numrene ligger fast, når de først er trukket");
        QuarkusTransaction.requiringNew().run(() -> {
            Queue event = Queue.findByUuid(drawn.queueUuid());
            assertEquals(firstDrawnAt, event.drawnAt,
                    "køen blev slet ikke rørt anden gang — uændret drawnAt er beviset");
            for (UUID sessionUuid : drawn.sessionUuids()) {
                QueueSession session = QueueSession.findByUuid(sessionUuid);
                assertEquals(1, QueueSessionStateChange.count("session = ?1 and state = ?2",
                        session, QueueSessionState.QUEUED),
                        "stadig præcis én QUEUED-række pr. deltager");
            }
        });
    }

    // === Acceptkriterierne ===

    @Test
    void givenManySeeds_whenPermuting_thenEveryParticipantGetsEveryNumberEquallyOften() {
        // Given: ti deltagere og ti tusind lodtrækninger med deterministiske seeds
        int participants = 10;
        int draws = 10_000;
        List<Long> cohort = LongStream.rangeClosed(1, participants).boxed().toList();
        int[][] landings = new int[participants][participants];

        // When
        for (int draw = 0; draw < draws; draw++) {
            List<Long> pool = new ArrayList<>(cohort);
            drawService.permute(pool, seed(draw));
            assertEquals(new HashSet<>(cohort), new HashSet<>(pool),
                    "hver permutation deler præcis de samme numre ud");
            for (int participant = 0; participant < participants; participant++) {
                landings[participant][pool.get(participant).intValue() - 1]++;
            }
        }

        // Then: forventet 1.000 pr. felt; tolerancen er fem standardafvigelser (σ = 30)
        int expected = draws / participants;
        double p = 1.0 / participants;
        int tolerance = (int) Math.ceil(5 * Math.sqrt(draws * p * (1 - p)));
        for (int participant = 0; participant < participants; participant++) {
            for (int number = 0; number < participants; number++) {
                assertTrue(Math.abs(landings[participant][number] - expected) <= tolerance,
                        "deltager %d fik nummer %d %d gange — forventet %d ± %d".formatted(
                                participant + 1, number + 1, landings[participant][number], expected, tolerance));
            }
        }
    }

    @Test
    void givenInterruptedDraw_whenDrawRunsAgain_thenNoDuplicatesAmongLiveParticipants() {
        // Given: en pod døde midt i en batch — drawnAt er null, nogle numre er skrevet
        Instant now = Instant.now();
        Drawn drawn = preQueue(now, 6);

        drawService.run();
        List<Long> pool = poolOf(drawn.queueUuid());
        QuarkusTransaction.requiringNew().run(() -> {
            QueueSession first = QueueSession.findByUuid(drawn.sessionUuids().getFirst());
            QueueSession second = QueueSession.findByUuid(drawn.sessionUuids().get(1));
            first.sequenceNumber = second.sequenceNumber;
            Queue.update("drawnAt = null where uuid = ?1", drawn.queueUuid());
        });

        // When
        drawService.run();

        // Then
        Map<UUID, Long> numbers = numbersOf(drawn.sessionUuids());
        assertEquals(numbers.size(), new HashSet<>(numbers.values()).size(),
                "genkørslen rydder op efter den halve skrivning — ingen deler nummer bagefter");
        assertEquals(new HashSet<>(pool), new HashSet<>(numbers.values()),
                "puljen er deltagernes id'er og ikke deres nuværende numre — derfor genskabes den fuldt "
                        + "uanset hvad den afbrudte kørsel nåede at skrive");
    }

    @Test
    void givenSameSeedAndCohort_whenDrawIsRepeated_thenOrderIsIdentical() {
        // Given: trukket kø rullet tilbage, men seed'en står — så to pods ikke fletter to rækkefølger
        Instant now = Instant.now();
        Drawn drawn = preQueue(now, 25);

        drawService.run();
        Map<UUID, Long> firstDraw = numbersOf(drawn.sessionUuids());
        QuarkusTransaction.requiringNew().run(() -> {
            for (UUID sessionUuid : drawn.sessionUuids()) {
                QueueSession session = QueueSession.findByUuid(sessionUuid);
                session.sequenceNumber = session.id;
            }
            Queue.update("drawnAt = null where uuid = ?1", drawn.queueUuid());
        });

        // When
        drawService.run();

        // Then
        assertEquals(firstDraw, numbersOf(drawn.sessionUuids()),
                "samme seed og samme deltagere giver nøjagtig samme rækkefølge");
    }

    @Test
    void givenTwoPodsDrawingTheSameQueue_whenTheyRunAtOnce_thenBothWriteTheOrderTheSeedDictates()
            throws Exception {
        // Given: 500 deltagere over mange batches, så låsen skifter hænder undervejs
        Instant now = Instant.now();
        Drawn drawn = preQueue(now, 500);

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        int firstPodTicks;
        int secondPodTicks;
        try {
            Future<Integer> firstPod = executor.submit(() -> drawTogether(ready, start));
            Future<Integer> secondPod = executor.submit(() -> drawTogether(ready, start));
            assertTrue(ready.await(5, TimeUnit.SECONDS), "begge simulerede pods skal være klar");

            // When
            start.countDown();
            firstPodTicks = firstPod.get(120, TimeUnit.SECONDS);
            secondPodTicks = secondPod.get(120, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
        // Begge pods gav op; én tråd gør resten færdig.
        finishDraw();

        // Then: begge holdt låsen — ellers er det bare en enkelt-pod-kørsel
        assertTrue(firstPodTicks > 0 && secondPodTicks > 0,
                "begge pods skal have haft låsen (%d/%d) — ellers var der ingen samtidighed at måle"
                        .formatted(firstPodTicks, secondPodTicks));

        QuarkusTransaction.requiringNew().run(() -> {
            Queue event = Queue.findByUuid(drawn.queueUuid());
            assertNotNull(event.drawnAt, "lodtrækningen blev fuldført af en af dem");

            List<Long> participants = drawService.findWaitingSessionIds(drawn.queueUuid());
            List<Long> pool = new ArrayList<>(participants);
            drawService.permute(pool, event.drawSeed);

            for (int i = 0; i < participants.size(); i++) {
                QueueSession session = QueueSession.findById(participants.get(i));
                assertEquals(pool.get(i).longValue(), session.sequenceNumber,
                        "deltageren har det nummer, seed'en foreskriver — ikke en fletning af to rækkefølger");
                assertEquals(1, QueueSessionStateChange.count("session = ?1 and state = ?2",
                        session, QueueSessionState.QUEUED),
                        "to skrivere må ikke give to QUEUED-rækker");
            }
        });
    }

    @Test
    void givenUndrawnPreQueue_whenActivationEngineTicks_thenNoSessionIsAdmittedUntilTheDrawHandsOver() {
        // Given: et venteværelse med passeret salgsstart, som endnu ikke er trukket
        Instant now = Instant.now();
        Drawn drawn = preQueue(now, 2);

        // When: motoren tikker, mens køen stadig er utrukket
        activationService.activateNow();

        // Then
        QuarkusTransaction.requiringNew().run(() -> {
            for (UUID sessionUuid : drawn.sessionUuids()) {
                assertEquals(QueueSessionState.INITIAL, QueueSession.findByUuid(sessionUuid).state,
                        "motoren må ikke røre en utrukket kø — så ville ankomstorden afgøre salget");
            }
        });

        // When: lodtrækningen kører og sætter drawnAt
        drawService.run();
        activationService.activateNow();

        // Then
        QuarkusTransaction.requiringNew().run(() -> {
            for (UUID sessionUuid : drawn.sessionUuids()) {
                assertEquals(QueueSessionState.OPEN, QueueSession.findByUuid(sessionUuid).state,
                        "efter overdragelsen er venteværelset en kø som alle andre");
            }
        });
    }

    @Test
    void givenParticipantClosedDuringDraw_whenRenumbering_thenItIsSkippedAndLeavesAHole() {
        // Given: seks deltagere, hvoraf én lukkede før lodtrækningen — med i kohorten, men uden nummer
        Instant now = Instant.now();
        Dropout fixture = QuarkusTransaction.requiringNew().call(() -> {
            Fixture f = fixture(minus(now, 1));
            List<UUID> live = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                live.add(session(f.event, QueueSessionState.INITIAL, minus(now, 60)).uuid);
            }
            QueueSession closed = session(f.event, QueueSessionState.CLOSED, minus(now, 60));
            return new Dropout(f.event.uuid, live, closed.uuid);
        });

        // When
        drawService.run();

        // Then
        QuarkusTransaction.requiringNew().run(() -> {
            QueueSession closed = QueueSession.findByUuid(fixture.closedUuid());
            assertEquals(QueueSessionState.CLOSED, closed.state,
                    "lodtrækningen genopliver ikke en lukket session");
            assertEquals(closed.id.longValue(), closed.sequenceNumber,
                    "den beholder sit oprindelige nummer — lodtrækningen sprang den over");

            // Regnes forfra med den gemte seed for at kende den lukkedes nummer.
            List<Long> participants = drawService.findWaitingSessionIds(fixture.queueUuid());
            List<Long> pool = new ArrayList<>(participants);
            drawService.permute(pool, Queue.findByUuid(fixture.queueUuid()).drawSeed);
            Long hole = pool.get(participants.indexOf(closed.id));

            Set<Long> assigned = new HashSet<>();
            for (UUID sessionUuid : fixture.liveUuids()) {
                assigned.add(QueueSession.findByUuid(sessionUuid).sequenceNumber);
            }
            Set<Long> expected = new HashSet<>(pool);
            expected.remove(hole);
            assertEquals(fixture.liveUuids().size(), assigned.size(),
                    "de tilbageværende deltagere deler ikke numre");
            assertEquals(expected, assigned,
                    "alle numre på nær ét er uddelt — hullet er den lukkede deltagers plads, "
                            + "og huller i nummerrækken er tilladt");
        });
    }

    // === Fixture-hjælpere ===

    /** Små batches, så testene går gennem mere end én. */
    private QueueDrawService configuredDraw(QueueDrawService service) {
        service.em = em;
        service.queueService = queueService;
        service.drawBatchSize = 2;
        return service;
    }

    /** Simulerer en pod, der stopper efter det committede claim, før første batch. */
    private static final class DrawInterruptedAfterClaim extends RuntimeException {
    }

    private record Fixture(Queue global, Queue subscription, Queue organizer, Queue event) {
    }

    /** Kun uuid'er — entiteterne er detached efter fixture-transaktionen. */
    private record Drawn(UUID queueUuid, List<UUID> sessionUuids) {
    }

    private record Dropout(UUID queueUuid, List<UUID> liveUuids, UUID closedUuid) {
    }

    private Fixture fixture(Instant salesStartAt) {
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        Queue subscription = queueService.createQueue("abonnement", QueueLevel.SUBSCRIPTION, global.uuid, 50, null);
        Queue organizer = queueService.createQueue("arrangør-a", QueueLevel.ORGANIZER, subscription.uuid, null, null);
        Queue event = queueService.createQueue("event-x", QueueLevel.EVENT, organizer.uuid, null, null);
        event.salesStartAt = salesStartAt;
        createdRoots.add(global.uuid);
        return new Fixture(global, subscription, organizer, event);
    }

    private Drawn preQueue(Instant now, int participants) {
        return QuarkusTransaction.requiringNew().call(() -> {
            Fixture f = fixture(minus(now, 1));
            List<UUID> sessionUuids = new ArrayList<>();
            for (int i = 0; i < participants; i++) {
                sessionUuids.add(session(f.event, QueueSessionState.INITIAL, minus(now, 60)).uuid);
            }
            return new Drawn(f.event.uuid, sessionUuids);
        });
    }

    private static final int POD_TURNS = 5;

    /**
     * Tikker, til podden har haft låsen {@link #POD_TURNS} gange: et fast antal tik
     * overlapper ikke, og to pods uden stop taber låsen til hinanden i det uendelige.
     */
    private int drawTogether(CountDownLatch ready, CountDownLatch start) {
        try {
            ready.countDown();
            if (!start.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Samtidighedstestens start-signal udeblev");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Samtidighedstesten blev afbrudt", e);
        }
        int turns = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (turns < POD_TURNS && System.nanoTime() < deadline) {
            QueueDrawService.DrawResult result = drawService.run();
            if (!result.lockAcquired()) {
                continue;
            }
            turns++;
            if (result.completed() && result.drawnQueues() == 0) {
                break;
            }
        }
        return turns;
    }

    private void finishDraw() {
        for (int tick = 0; tick < 50; tick++) {
            QueueDrawService.DrawResult result = drawService.run();
            if (result.lockAcquired() && result.completed() && result.drawnQueues() == 0) {
                return;
            }
        }
        throw new IllegalStateException("Lodtrækningen blev ikke færdig");
    }

    /** 32 bytes hex, som lodtrækningens. */
    private static String seed(int index) {
        byte[] material = new byte[32];
        ByteBuffer.wrap(material).putInt(index);
        return HexFormat.of().formatHex(material);
    }

    private Map<UUID, Long> numbersOf(List<UUID> sessionUuids) {
        return QuarkusTransaction.requiringNew().call(() -> {
            Map<UUID, Long> numbers = new LinkedHashMap<>();
            for (UUID sessionUuid : sessionUuids) {
                numbers.put(sessionUuid, QueueSession.findByUuid(sessionUuid).sequenceNumber);
            }
            return numbers;
        });
    }

    /** Deltagernes id'er — de numre, lodtrækningen deler ud igen. */
    private List<Long> poolOf(UUID queueUuid) {
        return QuarkusTransaction.requiringNew().call(() -> drawService.findWaitingSessionIds(queueUuid));
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

    private static Instant minus(Instant now, long minutes) {
        return now.minus(minutes, ChronoUnit.MINUTES);
    }

    private static Instant plus(Instant now, long minutes) {
        return now.plus(minutes, ChronoUnit.MINUTES);
    }
}
