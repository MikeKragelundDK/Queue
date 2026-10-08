package dk.eventit.queue.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import dk.eventit.queue.entity.CloseReason;
import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueSession;
import dk.eventit.queue.entity.QueueSessionState;
import dk.eventit.queue.entity.QueueSessionStateChange;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;

/** Fabrikerede tidslinjer via forudfyldt createdAt simulerer motorens output. */
@QuarkusTest
class StatsServiceTest {

    @Inject
    QueueService queueService;

    @Inject
    StatsService statsService;

    @Test
    @TestTransaction
    void givenCraftedTimelines_whenComputingStats_thenWaitProcessingAndAbandonmentAreCorrect() {
        // Given: to gennemførte (ventetid 10 og 20 min), én opgivet (18 min), én ventende.
        Instant now = Instant.now();
        Fixture f = fixture();
        session(f.event, minus(now, 60), minus(now, 50), minus(now, 45), CloseReason.COMPLETED);
        session(f.event, minus(now, 60), minus(now, 40), minus(now, 30), CloseReason.COMPLETED);
        session(f.event, minus(now, 60), null, minus(now, 42), CloseReason.ABANDONED);
        session(f.event, minus(now, 20), null, null, null);

        // When: scopet til fixturens rod — databasen genbruges på tværs af kørsler
        StatsService.StatsDto stats = statsService.compute(f.global, 24);

        // Then: ventetid — median af {10, 20} = 15, p95 = 20.
        assertEquals(2, stats.waitTime().samples());
        assertEquals(15.0, stats.waitTime().medianMinutes());
        assertEquals(20.0, stats.waitTime().p95Minutes());

        // Ekspeditionstid — median af {5, 10} = 7,5.
        assertEquals(2, stats.processingTime().samples());
        assertEquals(7.5, stats.processingTime().medianMinutes());

        // Frafald: 1 af 3, der forlod køen, opgav.
        assertEquals(2, stats.abandonment().completed());
        assertEquals(1, stats.abandonment().abandoned());
        assertEquals(33.3, stats.abandonment().ratePct());

        // Frafald vs. ventetid: i bucket 15–30 min opgav 1 af 2 (s2 lukket ind, s3 opgav).
        StatsService.NamedValue bucket = stats.abandonment().byWaitBucket().stream()
                .filter(b -> b.label().equals("15–30 min")).findFirst().orElseThrow();
        assertEquals(50.0, bucket.value());
        assertEquals(2, bucket.samples());

        // Ankomster (24 t) = 4; kølængde ved seneste bucket-slut = 1 (den ventende).
        assertEquals(4, stats.arrivals().stream().mapToLong(StatsService.TimeBucket::count).sum());
        assertEquals(1, stats.queueLength().getLast().count());
        assertEquals(1, stats.bucketHours(), "24-timers vindue bruger time-buckets");

        // Ventetid pr. kønummer: alle tre i første positionsinterval, median 15.
        assertEquals("1–10", stats.waitBySequenceBucket().getFirst().label());
        assertEquals(15.0, stats.waitBySequenceBucket().getFirst().value());
    }

    @Test
    @TestTransaction
    void givenCapacityOfOne_whenTwoSessionsOpenSequentially_thenTimeAtCapIsAccumulated() {
        // Given: event med loft 1; åben 5 min + åben 10 min = 15 min på loftet i to perioder.
        Instant now = Instant.now();
        Fixture f = fixture();
        f.event.maxCapacity = 1;
        session(f.event, minus(now, 60), minus(now, 50), minus(now, 45), CloseReason.COMPLETED);
        session(f.event, minus(now, 60), minus(now, 40), minus(now, 30), CloseReason.COMPLETED);

        // When
        StatsService.StatsDto stats = statsService.compute(f.subscription, 24);

        // Then
        StatsService.CapacityNode eventNode = stats.capacityPressure().stream()
                .filter(n -> n.name().equals("event-x")).findFirst().orElseThrow();
        assertEquals(15, eventNode.minutesAtCap());
        assertEquals(2, eventNode.hits());
    }

    @Test
    @TestTransaction
    void givenScopedQuery_whenComputingStats_thenSiblingBranchesAreExcluded() {
        // Given: al aktivitet ligger i den ene gren — søskende-grenen er tom.
        Instant now = Instant.now();
        Fixture f = fixture();
        session(f.event, minus(now, 60), minus(now, 50), minus(now, 45), CloseReason.COMPLETED);

        // When/Then
        assertEquals(1, statsService.compute(f.subscription, 24).waitTime().samples(), "grenen ser kun sin egen trafik");
        assertEquals(0, statsService.compute(f.sibling, 24).waitTime().samples(), "søskende-grenen er upåvirket");
        assertTrue(statsService.compute(f.sibling, 24).arrivals().stream()
                .allMatch(b -> b.count() == 0), "ingen ankomster i søskende-grenen");
    }

    @Test
    @TestTransaction
    void givenLargerWindow_whenComputing_thenBucketsScaleWithInterval() {
        // When: 7 dage → 6-timers buckets (28 stk.); ugyldigt interval afvises.
        StatsService.StatsDto stats = statsService.compute(null, 168);

        // Then
        assertEquals(6, stats.bucketHours());
        assertEquals(28, stats.arrivals().size());
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> statsService.compute(null, 99999));
    }

    @Test
    @TestTransaction
    void givenInitialQueuedAndOpenSessions_whenComputingLive_thenTotalAndSubscriptionLoadAgree() {
        // Given: én INITIAL, én QUEUED og én lukket ind
        Instant now = Instant.now();
        Fixture f = fixture();
        QueueSession justArrived = session(f.event, minus(now, 30), null, null, null);
        justArrived.state = QueueSessionState.INITIAL;
        session(f.event, minus(now, 30), null, null, null);
        session(f.event, minus(now, 30), minus(now, 10), null, null);

        // When
        StatsService.LiveStatsDto live = statsService.computeLive(f.global);

        // Then: INITIAL tæller med begge steder, ellers er søjlerne lavere end totalen
        assertEquals(2, live.waitingNow());
        assertEquals(1, live.openNow());
        assertEquals(3, live.arrivalsLastHour());
        StatsService.SubscriptionLoad subscription = live.perSubscription().stream()
                .filter(s -> s.name().equals("abonnement")).findFirst().orElseThrow();
        assertEquals(live.waitingNow(), subscription.waiting(), "totalen og abonnements-søjlen skal tælle ens");
        assertEquals(1, subscription.open());
    }

    @Test
    @TestTransaction
    void givenArrivalsOnBothSidesOfOneHourBoundary_whenComputingLive_thenOnlyRecentArrivalIsCounted() {
        // Given: én ankomst lige inden for og én lige uden for det rullende 60-minuttersvindue.
        Instant now = Instant.now();
        Fixture f = fixture();
        session(f.event, minus(now, 59), null, null, null);
        session(f.event, minus(now, 61), null, null, null);

        // When
        StatsService.LiveStatsDto live = statsService.computeLive(f.global);

        // Then
        assertEquals(1, live.arrivalsLastHour());
    }

    // === Fixture-hjælpere ===

    private record Fixture(Queue global, Queue subscription, Queue sibling, Queue organizer, Queue event) {
    }

    private Fixture fixture() {
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        Queue subscription = queueService.createQueue("abonnement", QueueLevel.SUBSCRIPTION, global.uuid, 50, null);
        Queue sibling = queueService.createQueue("abonnement-b", QueueLevel.SUBSCRIPTION, global.uuid, 200, null);
        Queue organizer = queueService.createQueue("arrangør-a", QueueLevel.ORGANIZER, subscription.uuid, null, null);
        Queue event = queueService.createQueue("event-x", QueueLevel.EVENT, organizer.uuid, null, null);
        return new Fixture(global, subscription, sibling, organizer, event);
    }

    private static Instant minus(Instant now, long minutes) {
        return now.minus(minutes, ChronoUnit.MINUTES);
    }

    private QueueSession session(Queue queue, Instant queuedAt, Instant openedAt, Instant closedAt,
            CloseReason reason) {
        QueueSession session = new QueueSession();
        session.uuid = UUID.randomUUID();
        session.queue = queue;
        session.createdAt = queuedAt;
        session.state = closedAt != null ? QueueSessionState.CLOSED
                : openedAt != null ? QueueSessionState.OPEN : QueueSessionState.QUEUED;
        session.openedAt = openedAt;
        session.closedAt = closedAt;
        session.closeReason = reason;
        session.persist();
        session.sequenceNumber = session.id;

        change(session, QueueSessionState.INITIAL, queuedAt, null);
        change(session, QueueSessionState.QUEUED, queuedAt, null);
        if (openedAt != null) {
            change(session, QueueSessionState.OPEN, openedAt, null);
        }
        if (closedAt != null) {
            change(session, QueueSessionState.CLOSED, closedAt, reason);
        }
        return session;
    }

    private void change(QueueSession session, QueueSessionState state, Instant at, CloseReason reason) {
        QueueSessionStateChange change = new QueueSessionStateChange();
        change.session = session;
        change.state = state;
        change.reason = reason;
        change.createdAt = at;
        change.persist();
    }
}
