package dk.eventit.queue.service;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dk.eventit.queue.entity.CloseReason;
import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueSessionState;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

/** Statistik ud fra transitionsloggen; scope null = hele systemet. Rå rækker, bounded til {@value #MAX_ROWS}. */
@ApplicationScoped
public class StatsService {

    private static final ZoneId ZONE = ZoneId.of("Europe/Copenhagen");
    private static final int MAX_ROWS = 50_000;
    public static final int MAX_WINDOW_HOURS = 2160; // 90 dage
    private static final int MAX_CAPACITY_NODES = 25;

    /** Én definition, så totalen og søjlerne pr. abonnement ikke kan tælle forskelligt. */
    private static final List<QueueSessionState> WAITING_STATES =
            List.of(QueueSessionState.INITIAL, QueueSessionState.QUEUED);

    private static final long[] SEQUENCE_BUCKETS = {10, 50, 100, 250, 500};
    private static final long[] WAIT_BUCKET_MINUTES = {5, 10, 15, 30};

    @Inject
    EntityManager em;

    public StatsDto compute(Queue scope, int windowHours) {
        if (windowHours < 1 || windowHours > MAX_WINDOW_HOURS) {
            throw new IllegalArgumentException("Interval skal være 1–" + MAX_WINDOW_HOURS + " timer");
        }
        Instant now = Instant.now();
        int bucketHours = bucketHoursFor(windowHours);
        int bucketCount = Math.max(1, (int) Math.ceil((double) windowHours / bucketHours));
        Instant seriesEnd = now.truncatedTo(ChronoUnit.HOURS).plus(1, ChronoUnit.HOURS);
        Instant firstBucket = seriesEnd.minus((long) bucketCount * bucketHours, ChronoUnit.HOURS);
        Instant windowStart = firstBucket;

        List<TransitionRow> rows = transitionRows(scope, windowStart);
        boolean truncated = rows.size() >= MAX_ROWS;

        Map<Long, Timeline> timelines = new LinkedHashMap<>();
        long[] arrivals = new long[bucketCount];
        long[] opens = new long[bucketCount];
        long[] completions = new long[bucketCount];
        long[] queueLength = new long[bucketCount];
        long waitingNow = 0;
        long queuedNow = 0;
        int nextSample = 0;

        for (TransitionRow row : rows) {
            long sessionId = row.sessionId();
            long sequenceNumber = row.sequenceNumber();
            QueueSessionState state = row.state();
            CloseReason reason = row.reason();
            Instant at = row.at();

            while (nextSample < bucketCount && at.isAfter(sampleTime(firstBucket, bucketHours, nextSample))) {
                queueLength[nextSample++] = waitingNow;
            }

            Timeline t = timelines.computeIfAbsent(sessionId, id -> new Timeline(sequenceNumber));
            switch (state) {
                case INITIAL -> {
                    t.initialAt = at;
                    waitingNow++;
                    bucket(arrivals, firstBucket, bucketHours, at);
                }
                case QUEUED -> {
                    t.queuedAt = at;
                    t.positionAtEntry = ++queuedNow;
                }
                case OPEN -> {
                    if (t.openedAt == null) {
                        t.openedAt = at;
                        waitingNow--;
                        if (t.queuedAt != null) {
                            queuedNow--;
                        }
                        bucket(opens, firstBucket, bucketHours, at);
                    }
                }
                case CLOSED -> {
                    if (t.closedAt == null) {
                        t.closedAt = at;
                        t.reason = reason;
                        if (t.openedAt == null) {
                            waitingNow--;
                            if (t.queuedAt != null) {
                                queuedNow--;
                            }
                        }
                        if (reason == CloseReason.COMPLETED) {
                            bucket(completions, firstBucket, bucketHours, at);
                        }
                    }
                }
            }
        }
        while (nextSample < bucketCount) {
            queueLength[nextSample++] = waitingNow;
        }

        return new StatsDto(
                bucketHours,
                timeBuckets(arrivals, firstBucket, bucketHours),
                timeBuckets(queueLength, firstBucket, bucketHours),
                timeBuckets(opens, firstBucket, bucketHours),
                timeBuckets(completions, firstBucket, bucketHours),
                waitStats(timelines, windowStart),
                waitByHourOfDay(timelines, windowStart),
                waitBySequenceBucket(timelines, windowStart),
                processingStats(timelines, windowStart),
                abandonmentStats(timelines, windowStart),
                capacityPressure(scope, windowStart, now),
                truncated);
    }

    /** Billige counts til polling. Loft-status bygger GUI'et lazy via {@code roots}/{@code children}. */
    public LiveStatsDto computeLive(Queue scope) {
        long waitingNow = countByStates(scope, WAITING_STATES);
        long openNow = countByStates(scope, List.of(QueueSessionState.OPEN));
        long arrivalsLastHour = arrivalsSince(scope, Instant.now().minus(1, ChronoUnit.HOURS));
        return new LiveStatsDto(waitingNow, openNow, arrivalsLastHour, perSubscription());
    }

    private static int bucketHoursFor(int windowHours) {
        if (windowHours <= 24) {
            return 1;
        }
        if (windowHours <= 168) {
            return 6;
        }
        if (windowHours <= 720) {
            return 24;
        }
        return 72;
    }

    // === Ventetid (OPEN − QUEUED) ===

    private DurationStats waitStats(Map<Long, Timeline> timelines, Instant windowStart) {
        List<Long> seconds = new ArrayList<>();
        for (Timeline t : timelines.values()) {
            if (t.waitSeconds() != null && t.openedAt.isAfter(windowStart)) {
                seconds.add(t.waitSeconds());
            }
        }
        return durationStats(seconds);
    }

    private List<NamedValue> waitByHourOfDay(Map<Long, Timeline> timelines, Instant windowStart) {
        Map<Integer, List<Long>> byHour = new HashMap<>();
        for (Timeline t : timelines.values()) {
            if (t.waitSeconds() != null && t.openedAt.isAfter(windowStart)) {
                int hour = ZonedDateTime.ofInstant(t.openedAt, ZONE).getHour();
                byHour.computeIfAbsent(hour, h -> new ArrayList<>()).add(t.waitSeconds());
            }
        }
        List<NamedValue> result = new ArrayList<>(24);
        for (int hour = 0; hour < 24; hour++) {
            List<Long> seconds = byHour.getOrDefault(hour, List.of());
            result.add(new NamedValue(String.format("%02d", hour), medianMinutes(seconds), seconds.size()));
        }
        return result;
    }

    private List<NamedValue> waitBySequenceBucket(Map<Long, Timeline> timelines, Instant windowStart) {
        Map<String, List<Long>> byBucket = new LinkedHashMap<>();
        for (Timeline t : timelines.values()) {
            if (t.waitSeconds() != null && t.openedAt.isAfter(windowStart) && t.positionAtEntry > 0) {
                byBucket.computeIfAbsent(positionBucket(t.positionAtEntry), b -> new ArrayList<>())
                        .add(t.waitSeconds());
            }
        }
        return byBucket.entrySet().stream()
                .map(e -> new NamedValue(e.getKey(), medianMinutes(e.getValue()), e.getValue().size()))
                .toList();
    }

    // === Gennemstrømning ===

    private DurationStats processingStats(Map<Long, Timeline> timelines, Instant windowStart) {
        List<Long> seconds = new ArrayList<>();
        for (Timeline t : timelines.values()) {
            if (t.openedAt != null && t.closedAt != null && t.reason == CloseReason.COMPLETED
                    && t.closedAt.isAfter(windowStart)) {
                seconds.add(Duration.between(t.openedAt, t.closedAt).getSeconds());
            }
        }
        return durationStats(seconds);
    }

    // === Frafald ===

    private AbandonmentStats abandonmentStats(Map<Long, Timeline> timelines, Instant windowStart) {
        long completed = 0;
        long abandoned = 0;
        long expired = 0;
        // Pr. ventetids-bucket: hvor stor andel opgav frem for at blive lukket ind?
        Map<String, long[]> byWaitBucket = new LinkedHashMap<>(); // [opgivne, indlukkede]
        for (Timeline t : timelines.values()) {
            if (t.closedAt != null && t.closedAt.isAfter(windowStart)) {
                if (t.reason == CloseReason.COMPLETED) {
                    completed++;
                } else if (t.reason == CloseReason.ABANDONED) {
                    abandoned++;
                } else if (t.reason == CloseReason.EXPIRED) {
                    expired++;
                }
            }
            if (t.queuedAt == null) {
                continue;
            }
            if (t.openedAt != null && t.openedAt.isAfter(windowStart)) {
                long waitMin = Duration.between(t.queuedAt, t.openedAt).toMinutes();
                byWaitBucket.computeIfAbsent(waitBucket(waitMin), b -> new long[2])[1]++;
            } else if (t.openedAt == null && t.closedAt != null && t.reason == CloseReason.ABANDONED
                    && t.closedAt.isAfter(windowStart)) {
                long waitMin = Duration.between(t.queuedAt, t.closedAt).toMinutes();
                byWaitBucket.computeIfAbsent(waitBucket(waitMin), b -> new long[2])[0]++;
            }
        }
        double rate = completed + abandoned == 0 ? 0
                : Math.round(1000.0 * abandoned / (completed + abandoned)) / 10.0;
        List<NamedValue> buckets = byWaitBucket.entrySet().stream()
                .map(e -> {
                    long total = e.getValue()[0] + e.getValue()[1];
                    double pct = total == 0 ? 0 : Math.round(1000.0 * e.getValue()[0] / total) / 10.0;
                    return new NamedValue(e.getKey(), pct, total);
                })
                .toList();
        return new AbandonmentStats(completed, abandoned, expired, rate, buckets);
    }

    // === Loft-tryk: tid på loftet pr. kapacitetsbærende knude ===

    private List<CapacityNode> capacityPressure(Queue scope, Instant windowStart, Instant now) {
        List<Queue> nodes = capacityNodes(scope);
        List<CapacityNode> result = new ArrayList<>();
        for (Queue node : nodes) {
            result.add(capacityPressureFor(node, windowStart, now));
        }
        return result;
    }

    private CapacityNode capacityPressureFor(Queue node, Instant windowStart, Instant now) {
        List<CapacityEvent> events = em.createQuery("""
                select c.state, c.createdAt, s.id from QueueSessionStateChange c
                join c.session s
                join s.queue q
                left join q.parent p1
                left join p1.parent p2
                left join p2.parent p3
                where c.state in (:open, :closed) and (q = :n or p1 = :n or p2 = :n or p3 = :n)
                order by c.createdAt, c.id
                """, CapacityEvent.class)
                .setParameter("open", QueueSessionState.OPEN)
                .setParameter("closed", QueueSessionState.CLOSED)
                .setParameter("n", node)
                .setMaxResults(MAX_ROWS)
                .getResultList();

        Set<Long> openSessions = new HashSet<>();
        long openCount = 0;
        long secondsAtCap = 0;
        long hits = 0;
        Instant atCapSince = null;
        for (CapacityEvent event : events) {
            QueueSessionState state = event.state();
            Instant at = event.at();
            long sessionId = event.sessionId();
            if (state == QueueSessionState.OPEN) {
                if (openSessions.add(sessionId)) {
                    openCount++;
                }
            } else if (openSessions.remove(sessionId)) {
                openCount--;
            }
            boolean atCap = openCount >= node.maxCapacity;
            if (atCap && atCapSince == null) {
                atCapSince = at;
            } else if (!atCap && atCapSince != null) {
                long overlap = overlapSeconds(atCapSince, at, windowStart, now);
                if (overlap > 0) {
                    secondsAtCap += overlap;
                    hits++;
                }
                atCapSince = null;
            }
        }
        if (atCapSince != null) {
            long overlap = overlapSeconds(atCapSince, now, windowStart, now);
            if (overlap > 0) {
                secondsAtCap += overlap;
                hits++;
            }
        }
        return new CapacityNode(node.name, node.level.name(), node.maxCapacity, secondsAtCap / 60, hits);
    }

    private record CapacityEvent(QueueSessionState state, Instant at, long sessionId) {
    }

    private List<Queue> capacityNodes(Queue scope) {
        if (scope == null) {
            return em.createQuery(
                    "from Queue q where q.maxCapacity is not null and q.archivedAt is null order by q.id",
                    Queue.class).setMaxResults(MAX_CAPACITY_NODES).getResultList();
        }
        return em.createQuery("""
                select q from Queue q
                left join q.parent p1
                left join p1.parent p2
                left join p2.parent p3
                where q.maxCapacity is not null and q.archivedAt is null
                  and (q = :n or p1 = :n or p2 = :n or p3 = :n)
                order by q.id
                """, Queue.class)
                .setParameter("n", scope)
                .setMaxResults(MAX_CAPACITY_NODES)
                .getResultList();
    }

    // === Kø-tryk pr. abonnement (live-tal) ===

    private List<SubscriptionLoad> perSubscription() {
        return em.createQuery(
                "from Queue q where q.level = :level and q.archivedAt is null order by q.name", Queue.class)
                .setParameter("level", QueueLevel.SUBSCRIPTION)
                .getResultList().stream()
                .map(sub -> new SubscriptionLoad(sub.name,
                        countInSubtree(sub, WAITING_STATES),
                        countInSubtree(sub, List.of(QueueSessionState.OPEN)),
                        sub.maxCapacity))
                .toList();
    }

    private long countInSubtree(Queue node, List<QueueSessionState> states) {
        return em.createQuery("""
                select count(s) from QueueSession s
                join s.queue q
                left join q.parent p1
                left join p1.parent p2
                left join p2.parent p3
                where s.state in :states and (q = :node or p1 = :node or p2 = :node or p3 = :node)
                """, Long.class)
                .setParameter("states", states)
                .setParameter("node", node)
                .getSingleResult();
    }

    // === Hjælpere ===

    /**
     * Vinduesprædikatet skal stå i SQL'en og sorteringen være faldende — ellers
     * rammer afkortningen de nyeste rækker. Vendes bagefter til kronologisk orden.
     */
    private List<TransitionRow> transitionRows(Queue scope, Instant windowStart) {
        String base = """
                select s.id, s.sequenceNumber, c.state, c.reason, c.createdAt
                from QueueSessionStateChange c
                join c.session s
                join s.queue q
                left join q.parent p1
                left join p1.parent p2
                left join p2.parent p3
                where c.createdAt >= :windowStart%s
                order by c.createdAt desc, c.id desc
                """;
        var query = scope == null
                ? em.createQuery(base.formatted(""), TransitionRow.class)
                : em.createQuery(base.formatted(
                        " and (q = :n or p1 = :n or p2 = :n or p3 = :n)"), TransitionRow.class)
                        .setParameter("n", scope);
        List<TransitionRow> rows = new ArrayList<>(query
                .setParameter("windowStart", windowStart)
                .setMaxResults(MAX_ROWS)
                .getResultList());
        Collections.reverse(rows);
        return rows;
    }

    /** {@code reason} er kun sat på CLOSED-rækker. */
    private record TransitionRow(long sessionId, long sequenceNumber, QueueSessionState state,
            CloseReason reason, Instant at) {
    }

    private static Instant sampleTime(Instant firstBucket, int bucketHours, int index) {
        return firstBucket.plus((long) (index + 1) * bucketHours, ChronoUnit.HOURS);
    }

    private static void bucket(long[] buckets, Instant firstBucket, int bucketHours, Instant at) {
        int index = (int) (ChronoUnit.HOURS.between(firstBucket, at) / bucketHours);
        if (index >= 0 && index < buckets.length) {
            buckets[index]++;
        }
    }

    private static List<TimeBucket> timeBuckets(long[] counts, Instant firstBucket, int bucketHours) {
        List<TimeBucket> result = new ArrayList<>(counts.length);
        for (int i = 0; i < counts.length; i++) {
            result.add(new TimeBucket(firstBucket.plus((long) i * bucketHours, ChronoUnit.HOURS), counts[i]));
        }
        return result;
    }

    private long countByStates(Queue scope, List<QueueSessionState> states) {
        if (scope == null) {
            return em.createQuery("select count(s) from QueueSession s where s.state in :states", Long.class)
                    .setParameter("states", states)
                    .getSingleResult();
        }
        return em.createQuery("""
                select count(s) from QueueSession s
                join s.queue q
                left join q.parent p1
                left join p1.parent p2
                left join p2.parent p3
                where s.state in :states and (q = :n or p1 = :n or p2 = :n or p3 = :n)
                """, Long.class)
                .setParameter("states", states)
                .setParameter("n", scope)
                .getSingleResult();
    }

    private long arrivalsSince(Queue scope, Instant cutoff) {
        if (scope == null) {
            return em.createQuery(
                    "select count(c) from QueueSessionStateChange c where c.state = :state and c.createdAt >= :cutoff",
                    Long.class)
                    .setParameter("state", QueueSessionState.INITIAL)
                    .setParameter("cutoff", cutoff)
                    .getSingleResult();
        }
        return em.createQuery("""
                select count(c) from QueueSessionStateChange c
                join c.session s
                join s.queue q
                left join q.parent p1
                left join p1.parent p2
                left join p2.parent p3
                where c.state = :state and c.createdAt >= :cutoff
                  and (q = :n or p1 = :n or p2 = :n or p3 = :n)
                """, Long.class)
                .setParameter("state", QueueSessionState.INITIAL)
                .setParameter("cutoff", cutoff)
                .setParameter("n", scope)
                .getSingleResult();
    }

    private static String positionBucket(long position) {
        long previous = 0;
        for (long limit : SEQUENCE_BUCKETS) {
            if (position <= limit) {
                return (previous + 1) + "–" + limit;
            }
            previous = limit;
        }
        return SEQUENCE_BUCKETS[SEQUENCE_BUCKETS.length - 1] + "+";
    }

    private static String waitBucket(long minutes) {
        long previous = 0;
        for (long limit : WAIT_BUCKET_MINUTES) {
            if (minutes < limit) {
                return previous + "–" + limit + " min";
            }
            previous = limit;
        }
        return WAIT_BUCKET_MINUTES[WAIT_BUCKET_MINUTES.length - 1] + "+ min";
    }

    private static DurationStats durationStats(List<Long> seconds) {
        if (seconds.isEmpty()) {
            return new DurationStats(0, 0, 0);
        }
        List<Long> sorted = seconds.stream().sorted().toList();
        return new DurationStats(medianMinutes(sorted), percentileMinutes(sorted, 0.95), sorted.size());
    }

    private static double medianMinutes(List<Long> seconds) {
        if (seconds.isEmpty()) {
            return 0;
        }
        List<Long> sorted = seconds.stream().sorted().toList();
        int n = sorted.size();
        double medianSeconds = n % 2 == 1 ? sorted.get(n / 2)
                : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
        return Math.round(10.0 * medianSeconds / 60.0) / 10.0;
    }

    private static double percentileMinutes(List<Long> sortedSeconds, double percentile) {
        int index = (int) Math.ceil(percentile * sortedSeconds.size()) - 1;
        long seconds = sortedSeconds.get(Math.max(0, Math.min(index, sortedSeconds.size() - 1)));
        return Math.round(10.0 * seconds / 60.0) / 10.0;
    }

    private static long overlapSeconds(Instant from, Instant to, Instant windowStart, Instant windowEnd) {
        Instant start = from.isBefore(windowStart) ? windowStart : from;
        Instant end = to.isAfter(windowEnd) ? windowEnd : to;
        return end.isAfter(start) ? Duration.between(start, end).getSeconds() : 0;
    }

    private static final class Timeline {
        final long sequenceNumber;
        Instant initialAt;
        Instant queuedAt;
        Instant openedAt;
        Instant closedAt;
        CloseReason reason;
        long positionAtEntry = -1;

        Timeline(long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
        }

        Long waitSeconds() {
            return queuedAt != null && openedAt != null ? Duration.between(queuedAt, openedAt).getSeconds() : null;
        }
    }

    // === DTO'er ===

    public record StatsDto(int bucketHours,
            List<TimeBucket> arrivals, List<TimeBucket> queueLength,
            List<TimeBucket> opens, List<TimeBucket> completions,
            DurationStats waitTime, List<NamedValue> waitByHourOfDay, List<NamedValue> waitBySequenceBucket,
            DurationStats processingTime, AbandonmentStats abandonment,
            List<CapacityNode> capacityPressure, boolean truncated) {
    }

    public record LiveStatsDto(long waitingNow, long openNow, long arrivalsLastHour,
            List<SubscriptionLoad> perSubscription) {
    }

    public record TimeBucket(Instant start, long count) {
    }

    public record NamedValue(String label, double value, long samples) {
    }

    public record DurationStats(double medianMinutes, double p95Minutes, long samples) {
    }

    public record AbandonmentStats(long completed, long abandoned, long expired, double ratePct,
            List<NamedValue> byWaitBucket) {
    }

    public record CapacityNode(String name, String level, int maxCapacity, long minutesAtCap, long hits) {
    }

    /** {@code waiting} må aldrig vises som brøk af {@code maxCapacity}, der kun begrænser OPEN. */
    public record SubscriptionLoad(String name, long waiting, long open, Integer maxCapacity) {
    }
}
