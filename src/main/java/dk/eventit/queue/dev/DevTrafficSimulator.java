package dk.eventit.queue.dev;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import dk.eventit.queue.entity.CloseReason;
import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueSession;
import dk.eventit.queue.entity.QueueSessionState;
import dk.eventit.queue.service.QueueService;
import dk.eventit.queue.service.SqlErrors;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.logging.Log;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.Scheduled.ConcurrentExecution;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

/**
 * Enqueue og close i hver sin transaktion, som rigtige HTTP-kald — flere grene
 * i én transaktion ville bryde låseordenen.
 */
@ApplicationScoped
@IfBuildProperty(name = "queue.dev-tools.enabled", stringValue = "true")
public class DevTrafficSimulator {

    public enum Mode {
        FAST(4.0, Duration.ofSeconds(30), Duration.ofSeconds(120)),
        SLOW(0.35, Duration.ofMinutes(4), Duration.ofMinutes(15)),
        /** Ønsket rate: ticks over et sekund springes over (SKIP), og det er dét, testen skal vise. */
        EXTREME(1000.0, Duration.ofSeconds(10), Duration.ofSeconds(40));

        final double arrivalsPerSecond;
        final Duration dwellMin;
        final Duration dwellMax;

        Mode(double arrivalsPerSecond, Duration dwellMin, Duration dwellMax) {
            this.arrivalsPerSecond = arrivalsPerSecond;
            this.dwellMin = dwellMin;
            this.dwellMax = dwellMax;
        }
    }

    private enum Fate { COMPLETER, GHOST, FORGETTER }

    private record Profile(Fate fate, Duration dwell) {
    }

    private record LeafTarget(UUID uuid, String name, int weight) {
    }

    private record PingCandidate(long id, UUID uuid) {
    }

    private record OpenRow(UUID uuid, Instant openedAt) {
    }

    public record SimStatus(boolean running, String mode, double arrivalsPerSecond,
            long enqueued, long completed, long waitingNow, long openNow, List<String> hotQueues) {
    }

    private static final double GHOST_SHARE = 0.15;
    private static final double FORGETTER_SHARE = 0.08;
    private static final int PING_EVERY_TICKS = 15;
    private static final int PING_CHUNK_SIZE = 200;
    private static final int LEAF_REFRESH_TICKS = 30;
    private static final int PURGE_EVERY_TICKS = 60;
    private static final int MAX_COMPLETIONS_PER_TICK = 200;
    private static final double MAX_ARRIVAL_RATE = 2000.0;
    /** Ved {@code rate=500} kan en ubegrænset {@code list()} ikke bæres — alle scanninger er bounded. */
    private static final int MAX_SESSION_SCAN = 5000;

    private static final List<QueueSessionState> WAITING_STATES =
            List.of(QueueSessionState.INITIAL, QueueSessionState.QUEUED);

    @Inject
    QueueService queueService;

    @Inject
    EntityManager em;

    private final Map<UUID, Profile> profiles = new ConcurrentHashMap<>();
    /** Fra første observation af OPEN, ikke openedAt — ellers forfalder en adopteret pukkel samlet. */
    private final Map<UUID, Instant> completionDueAt = new ConcurrentHashMap<>();
    private final AtomicLong enqueued = new AtomicLong();
    private final AtomicLong completed = new AtomicLong();
    private final Random random = new Random();

    private volatile boolean running;
    private volatile Mode mode = Mode.FAST;
    private volatile double arrivalsPerSecond = Mode.FAST.arrivalsPerSecond;
    private volatile List<LeafTarget> leaves = List.of();
    private volatile List<String> hotQueues = List.of();
    private volatile UUID hotLeaf;
    private volatile double hotShare;
    private long tick;
    private Instant lastTickAt;

    public synchronized SimStatus start(Mode requestedMode, Double rateOverride,
            UUID hotLeafUuid, Double hotLeafShare) {
        // isFinite først: NaN slipper ellers gennem begge tjek.
        if (rateOverride != null
                && (!Double.isFinite(rateOverride) || rateOverride <= 0 || rateOverride > MAX_ARRIVAL_RATE)) {
            throw new IllegalArgumentException(
                    "Ankomstraten skal være et tal mellem 0 og " + (int) MAX_ARRIVAL_RATE + " pr. sekund");
        }
        if (hotLeafShare != null && (!Double.isFinite(hotLeafShare) || hotLeafShare <= 0 || hotLeafShare > 1)) {
            throw new IllegalArgumentException("hotShare skal være et tal mellem 0 og 1");
        }
        if (hotLeafUuid != null && hotLeafShare == null) {
            throw new IllegalArgumentException("Angiv hotShare sammen med hotEvent");
        }
        mode = requestedMode;
        arrivalsPerSecond = rateOverride != null ? rateOverride : requestedMode.arrivalsPerSecond;
        hotLeaf = hotLeafUuid;
        hotShare = hotLeafShare == null ? 0 : hotLeafShare;
        if (!running) {
            enqueued.set(0);
            completed.set(0);
            // Nulstilles, så en gammel pukkel spredes på ny ved genstart.
            completionDueAt.clear();
            refreshLeaves();
            adoptExistingSessions();
            running = true;
            Log.infof("Dev-sim startet: %s (%.2f ankomster/s)%s", mode, arrivalsPerSecond, hotDescription());
        } else {
            Log.infof("Dev-sim skiftet til %s (%.2f ankomster/s)%s", mode, arrivalsPerSecond, hotDescription());
        }
        return status();
    }

    public synchronized SimStatus stop() {
        if (running) {
            running = false;
            Log.info("Dev-sim stoppet — expiry-nettet fejer de efterladte sessioner op");
        }
        return status();
    }

    public SimStatus status() {
        long waiting = QuarkusTransaction.requiringNew()
                .call(() -> QueueSession.count("state in ?1", WAITING_STATES));
        long open = QuarkusTransaction.requiringNew()
                .call(() -> QueueSession.count("state = ?1", QueueSessionState.OPEN));
        return new SimStatus(running, running ? mode.name() : null, arrivalsPerSecond,
                enqueued.get(), completed.get(), waiting, open, hotQueues);
    }

    @Scheduled(every = "1s", concurrentExecution = ConcurrentExecution.SKIP)
    void onTick() {
        if (!running) {
            return;
        }
        tick++;
        if (tick % LEAF_REFRESH_TICKS == 0) {
            safely("blad-refresh", this::refreshLeaves);
        }
        safely("ankomster", this::spawnArrivals);
        if (tick % PING_EVERY_TICKS == 0) {
            safely("pings", this::pingWaitingSessions);
        }
        safely("gennemførelser", this::completeOpenSessions);
        if (tick % PURGE_EVERY_TICKS == 0) {
            safely("profil-oprydning", this::purgeClosedProfiles);
        }
    }

    /** Deadlocks mod motoren er forventet under last — kun rigtige fejl må støje. */
    private void safely(String step, Runnable action) {
        try {
            action.run();
        } catch (RuntimeException e) {
            if (SqlErrors.isLockContention(e)) {
                Log.debugf("Dev-sim %s ramte en deadlock — prøver igen næste tick", step);
            } else {
                Log.errorf(e, "Dev-sim %s fejlede", step);
            }
        }
    }

    /** Raten skaleres med den faktiske tick-afstand, da scheduleren springer ticks over under last. */
    private void spawnArrivals() {
        Instant now = Instant.now();
        double elapsedSeconds = lastTickAt == null ? 1.0
                : Math.min(10.0, Duration.between(lastTickAt, now).toMillis() / 1000.0);
        lastTickAt = now;
        int arrivals = poisson(arrivalsPerSecond * elapsedSeconds);
        List<LeafTarget> targets = leaves;
        if (targets.isEmpty() || arrivals == 0) {
            return;
        }
        for (int i = 0; i < arrivals; i++) {
            LeafTarget target = pickTarget(targets);
            try {
                QueueSession session = queueService.enqueue(target.uuid());
                profiles.put(session.uuid, newProfile());
                enqueued.incrementAndGet();
            } catch (IllegalArgumentException e) {
                Log.debugf("Dev-sim sprang ankomst over: %s", e.getMessage());
            }
        }
    }

    /**
     * Spøgelser pinger ikke, så expiry-nettet får dem. Opdateres i bidder på
     * primærnøglen, så låsene tages i id-orden som motorens — uuid-orden deadlocker.
     */
    private void pingWaitingSessions() {
        Instant now = Instant.now();
        List<Long> pingerIds = QuarkusTransaction.requiringNew().call(() ->
                em.createQuery("""
                        select s.id, s.uuid from QueueSession s
                        where s.state in :states
                        order by s.id
                        """, PingCandidate.class)
                        .setParameter("states", WAITING_STATES)
                        .setMaxResults(MAX_SESSION_SCAN)
                        .getResultList().stream()
                        .filter(row -> {
                            Profile profile = profiles.get(row.uuid());
                            return profile == null || profile.fate() != Fate.GHOST;
                        })
                        .map(PingCandidate::id)
                        .toList());
        for (int from = 0; from < pingerIds.size(); from += PING_CHUNK_SIZE) {
            List<Long> chunk = pingerIds.subList(from, Math.min(from + PING_CHUNK_SIZE, pingerIds.size()));
            safely("ping-bid", () -> QuarkusTransaction.requiringNew().run(() ->
                    QueueSession.update("lastPingAt = ?1 where id in ?2 and state in ?3",
                            now, chunk, WAITING_STATES)));
        }
    }

    /** Glemte faner og spøgelser lukker aldrig selv — dem tager expiry-nettet. */
    private void completeOpenSessions() {
        Instant now = Instant.now();
        List<OpenRow> open = QuarkusTransaction.requiringNew().call(() ->
                em.createQuery("""
                        select s.uuid, s.openedAt from QueueSession s
                        where s.state = :state
                        order by s.id
                        """, OpenRow.class)
                        .setParameter("state", QueueSessionState.OPEN)
                        .setMaxResults(MAX_SESSION_SCAN)
                        .getResultList());

        int closedThisTick = 0;
        for (OpenRow row : open) {
            Profile profile = profiles.computeIfAbsent(row.uuid(), ignored -> newProfile());
            if (profile.fate() != Fate.COMPLETER || row.openedAt() == null) {
                continue;
            }
            Instant dueAt = completionDueAt.computeIfAbsent(row.uuid(), ignored -> now.plus(profile.dwell()));
            if (dueAt.isBefore(now)) {
                queueService.closeSession(row.uuid(), CloseReason.COMPLETED);
                completed.incrementAndGet();
                if (++closedThisTick >= MAX_COMPLETIONS_PER_TICK) {
                    break;
                }
            }
        }
    }

    private void adoptExistingSessions() {
        List<UUID> alive = QuarkusTransaction.requiringNew().call(() ->
                em.createQuery("""
                        select s.uuid from QueueSession s
                        where s.state <> :closed
                        order by s.id
                        """, UUID.class)
                        .setParameter("closed", QueueSessionState.CLOSED)
                        .setMaxResults(MAX_SESSION_SCAN)
                        .getResultList());
        int adopted = 0;
        for (UUID uuid : alive) {
            if (profiles.putIfAbsent(uuid, newProfile()) == null) {
                adopted++;
            }
        }
        Log.infof("Dev-sim adopterede %d levende sessioner", adopted);
    }

    /** Slår op på de uuid'er, vi har profiler for — en afkortet levende liste ville smide aktive væk. */
    private void purgeClosedProfiles() {
        List<UUID> known = List.copyOf(profiles.keySet());
        Set<UUID> alive = new HashSet<>();
        for (int from = 0; from < known.size(); from += PING_CHUNK_SIZE) {
            List<UUID> chunk = known.subList(from, Math.min(from + PING_CHUNK_SIZE, known.size()));
            alive.addAll(QuarkusTransaction.requiringNew().call(() ->
                    em.createQuery("""
                            select s.uuid from QueueSession s
                            where s.uuid in :uuids and s.state <> :closed
                            """, UUID.class)
                            .setParameter("uuids", chunk)
                            .setParameter("closed", QueueSessionState.CLOSED)
                            .getResultList()));
        }
        profiles.keySet().retainAll(alive);
        completionDueAt.keySet().retainAll(alive);
    }

    /** Hvert 8. blad varmt, hvert 3. lunt; sorteret på id, så vægtene er stabile. */
    private void refreshLeaves() {
        List<Queue> leafQueues = QuarkusTransaction.requiringNew().call(() ->
                Queue.<Queue>list("level in ?1 and archivedAt is null order by id",
                        List.of(QueueLevel.EVENT, QueueLevel.MEMBERSYSTEM)));
        List<LeafTarget> targets = new ArrayList<>(leafQueues.size());
        List<String> hot = new ArrayList<>();
        for (int i = 0; i < leafQueues.size(); i++) {
            Queue queue = leafQueues.get(i);
            int weight = i % 8 == 0 ? 20 : i % 3 == 0 ? 5 : 1;
            if (weight == 20) {
                hot.add(queue.name);
            }
            targets.add(new LeafTarget(queue.uuid, queue.name, weight));
        }
        leaves = List.copyOf(targets);
        hotQueues = List.copyOf(hot);
    }

    private String hotDescription() {
        return hotLeaf == null ? "" : String.format(" — %.0f%% målrettet %s", hotShare * 100, hotLeaf);
    }

    private LeafTarget pickTarget(List<LeafTarget> targets) {
        UUID hot = hotLeaf;
        if (hot != null && random.nextDouble() < hotShare) {
            for (LeafTarget target : targets) {
                if (target.uuid().equals(hot)) {
                    return target;
                }
            }
        }
        return pickWeighted(targets);
    }

    private LeafTarget pickWeighted(List<LeafTarget> targets) {
        int total = targets.stream().mapToInt(LeafTarget::weight).sum();
        int roll = random.nextInt(total);
        for (LeafTarget target : targets) {
            roll -= target.weight();
            if (roll < 0) {
                return target;
            }
        }
        return targets.getLast();
    }

    private Profile newProfile() {
        double roll = random.nextDouble();
        Fate fate = roll < GHOST_SHARE ? Fate.GHOST
                : roll < GHOST_SHARE + FORGETTER_SHARE ? Fate.FORGETTER
                : Fate.COMPLETER;
        long spanMillis = mode.dwellMax.toMillis() - mode.dwellMin.toMillis();
        Duration dwell = mode.dwellMin.plusMillis((long) (random.nextDouble() * spanMillis));
        return new Profile(fate, dwell);
    }

    private int poisson(double lambda) {
        if (lambda > 30) {
            return (int) Math.max(0, Math.round(lambda + Math.sqrt(lambda) * random.nextGaussian()));
        }
        double limit = Math.exp(-lambda);
        int k = 0;
        double p = 1.0;
        do {
            k++;
            p *= random.nextDouble();
        } while (p > limit);
        return k - 1;
    }
}
