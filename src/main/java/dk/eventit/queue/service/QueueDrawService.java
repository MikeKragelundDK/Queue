package dk.eventit.queue.service;

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import jakarta.persistence.LockModeType;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueSession;
import dk.eventit.queue.entity.QueueSessionState;
import io.quarkus.logging.Log;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Ved salgsstart blandes rækkefølgen i en pre-queue. Lås-række id 4. */
@ApplicationScoped
public class QueueDrawService {

    private static final int LOCK_ID = 4;
    private static final String HMAC_ALGORITHM = "HmacSHA256";

    private final SecureRandom random = new SecureRandom();

    @ConfigProperty(name = "queue.draw.batch-size", defaultValue = "500")
    int drawBatchSize;

    @Inject
    EntityManager em;

    @Inject
    QueueService queueService;

    @PostConstruct
    void validateConfig() {
        if (drawBatchSize < 1) {
            throw new IllegalStateException("Lodtrækningens batchstørrelse skal være positiv");
        }
    }

    public DrawResult run() {
        List<UUID> candidates = QuarkusTransaction.requiringNew().call(() ->
                tryLock() ? findDrawCandidates() : null);
        if (candidates == null) {
            return DrawResult.skipped();
        }
        if (candidates.isEmpty()) {
            return new DrawResult(true, true, 0, 0);
        }

        int drawnQueues = 0;
        int shuffledSessions = 0;
        for (UUID uuid : candidates) {
            Integer renumbered;
            try {
                renumbered = drawQueue(uuid, Instant.now());
            } catch (RuntimeException e) {
                if (SqlErrors.isLockContention(e)) {
                    Log.debug("Lodtrækningen ramte kontention — næste tick tager køen");
                    continue;
                }
                throw e;
            }
            if (renumbered == null) {
                return new DrawResult(true, false, drawnQueues, shuffledSessions);
            }
            drawnQueues++;
            shuffledSessions += renumbered;
        }
        return new DrawResult(true, true, drawnQueues, shuffledSessions);
    }

    /** {@code null} = låsen er tabt eller kandidaten kunne ikke claimes; tick'et stopper. */
    private Integer drawQueue(UUID queueUuid, Instant now) {

        ClaimResult result = QuarkusTransaction.requiringNew().call(() ->
                tryLock() ? claimDraw(queueUuid, now) : null
        );

        if (result == null) {
            return null;
        }
        List<Long> participants = result.waitingQueueSessionsID();

        List<Long> pool = new ArrayList<>(participants);
        permute(pool, result.drawSeed());

        int renumbered = 0;
        for (int start = 0; start < participants.size(); start += drawBatchSize) {
            int from = start;
            int to = Math.min(start + drawBatchSize, participants.size());
            Integer batch = QuarkusTransaction.requiringNew().call(() ->
                    tryLock() ? renumberBatch(participants, pool, from, to, now) : null);
            if (batch == null) {
                return null;
            }
            renumbered += batch;
        }

        if (!markDrawn(queueUuid, now)) {
            return null;
        }
        return renumbered;
    }

    /**
     * Samme rækkelås som updateSchedule, så et samtidigt skift af salgsstart enten
     * ses her eller afvises dér. Et eksisterende seed genbruges ved genoptagelse.
     */
    private ClaimResult claimDraw(UUID queueUuid, Instant now) {
        Queue queue = Queue.<Queue>find("uuid", queueUuid)
                .withLock(LockModeType.PESSIMISTIC_WRITE)
                .firstResult();

        if (queue == null
                || queue.archivedAt != null
                || queue.drawnAt != null
                || queue.salesStartAt == null
                || queue.salesStartAt.isAfter(now)) {
            return null;
        }

        if (queue.drawSeed == null) {
            byte[] randomSeed = new byte[32];
            random.nextBytes(randomSeed);
            queue.drawSeed = HexFormat.of().formatHex(randomSeed);
        }
        em.flush();

        return new ClaimResult(queue.drawSeed, findWaitingSessionIds(queueUuid));
    }

    /** Deterministisk via HMAC, så to pods skriver samme permutation. */
    protected void permute(List<Long> pool, String seedHex) {
        Mac mac;
        try {
            mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(HexFormat.of().parseHex(seedHex), HMAC_ALGORITHM));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("Kunne ikke initialisere " + HMAC_ALGORITHM, e);
        }
        Map<Long, byte[]> tickets = HashMap.newHashMap(pool.size());
        for (Long id : pool) {
            tickets.put(id, mac.doFinal(ByteBuffer.allocate(8).putLong(id).array()));
        }
        pool.sort(Comparator.<Long, byte[]>comparing(tickets::get, Arrays::compareUnsigned)
                .thenComparing(Comparator.<Long>naturalOrder()));
    }

    /** {@code participants.get(i)} får nummeret {@code pool.get(i)}. */
    private int renumberBatch(List<Long> participants, List<Long> pool, int from, int to, Instant now) {
        int renumbered = 0;
        for (int i = from; i < to; i++) {
            QueueSession session = QueueSession.findById(participants.get(i));
            if (session == null) {
                continue;
            }

            if (!SessionLocks.lockAndRefresh(em, session)) {
                continue;
            }

            if (session.state != QueueSessionState.INITIAL && session.state != QueueSessionState.QUEUED) {
                continue;
            }

            session.sequenceNumber = pool.get(i);

            queueService.transitionToQueued(session, now);
            renumbered++;
        }
        em.flush();
        return renumbered;
    }

    private boolean markDrawn(UUID queueUuid, Instant now) {
        Boolean done = QuarkusTransaction.requiringNew().call(() -> {
            if (!tryLock()) {
                return null;
            }
            Queue queue = Queue.findByUuid(queueUuid);
            if (queue != null) {
                queue.drawnAt = now;
            }
            em.flush();
            return Boolean.TRUE;
        });
        return done != null;
    }

    List<Long> findWaitingSessionIds(UUID queueUuid) {
        return em.createQuery("""
                        select s.id from QueueSession s
                        where s.queue.uuid = :queueUuid
                          and s.createdAt < s.queue.salesStartAt
                        order by s.id
                        """, Long.class)
                .setParameter("queueUuid", queueUuid)
                .getResultList();
    }

    List<UUID> findDrawCandidates() {
        Instant now = Instant.now();
        return em.createQuery("""
                        select q.uuid from Queue q
                        where q.salesStartAt <= :now
                          and q.drawnAt is null
                          and q.archivedAt is null
                        order by q.salesStartAt
                        """, UUID.class)
                .setParameter("now", now)
                .setMaxResults(drawBatchSize)
                .getResultList();
    }

    private boolean tryLock() {
        List<?> rows = em.createNativeQuery("""
                        select id from queue_activation_lock
                        where id = :id
                        for update skip locked
                        """)
                .setParameter("id", LOCK_ID)
                .getResultList();
        return !rows.isEmpty();
    }

    public record DrawResult(boolean lockAcquired, boolean completed, int drawnQueues, int shuffledSessions) {
        static DrawResult skipped() {
            return new DrawResult(false, false, 0, 0);
        }
    }

    public record ClaimResult(String drawSeed, List<Long> waitingQueueSessionsID) {
    }
}
