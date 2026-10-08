package dk.eventit.queue.service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.IntSupplier;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import dk.eventit.queue.entity.CloseReason;
import dk.eventit.queue.entity.QueueSession;
import dk.eventit.queue.entity.QueueSessionState;
import io.quarkus.logging.Log;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

/**
 * Sikkerhedsnettet — platformens close er kun best-effort. Hver lukning i egen
 * transaktion: én samlet ville AB-BA-deadlocke mod enkelt-lukninger. Lås-række id 2.
 */
@ApplicationScoped
public class QueueExpiryService {

    private static final int LOCK_ID = 2;

    @ConfigProperty(name = "queue.expiry.open-ttl", defaultValue = "30m")
    Duration openTtl;

    @ConfigProperty(name = "queue.expiry.waiting-timeout", defaultValue = "4h")
    Duration waitingTimeout;

    @ConfigProperty(name = "queue.expiry.batch-size", defaultValue = "500")
    int batchSize;

    @Inject
    EntityManager em;

    @Inject
    QueueService queueService;

    @PostConstruct
    void validateConfig() {
        if (batchSize < 1 || openTtl.isNegative() || openTtl.isZero()
                || waitingTimeout.isNegative() || waitingTimeout.isZero()) {
            throw new IllegalStateException("Expiry-jobbets grænser skal være positive");
        }
    }

    public ExpiryResult sweep() {
        Instant now = Instant.now();

        List<UUID> expiredCandidates = QuarkusTransaction.requiringNew().call(() ->
                tryLock() ? expiredOpenSessionUuids(now) : null);
        if (expiredCandidates == null) {
            return ExpiryResult.skipped();
        }
        int expired = 0;
        for (UUID uuid : expiredCandidates) {
            Integer n = closeInOwnTransaction(() -> closeIfExpired(uuid, now));
            if (n == null) {
                return new ExpiryResult(true, false, expired, 0);
            }
            expired += n;
        }

        Instant waitingCutoff = now.minus(waitingTimeout);
        List<UUID> abandonedCandidates = QuarkusTransaction.requiringNew().call(() ->
                tryLock() ? silentWaitingSessionUuids(waitingCutoff) : null);
        if (abandonedCandidates == null) {
            return new ExpiryResult(true, false, expired, 0);
        }
        int abandoned = 0;
        for (UUID uuid : abandonedCandidates) {
            Integer n = closeInOwnTransaction(() -> closeIfAbandoned(uuid, waitingCutoff));
            if (n == null) {
                return new ExpiryResult(true, false, expired, abandoned);
            }
            abandoned += n;
        }
        return new ExpiryResult(true, true, expired, abandoned);
    }

    /** {@code null} = låsen er tabt, stop fejningen. */
    private Integer closeInOwnTransaction(IntSupplier work) {
        try {
            return QuarkusTransaction.requiringNew().call(() -> tryLock() ? work.getAsInt() : null);
        } catch (RuntimeException e) {
            if (SqlErrors.isLockContention(e)) {
                Log.debug("Expiry-lukning ramte en deadlock — næste fejning tager den");
                return 0;
            }
            throw e;
        }
    }

    int closeIfExpired(UUID sessionUuid, Instant now) {
        QueueSession session = QueueSession.findByUuid(sessionUuid);
        if (session == null) {
            return 0;
        }
        if (!SessionLocks.lockAndRefresh(em, session)) {
            return 0;
        }
        if (session.state == QueueSessionState.OPEN && deadlineFor(session).isBefore(now)) {
            queueService.closeSession(session.uuid, CloseReason.EXPIRED);
            em.flush();
            return 1;
        }
        return 0;
    }

    int closeIfAbandoned(UUID sessionUuid, Instant waitingCutoff) {
        QueueSession session = QueueSession.findByUuid(sessionUuid);
        if (session == null) {
            return 0;
        }
        if (!SessionLocks.lockAndRefresh(em, session)) {
            return 0;
        }
        boolean waiting = session.state == QueueSessionState.INITIAL
                || session.state == QueueSessionState.QUEUED;
        if (waiting && lastSignOfLife(session).isBefore(waitingCutoff)) {
            queueService.closeSession(session.uuid, CloseReason.ABANDONED);
            em.flush();
            return 1;
        }
        return 0;
    }

    /** Fallback for ældre rækker uden {@code expiresAt}. */
    private Instant deadlineFor(QueueSession session) {
        if (session.expiresAt != null) {
            return session.expiresAt;
        }
        Instant anchor = session.openedAt != null ? session.openedAt : session.createdAt;
        return anchor.plus(openTtl);
    }

    private static Instant lastSignOfLife(QueueSession session) {
        return session.lastPingAt != null ? session.lastPingAt : session.createdAt;
    }

    List<UUID> expiredOpenSessionUuids(Instant now) {
        return em.createQuery("""
                select s.uuid from QueueSession s
                where s.state = :state
                  and ((s.expiresAt is not null and s.expiresAt < :now)
                    or (s.expiresAt is null and coalesce(s.openedAt, s.createdAt) < :openCutoff))
                order by s.id
                """, UUID.class)
                .setParameter("state", QueueSessionState.OPEN)
                .setParameter("now", now)
                .setParameter("openCutoff", now.minus(openTtl))
                .setMaxResults(batchSize)
                .getResultList();
    }

    List<UUID> silentWaitingSessionUuids(Instant waitingCutoff) {
        return em.createQuery("""
                select s.uuid from QueueSession s
                join s.queue q
                where s.state in (:states)
                  and coalesce(s.lastPingAt, s.createdAt) < :cutoff
                  and (q.salesStartAt is null or q.drawnAt is not null)
                order by s.id
                """, UUID.class)
                .setParameter("states", List.of(QueueSessionState.INITIAL, QueueSessionState.QUEUED))
                .setParameter("cutoff", waitingCutoff)
                .setMaxResults(batchSize)
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

    public record ExpiryResult(boolean lockAcquired, boolean completed, int expired, int abandoned) {
        static ExpiryResult skipped() {
            return new ExpiryResult(false, false, 0, 0);
        }

        public int total() {
            return expired + abandoned;
        }
    }
}
