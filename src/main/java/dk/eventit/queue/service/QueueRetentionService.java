package dk.eventit.queue.service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import dk.eventit.queue.entity.QueueSession;
import dk.eventit.queue.entity.QueueSessionState;
import io.quarkus.logging.Log;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

/**
 * Regel A: arkiverede, barnløse køer forbi perioden slettes (træer forsvinder bund-op).
 * Regel B: gamle CLOSED-sessioner slettes på alle køer. Lås-række id 3.
 */
@ApplicationScoped
public class QueueRetentionService {

    private static final int LOCK_ID = 3;
    private static final ZoneId ZONE = ZoneId.of("Europe/Copenhagen");

    @ConfigProperty(name = "queue.retention.months", defaultValue = "12")
    int retentionMonths;

    @ConfigProperty(name = "queue.retention.batch-size", defaultValue = "5000")
    int batchSize;

    @Inject
    EntityManager em;

    @Inject
    QueueService queueService;

    @PostConstruct
    void validateConfig() {
        if (retentionMonths < 1 || batchSize < 1) {
            throw new IllegalStateException("Retention-perioden og batchstørrelsen skal være positive");
        }
    }

    public RetentionResult run() {
        Instant cutoff = ZonedDateTime.now(ZONE).minusMonths(retentionMonths).toInstant();

        int sessions = 0;
        boolean everLocked = false;
        while (true) {
            Integer n = QuarkusTransaction.requiringNew().call(() ->
                    tryLock() ? deleteClosedSessionsBatch(cutoff, batchSize) : null);
            if (n == null) {
                return everLocked ? new RetentionResult(true, false, sessions, 0) : RetentionResult.skipped();
            }
            everLocked = true;
            sessions += n;
            if (n < batchSize) {
                break;
            }
        }

        int queues = 0;
        while (true) {
            List<UUID> victims = QuarkusTransaction.requiringNew().call(() ->
                    tryLock() ? expiredArchivedQueueUuids(cutoff) : null);
            if (victims == null) {
                return new RetentionResult(true, false, sessions, queues);
            }
            if (victims.isEmpty()) {
                break;
            }

            int deletedThisRound = 0;
            boolean lockLost = false;
            for (UUID uuid : victims) {
                Integer n = deleteQueueInOwnTransaction(uuid);
                if (n == null) {
                    lockLost = true;
                    break;
                }
                deletedThisRound += n;
            }
            queues += deletedThisRound;
            if (lockLost) {
                return new RetentionResult(true, false, sessions, queues);
            }
            // Alle ramte kontention — ellers henter næste runde de samme i det uendelige.
            if (deletedThisRound == 0) {
                break;
            }
        }
        return new RetentionResult(true, true, sessions, queues);
    }

    /** Én transaktion pr. knude, så hver holder sig inde i én kæde. {@code null} = låsen er tabt. */
    private Integer deleteQueueInOwnTransaction(UUID uuid) {
        try {
            return QuarkusTransaction.requiringNew().call(() -> {
                if (!tryLock()) {
                    return null;
                }
                queueService.deleteQueue(uuid);
                em.flush();
                return 1;
            });
        } catch (RuntimeException e) {
            if (SqlErrors.isLockContention(e)) {
                Log.debug("Retention-sletning ramte en deadlock — næste kørsel tager knuden");
                return 0;
            }
            throw e;
        }
    }

    /**
     * Sorteret på {@code closedAt}, ikke {@code id}, så MySQL kan læse direkte ud af
     * {@code ix_session_state_closed} i stedet for at sortere hele mængden pr. batch.
     */
    int deleteClosedSessionsBatch(Instant cutoff, int limit) {
        List<Long> ids = em.createQuery("""
                select s.id from QueueSession s
                where s.state = :state and s.closedAt < :cutoff
                order by s.closedAt
                """, Long.class)
                .setParameter("state", QueueSessionState.CLOSED)
                .setParameter("cutoff", cutoff)
                .setMaxResults(limit)
                .getResultList();
        if (ids.isEmpty()) {
            return 0;
        }

        em.flush();
        em.createQuery("delete from QueueSessionStateChange c where c.session.id in :ids")
                .setParameter("ids", ids)
                .executeUpdate();
        em.createQuery("delete from QueueSession s where s.id in :ids")
                .setParameter("ids", ids)
                .executeUpdate();
        em.clear();
        return ids.size();
    }

    List<UUID> expiredArchivedQueueUuids(Instant cutoff) {
        return em.createQuery("""
                select q.uuid from Queue q
                where q.archivedAt < :cutoff
                  and not exists (select 1 from Queue c where c.parent = q)
                order by q.id
                """, UUID.class)
                .setParameter("cutoff", cutoff)
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

    public record RetentionResult(boolean lockAcquired, boolean completed, int deletedSessions, int deletedQueues) {
        static RetentionResult skipped() {
            return new RetentionResult(false, false, 0, 0);
        }
    }
}
