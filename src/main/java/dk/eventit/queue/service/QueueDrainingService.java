package dk.eventit.queue.service;

import java.util.List;
import java.util.UUID;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import dk.eventit.queue.entity.Queue;
import io.quarkus.logging.Log;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

/**
 * Arkiverer tømte drænende grene (KPM-88) — motoren ser dem aldrig, for de har
 * ingen ventende. Én transaktion pr. gren; lås-række id 5.
 */
@ApplicationScoped
public class QueueDrainingService {

    static final int LOCK_ID = 5;

    @ConfigProperty(name = "queue.draining.batch-size", defaultValue = "100")
    int batchSize;

    @Inject
    EntityManager em;

    @Inject
    QueueService queueService;

    @PostConstruct
    void validateConfig() {
        if (batchSize < 1) {
            throw new IllegalStateException("Dræningsjobbets batchstørrelse skal være positiv");
        }
    }

    public DrainingResult sweep() {
        List<UUID> candidates = QuarkusTransaction.requiringNew().call(() ->
                tryLock() ? drainingRootUuids() : null);
        if (candidates == null) {
            return DrainingResult.skipped();
        }

        int archived = 0;
        for (UUID uuid : candidates) {
            Integer n = archiveInOwnTransaction(uuid);
            if (n == null) {
                return new DrainingResult(true, false, archived);
            }
            archived += n;
        }
        return new DrainingResult(true, true, archived);
    }

    /** {@code null} = låsen er tabt, stop kørslen. */
    private Integer archiveInOwnTransaction(UUID queueUuid) {
        try {
            return QuarkusTransaction.requiringNew().call(() -> tryLock() ? archiveIfDrained(queueUuid) : null);
        } catch (RuntimeException e) {
            if (SqlErrors.isLockContention(e)) {
                Log.debug("Arkivering af drænet gren ramte en deadlock — næste kørsel tager den");
                return 0;
            }
            throw e;
        }
    }

    /** Gentjekker: en session kan være kommet til, eller dræningen ryddet. */
    int archiveIfDrained(UUID queueUuid) {
        Queue queue = Queue.findByUuid(queueUuid);
        if (queue == null || queue.drainingAt == null || queue.archivedAt != null) {
            return 0;
        }
        if (liveSessionsInSubtree(queue) > 0) {
            return 0;
        }

        queueService.archiveQueue(queue.uuid);
        em.flush();
        return 1;
    }

    /** Toppen af hver drænende gren. */
    private List<UUID> drainingRootUuids() {
        return em.createQuery("""
                select q.uuid from Queue q
                    left join q.parent parent
                where q.drainingAt is not null
                  and q.archivedAt is null
                  and (parent is null or parent.drainingAt is null)
                order by q.id
                """, UUID.class)
                .setMaxResults(batchSize)
                .getResultList();
    }

    private long liveSessionsInSubtree(Queue root) {
        return em.createQuery("""
                select count(s) from QueueSession s
                    left join s.queue q
                    left join q.parent p1
                    left join p1.parent p2
                    left join p2.parent p3
                where s.closedAt is null
                  and (q.id = :id or p1.id = :id or p2.id = :id or p3.id = :id)
                """, Long.class)
                .setParameter("id", root.id)
                .getSingleResult();
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

    public record DrainingResult(boolean lockAcquired, boolean completed, int archived) {
        static DrainingResult skipped() {
            return new DrainingResult(false, false, 0);
        }
    }
}
