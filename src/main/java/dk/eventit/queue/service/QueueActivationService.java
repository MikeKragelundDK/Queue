package dk.eventit.queue.service;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueSession;
import dk.eventit.queue.entity.QueueSessionState;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;

/** Aktiverer sessions efter FIFO og alle kapacitetslofter på deres køsti. */
@ApplicationScoped
public class QueueActivationService {

    private static final int LOCK_ID = 1;

    @ConfigProperty(name = "queue.activation.page-size", defaultValue = "500")
    int pageSize;

    @ConfigProperty(name = "queue.activation.max-admissions", defaultValue = "500")
    int maxAdmissions;

    @ConfigProperty(name = "queue.activation.max-queue-transitions", defaultValue = "2000")
    int maxQueueTransitions;

    @Inject
    EntityManager em;

    @Inject
    QueueService queueService;

    @PostConstruct
    void validateConfig() {
        if (pageSize < 1 || maxAdmissions < 1 || maxQueueTransitions < 1) {
            throw new IllegalStateException("Aktiveringsmotorens batchgrænser skal være positive heltal");
        }
    }

    /**
     * Claimer på om nogen venter, ikke på dirty-flaget: en allerede-dirty kø tager
     * ingen rækkelås, så flaget kan ryddes før en lukning er committet. Utrukne
     * venteværelser tæller ikke, ellers ligner timen før salgsstart en driftsalarm.
     */
    @Transactional
    public WorkClaim claimDirtyWork() {
        if (!tryLockEngine()) {
            return WorkClaim.LOCK_BUSY;
        }

        clearDirtyFlags();

        long waiting = em.createQuery("""
                select count(s) from QueueSession s
                join s.queue q
                where s.state in (:waiting)
                and (q.salesStartAt is null or q.drawnAt is not null)
                """, Long.class)
                .setParameter("waiting", List.of(QueueSessionState.INITIAL, QueueSessionState.QUEUED))
                .getSingleResult();
        boolean hasWaiting = waiting > 0;
        return hasWaiting ? WorkClaim.CLAIMED : WorkClaim.IDLE;
    }

    /** Pr. række, børn før forældre: en bulk-update låser i scan-orden og deadlocker. */
    private int clearDirtyFlags() {
        List<DirtyQueue> dirtyRows = em.createQuery(
                "select q.id, q.level from Queue q where q.dirty = true", DirtyQueue.class)
                .getResultList();
        dirtyRows.sort(Comparator
                .comparingInt((DirtyQueue row) -> row.level().ordinal())
                .reversed()
                .thenComparing(DirtyQueue::id, Comparator.reverseOrder()));
        for (DirtyQueue row : dirtyRows) {
            Queue.update("dirty = false where id = ?1", row.id());
        }
        return dirtyRows.size();
    }

    private record DirtyQueue(Long id, QueueLevel level) {
    }

    @Transactional
    public BacklogSnapshot backlogSnapshot() {
        WaitingAggregate aggregate = em.createQuery("""
                select count(s), min(s.createdAt)
                from QueueSession s
                where s.state in (:states)
                """, WaitingAggregate.class)
                .setParameter("states", List.of(QueueSessionState.INITIAL, QueueSessionState.QUEUED))
                .getSingleResult();

        Instant oldest = aggregate.oldest();
        long oldestWaitSeconds = oldest == null
                ? 0
                : Math.max(0, Duration.between(oldest, Instant.now()).getSeconds());
        return new BacklogSnapshot(aggregate.waitingSessions(), oldestWaitSeconds);
    }

    /** {@code oldest} skal være en referencetype: {@code min()} giver null, når ingen venter. */
    private record WaitingAggregate(long waitingSessions, Instant oldest) {
    }

    /** Rører kun køer med ventende sessioner, aldrig hele træet — kører hvert sekund. */
    @Transactional
    public ActivationResult activateNow() {
        if (!tryLockEngine()) {
            return ActivationResult.skipped();
        }

        List<Queue> waitingQueues = queuesWithWaitingSessions();
        if (waitingQueues.isEmpty()) {
            return new ActivationResult(true, false, 0, 0, 0);
        }

        Map<Long, Long> openByQueue = currentOpenCounts();
        int scanned = 0;
        int opened = 0;
        int queued = 0;
        long afterSequence = -1;
        long afterId = -1;
        Queue continuationQueue = null;

        Set<Long> eligibleQueueIds = eligibleQueueIds(waitingQueues, openByQueue);
        while (opened < maxAdmissions && !eligibleQueueIds.isEmpty()) {
            List<QueueSession> page = candidatePage(eligibleQueueIds, afterSequence, afterId);
            if (page.isEmpty()) {
                break;
            }

            for (QueueSession session : page) {
                afterSequence = session.sequenceNumber;
                afterId = session.id;
                scanned++;

                if (!SessionLocks.lockAndRefresh(em, session)) {
                    continue;
                }
                if (session.state != QueueSessionState.INITIAL && session.state != QueueSessionState.QUEUED) {
                    continue;
                }
                if (pathIsArchived(session.queue) || !hasCapacity(session.queue, openByQueue)) {
                    if (session.state == QueueSessionState.INITIAL) {
                        queueService.transitionToQueued(session, Instant.now());
                        queued++;
                    }
                    continue;
                }

                // Altid QUEUED før OPEN, ellers falder 0-sekunders-ventetider ud af statistikken.
                Instant now = Instant.now();
                queueService.transitionToQueued(session, now);
                queueService.transitionToOpen(session, now);
                incrementPath(session.queue, openByQueue);
                continuationQueue = session.queue;
                opened++;
                if (opened == maxAdmissions) {
                    break;
                }
            }
            eligibleQueueIds = eligibleQueueIds(waitingQueues, openByQueue);
        }

        queued += queueBlockedInitialSessions(openByQueue);
        boolean batchFull = opened == maxAdmissions;
        if (batchFull && continuationQueue != null) {
            continuationQueue.dirty = true;
        }
        // Flush før commit, så en deadlock kan genkendes af SqlErrors.
        em.flush();
        return new ActivationResult(true, batchFull, scanned, opened, queued);
    }

    private List<QueueSession> candidatePage(Set<Long> eligibleQueueIds, long afterSequence, long afterId) {
        return em.createQuery("""
                select s from QueueSession s
                join fetch s.queue q
                where s.state in (:states)
                  and q.id in (:queueIds)
                  and (s.sequenceNumber > :sequence
                    or (s.sequenceNumber = :sequence and s.id > :id))
                order by s.sequenceNumber, s.id
                """, QueueSession.class)
                .setParameter("states", List.of(QueueSessionState.INITIAL, QueueSessionState.QUEUED))
                .setParameter("queueIds", eligibleQueueIds)
                .setParameter("sequence", afterSequence)
                .setParameter("id", afterId)
                .setMaxResults(pageSize)
                .getResultList();
    }

    private int queueBlockedInitialSessions(Map<Long, Long> openByQueue) {
        List<QueueSession> initial = em.createQuery("""
                select s from QueueSession s
                join fetch s.queue q
                where s.state = :state and (q.salesStartAt is null or q.drawnAt is not null)
                order by s.sequenceNumber, s.id
                """, QueueSession.class)
                .setParameter("state", QueueSessionState.INITIAL)
                .setMaxResults(maxQueueTransitions)
                .getResultList();

        int queued = 0;
        for (QueueSession session : initial) {
            if (!pathIsArchived(session.queue) && hasCapacity(session.queue, openByQueue)) {
                continue;
            }
            if (!SessionLocks.lockAndRefresh(em, session)) {
                continue;
            }
            if (session.state == QueueSessionState.INITIAL) {
                queueService.transitionToQueued(session, Instant.now());
                queued++;
            }
        }
        return queued;
    }

    /** Eksplicitte left joins: implicitte HQL-joins er inner joins og taber roden. */
    private List<Queue> queuesWithWaitingSessions() {
        List<Long> ids = em.createQuery("""
                select s.queue.id
                from QueueSession s
                where s.state in (:states)
                group by s.queue.id
                """, Long.class)
                .setParameter("states", List.of(QueueSessionState.INITIAL, QueueSessionState.QUEUED))
                .getResultList();
        if (ids.isEmpty()) {
            return List.of();
        }
        return em.createQuery("""
                select q from Queue q
                left join fetch q.parent p1
                left join fetch p1.parent p2
                left join fetch p2.parent p3
                where q.id in (:ids) and (q.salesStartAt is null or q.drawnAt is not null)
                """, Queue.class)
                .setParameter("ids", ids)
                .getResultList();
    }

    /** Tæller på hele stien: åbne sessioner på køer uden ventende skal stadig mod de fælles lofter. */
    private Map<Long, Long> currentOpenCounts() {
        List<OpenCountRow> rows = em.createQuery("""
                select q.id, p1.id, p2.id, p3.id, count(s)
                from QueueSession s
                join s.queue q
                left join q.parent p1
                left join p1.parent p2
                left join p2.parent p3
                where s.state = :state
                group by q.id, p1.id, p2.id, p3.id
                """, OpenCountRow.class)
                .setParameter("state", QueueSessionState.OPEN)
                .getResultList();

        Map<Long, Long> counts = new HashMap<>();
        for (OpenCountRow row : rows) {
            for (Long nodeId : row.pathIds()) {
                if (nodeId != null) {
                    counts.merge(nodeId, row.openSessions(), Long::sum);
                }
            }
        }
        return counts;
    }

    /** Forfader-id'erne skal være {@code Long}: null for stier kortere end fire niveauer. */
    private record OpenCountRow(long queueId, Long p1Id, Long p2Id, Long p3Id, long openSessions) {

        List<Long> pathIds() {
            return Arrays.asList(queueId, p1Id, p2Id, p3Id);
        }
    }

    private static Set<Long> eligibleQueueIds(List<Queue> queues, Map<Long, Long> openByQueue) {
        Set<Long> result = new HashSet<>();
        for (Queue queue : queues) {
            if (!pathIsArchived(queue) && hasCapacity(queue, openByQueue)) {
                result.add(queue.id);
            }
        }
        return result;
    }

    private static boolean hasCapacity(Queue queue, Map<Long, Long> openByQueue) {
        for (Queue node = queue; node != null; node = node.parent) {
            if (node.maxCapacity != null && openByQueue.getOrDefault(node.id, 0L) >= node.maxCapacity) {
                return false;
            }
        }
        return true;
    }

    private static boolean pathIsArchived(Queue queue) {
        for (Queue node = queue; node != null; node = node.parent) {
            if (node.archivedAt != null) {
                return true;
            }
        }
        return false;
    }

    private static void incrementPath(Queue queue, Map<Long, Long> openByQueue) {
        for (Queue node = queue; node != null; node = node.parent) {
            openByQueue.merge(node.id, 1L, Long::sum);
        }
    }

    private boolean tryLockEngine() {
        List<?> rows = em.createNativeQuery("""
                select id from queue_activation_lock
                where id = :id
                for update skip locked
                """)
                .setParameter("id", LOCK_ID)
                .getResultList();
        return !rows.isEmpty();
    }

    public record ActivationResult(boolean lockAcquired, boolean batchFull, int scanned, int opened, int queued) {
        static ActivationResult skipped() {
            return new ActivationResult(false, false, 0, 0, 0);
        }
    }

    public record BacklogSnapshot(long waitingSessions, long oldestWaitSeconds) {
    }

    public enum WorkClaim {
        CLAIMED,
        IDLE,
        LOCK_BUSY
    }
}
