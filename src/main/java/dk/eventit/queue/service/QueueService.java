package dk.eventit.queue.service;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import dk.eventit.queue.entity.CloseReason;
import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueSession;
import dk.eventit.queue.entity.QueueSessionState;
import dk.eventit.queue.entity.QueueSessionStateChange;
import dk.eventit.queue.entity.ClientType;
import dk.eventit.queue.entity.QueueType;
import io.quarkus.panache.common.Page;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;

/** Kø-træets struktur og sessionernes livscyklus. Alle tilstandsskift skriver transitionslog i samme transaktion. */
@ApplicationScoped
public class QueueService {

    private static final int ARCHIVE_BATCH_SIZE = 500;

    /** Skal svare til {@code visitor_key VARCHAR(64)}. */
    private static final int VISITOR_KEY_MAX_LENGTH = 64;

    @ConfigProperty(name = "queue.expiry.open-ttl", defaultValue = "30m")
    Duration openTtl;

    @Transactional
    public Queue createQueue(String name, QueueLevel level, UUID parentUuid, Integer maxCapacity,
                             String externalReference) {
        return createQueue(name, level, parentUuid, maxCapacity, externalReference, null);
    }

    /** Højst én aktiv af hver type håndhæves af {@code ux_queue_active_type}, ikke her (to pods). */
    @Transactional
    public Queue createQueue(String name, QueueLevel level, UUID parentUuid, Integer maxCapacity,
                             String externalReference, QueueType queueType) {
        if (queueType != null && level != QueueLevel.SUBSCRIPTION) {
            throw new IllegalArgumentException(
                    "Kø-typen hører til på " + QueueLevel.SUBSCRIPTION + ", ikke på " + level);
        }
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Køen skal have et navn");
        }
        if (level == null) {
            throw new IllegalArgumentException("Køen skal have et niveau");
        }
        validateMaxCapacity(maxCapacity);
        lockQueueCreationAndDraining();

        Queue parent = null;
        if (level == QueueLevel.GLOBAL) {
            if (parentUuid != null) {
                throw new IllegalArgumentException("GLOBAL er rodniveauet og kan ikke have en parent");
            }
        } else {
            if (parentUuid == null) {
                throw new IllegalArgumentException(level + " kræver en parent på niveau " + expectedParentLevel(level));
            }
            parent = Queue.findByUuid(parentUuid);
            if (parent == null) {
                throw new IllegalArgumentException("Ukendt parent-kø: " + parentUuid);
            }
            // Kalderen kan have læst forælderen før en nedgradering.
            Queue.getEntityManager().flush();
            Queue.getEntityManager().refresh(parent, LockModeType.PESSIMISTIC_READ);
            if (parent.level != expectedParentLevel(level)) {
                throw new IllegalArgumentException(
                        level + " skal hænge under " + expectedParentLevel(level) + ", ikke " + parent.level);
            }
            if (parent.archivedAt != null) {
                throw new IllegalArgumentException("Kan ikke oprette køer under den arkiverede kø '" + parent.name + "'");
            }
            if (parent.drainingAt != null) {
                throw new IllegalArgumentException("Kan ikke oprette køer under den drænende kø '" + parent.name + "'");
            }
            requireRoomUnderNormal(parent, level);
        }

        Queue queue = new Queue();
        queue.uuid = UUID.randomUUID();
        queue.name = name.trim();
        queue.level = level;
        queue.parent = parent;
        queue.maxCapacity = maxCapacity;
        queue.externalReference = externalReference;
        queue.queueType = queueType;
        queue.persist();
        return queue;
    }

    /** Platformens blad-oprettelse er en no-op, hvis forælderen er ved at udgå. */
    @Transactional
    public Queue createQueueIfParentAccepts(String name, QueueLevel level, UUID parentUuid,
                                           String externalReference) {
        lockQueueCreationAndDraining();
        Queue parent = requireQueue(parentUuid);
        Queue.getEntityManager().flush();
        Queue.getEntityManager().refresh(parent, LockModeType.PESSIMISTIC_READ);
        if (parent.archivedAt != null || parent.drainingAt != null) {
            return null;
        }
        return createQueue(name, level, parentUuid, null, externalReference);
    }

    /** {@code null} = intet loft; nul stopper alle nye indlukninger. */
    @Transactional
    public Queue updateMaxCapacity(UUID queueUuid, Integer maxCapacity) {
        Queue queue = requireQueue(queueUuid);
        if (queue.archivedAt != null) {
            throw new IllegalArgumentException("Kan ikke ændre loftet på den arkiverede kø '" + queue.name + "'");
        }
        validateMaxCapacity(maxCapacity);
        // Normale kunders loft sidder på bladene og NORMAL, aldrig på den generiske arrangør.
        if (queue.level == QueueLevel.ORGANIZER && queue.parent != null
                && queue.parent.queueType == QueueType.NORMAL && maxCapacity != null) {
            throw new IllegalArgumentException(
                    "Den generiske arrangør bærer intet loft — sæt det på bladene eller på fælleskøen");
        }
        if (java.util.Objects.equals(queue.maxCapacity, maxCapacity)) {
            return queue;
        }

        queue.maxCapacity = maxCapacity;
        markDirtyUpwards(queue);
        return queue;
    }

    /**
     * Begge null gør køen almindelig igen. Felterne fryses, så snart lodtrækningen
     * claimer sit seed; rækken låses og genlæses, så de to ikke godkender hinanden
     * ud fra forældede værdier. Ingen dirty-markering — motoren claimer ikke på flaget.
     */
    @Transactional
    public Queue updateSchedule(UUID queueUuid, Instant opensAt, Instant salesStartAt) {
        Queue queue = requireQueue(queueUuid);
        Queue.getEntityManager().refresh(queue, LockModeType.PESSIMISTIC_WRITE);
        if (queue.archivedAt != null) {
            throw new IllegalArgumentException(
                    "Kan ikke ændre tidspunkter på den arkiverede kø '" + queue.name + "'");
        }
        if (queue.level != QueueLevel.EVENT && queue.level != QueueLevel.MEMBERSYSTEM) {
            throw new IllegalArgumentException(
                    "Salgsstart hører til på et event eller et medlemssystem, ikke på niveau " + queue.level);
        }
        if (queue.drawnAt != null) {
            throw new IllegalArgumentException(
                    "Køen '" + queue.name + "' er allerede lodtrukket — tidspunkterne kan ikke ændres bagefter");
        }
        if (queue.drawSeed != null) {
            throw new IllegalArgumentException(
                    "Lodtrækningen for køen '" + queue.name
                            + "' er startet — tidspunkterne kan ikke ændres");
        }
        // De generiske blade blander alle normale arrangementer. Rydning er ok.
        if ((opensAt != null || salesStartAt != null) && isUnderSharedQueue(queue)) {
            throw new IllegalArgumentException(
                    "Fælleskøen kan hverken have venteværelse eller lodtrækning — det kræver en knude pr. "
                            + "arrangement, altså en priority-kunde");
        }
        if (opensAt != null && salesStartAt == null) {
            throw new IllegalArgumentException("Et åbningstidspunkt kræver en salgsstart at åbne op mod");
        }
        if (opensAt != null && !opensAt.isBefore(salesStartAt)) {
            throw new IllegalArgumentException("Venteværelset skal åbne før salgsstart");
        }
        // Omnummereringen springer indlukkede over, så deres numre ville blive delt ud igen.
        // Restrisiko: motoren kan lukke én ind i samme tick.
        if (salesStartAt != null
                && QueueSession.count("queue = ?1 and state = ?2", queue, QueueSessionState.OPEN) > 0) {
            throw new IllegalArgumentException("Køen '" + queue.name + "' har indlukkede sessioner — "
                    + "en salgsstart ville dele deres numre ud til andre");
        }

        queue.opensAt = opensAt;
        queue.salesStartAt = salesStartAt;
        if (salesStartAt == null) {
            queue.drawSeed = null;
        }
        return queue;
    }

    @Transactional
    public QueueSession enqueue(UUID queueUuid) {
        return doEnqueue(requireQueue(queueUuid), null, "");
    }

    /** En levende session for nøglen genbruges: ti faner giver ét lod. */
    public QueueSession enqueue(UUID queueUuid, String visitorKey, String eventContext) {
        if (visitorKey == null) {
            return QuarkusTransaction.requiringNew()
                    .call(() -> doEnqueue(requireQueue(queueUuid), null, eventContext));
        }
        String key = normalizeVisitorKey(visitorKey);

        // Vinderens session kan nå at lukke før genopslaget; så tages det forfra.
        QueueSession session = enqueueForVisitor(queueUuid, key, eventContext);
        return session != null ? session : enqueueForVisitor(queueUuid, key, eventContext);
    }

    /**
     * Ukendt nøgle er ikke en fejl, men fælleskøen. Prisen: et priority-event uden
     * knude havner lydløst dér, indtil synkroniseringen er bygget.
     */
    public QueueSession enqueueByKey(ClientType clientType, String key, String visitorKey) {
        validateRoutingKey(clientType, key);
        String visitor = visitorKey == null ? null : normalizeVisitorKey(visitorKey);
        String eventContext = clientType.name() + ":" + key;
        // Et tabt kapløb vælger køen igen i en frisk transaktion.
        for (int attempt = 1; ; attempt++) {
            try {
                return QuarkusTransaction.requiringNew().call(() -> {
                    QueueLevel level = clientType.leafLevel();
                    Queue leaf = Queue.findActiveByReference(level, Queue.leafReference(level, key));
                    if (leaf != null && visitor != null) {
                        QueueSession existing = QueueSession.findLiveForVisitor(leaf, visitor, eventContext);
                        if (existing != null) {
                            return existing;
                        }
                    }
                    Queue target = leaf != null && leaf.drainingAt == null
                            ? leaf : requireGenericLeaf(clientType);
                    QueueSession existing = visitor == null ? null
                            : QueueSession.findLiveForVisitor(target, visitor, eventContext);
                    return existing != null ? existing : doEnqueue(target, visitor, eventContext);
                });
            } catch (RuntimeException e) {
                if (attempt >= 3 || (!(e instanceof QueueNotAcceptingException) && !SqlErrors.isDuplicateKey(e))) {
                    throw e;
                }
            }
        }
    }

    /** Arkiverede og drænende blade regnes som ukendte; om eventet kører, afgør platformen. */
    @Transactional
    public Queue resolveQueueForKey(ClientType clientType, String key) {
        validateRoutingKey(clientType, key);

        QueueLevel leafLevel = clientType.leafLevel();
        Queue leaf = Queue.findActiveByReference(leafLevel, Queue.leafReference(leafLevel, key));
        return leaf != null && leaf.drainingAt == null ? leaf : requireGenericLeaf(clientType);
    }

    private static void validateRoutingKey(ClientType clientType, String key) {
        if (clientType == null) {
            throw new IllegalArgumentException("Tilmeldingen skal have en klienttype");
        }
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Tilmeldingen skal have en nøgle");
        }

    }

    private static boolean isSharedQueueStructure(Queue queue) {
        return (queue.parent != null && queue.parent.queueType == QueueType.NORMAL)
                || isUnderSharedQueue(queue);
    }

    private static boolean isUnderSharedQueue(Queue leaf) {
        return leaf.parent != null && leaf.parent.parent != null
                && leaf.parent.parent.queueType == QueueType.NORMAL;
    }

    /** Findes på struktur, ikke navn; de generiske knuder er dem uden platform-nøgle. */
    private static Queue requireGenericLeaf(ClientType clientType) {
        Queue leaf = Queue.find("""
                select leaf from Queue leaf
                    left join leaf.parent organizer
                    left join organizer.parent subscription
                where subscription.queueType = ?1
                  and subscription.archivedAt is null
                  and organizer.archivedAt is null
                  and organizer.externalReference is null
                  and leaf.level = ?2
                  and leaf.archivedAt is null
                  and leaf.externalReference is null
                order by leaf.id
                """, QueueType.NORMAL, clientType.leafLevel()).firstResult();

        if (leaf == null) {
            throw new IllegalStateException(
                    "Fælleskøen mangler sit " + clientType.leafLevel() + "-blad — kø-træet er ufuldstændigt");
        }
        return leaf;
    }

    /** Et mellemrum ville give to lodder; en tom nøgle ville dele én session (og token) mellem fremmede. */
    private static String normalizeVisitorKey(String visitorKey) {
        String key = visitorKey.trim();
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Besøgendes-nøglen må ikke være tom");
        }
        if (key.length() > VISITOR_KEY_MAX_LENGTH) {
            throw new IllegalArgumentException(
                    "Besøgendes-nøglen må højst være " + VISITOR_KEY_MAX_LENGTH + " tegn");
        }
        return key;
    }

    private QueueSession enqueueForVisitor(UUID queueUuid, String visitorKey, String eventContext) {
        try {
            return QuarkusTransaction.requiringNew().call(() -> {
                Queue queue = requireQueue(queueUuid);
                QueueSession existing = QueueSession.findLiveForVisitor(queue, visitorKey, eventContext);
                return existing != null ? existing : doEnqueue(queue, visitorKey, eventContext);
            });
        } catch (RuntimeException e) {
            if (!SqlErrors.isDuplicateKey(e)) {
                throw e;
            }
            return QuarkusTransaction.requiringNew().call(
                    () -> QueueSession.findLiveForVisitor(requireQueue(queueUuid), visitorKey, eventContext));
        }
    }

    protected QueueSession doEnqueue(Queue queue, String visitorKey, String eventContext) {
        requireAcceptingNewSessions(queue);
        // FØR insert: FK'en tager S på køen, og en senere X gav S→X-deadlocks.
        markDirtyUpwards(queue);

        QueueSession session = new QueueSession();
        session.uuid = UUID.randomUUID();
        session.queue = queue;
        session.state = QueueSessionState.INITIAL;
        session.visitorKey = visitorKey;
        session.eventContext = eventContext;
        session.persist();
        session.sequenceNumber = session.id;
        recordStateChange(session, QueueSessionState.INITIAL);
        Queue.getEntityManager().flush();
        // Genlæs efter insert: vandt dræning/arkivering, rulles vi tilbage.
        // Aldrig S-lås før dirty-markeringen (S→X-deadlock).
        Queue.getEntityManager().refresh(queue, LockModeType.PESSIMISTIC_READ);
        requireAcceptingNewSessions(queue);
        return session;
    }

    @Transactional
    public Queue moveQueue(UUID queueUuid, UUID newParentUuid) {
        lockQueueCreationAndDraining();
        Queue queue = requireQueue(queueUuid);
        if (queue.level == QueueLevel.GLOBAL) {
            throw new IllegalArgumentException("GLOBAL er roden og kan ikke flyttes");
        }
        if (queue.archivedAt != null) {
            throw new IllegalArgumentException("Køen '" + queue.name + "' er arkiveret — genaktivér den før flyt");
        }
        if (queue.drainingAt != null) {
            throw new IllegalArgumentException(
                    "Køen '" + queue.name + "' dræner — stop dræningen før flyt");
        }
        if (newParentUuid == null) {
            throw new IllegalArgumentException(
                    queue.level + " kræver en parent på niveau " + expectedParentLevel(queue.level));
        }
        Queue newParent = Queue.findByUuid(newParentUuid);
        if (newParent == null) {
            throw new IllegalArgumentException("Ukendt parent-kø: " + newParentUuid);
        }
        if (newParent.level != expectedParentLevel(queue.level)) {
            throw new IllegalArgumentException(
                    queue.level + " skal hænge under " + expectedParentLevel(queue.level) + ", ikke " + newParent.level);
        }
        if (newParent.archivedAt != null) {
            throw new IllegalArgumentException("Kan ikke flytte ind under den arkiverede kø '" + newParent.name + "'");
        }
        // Det flyttede ville stå uden flag og holde grenen fra at blive arkiveret.
        if (newParent.drainingAt != null) {
            throw new IllegalArgumentException(
                    "Kan ikke flytte ind under den drænende kø '" + newParent.name + "'");
        }
        if (queue.parent != null && queue.parent.id.equals(newParent.id)) {
            return queue;
        }
        // Fælleskøens gren har fast form; nedgradering dræner i stedet (KPM-88).
        if (newParent.queueType == QueueType.NORMAL) {
            throw new IllegalArgumentException(
                    "Der flyttes ikke ind i fælleskøen — en nedgradering sætter grenen til at dræne i stedet");
        }
        if (isSharedQueueStructure(queue)) {
            throw new IllegalArgumentException(
                    "De generiske knuder er fælleskøens struktur og kan ikke flyttes");
        }

        Queue oldParent = queue.parent;
        queue.parent = newParent;

        // Den flyttede knude først: flushen tager parent-ændringen med, sorteret
        // efter id, og kom oldParent først, blev låseordenen rod → blad.
        markDirtyUpwards(queue);
        markDirtyUpwards(oldParent);
        return queue;
    }

    /** Idempotent; første lukning og dens årsag vinder. */
    @Transactional
    public QueueSession closeSession(UUID sessionUuid, CloseReason reason) {
        QueueSession session = QueueSession.findByUuid(sessionUuid);
        if (session == null) {
            throw new IllegalArgumentException("Ukendt session: " + sessionUuid);
        }
        EntityManager em = QueueSession.getEntityManager();
        if (!SessionLocks.lockAndRefresh(em, session)) {
            throw new IllegalArgumentException("Ukendt session: " + sessionUuid);
        }

        if (session.state == QueueSessionState.CLOSED) {
            return session;
        }

        closeInternal(session, Instant.now(), reason);
        markDirtyUpwards(session.queue);
        return session;
    }

    @Transactional
    public Queue archiveQueue(UUID queueUuid) {
        Queue queue = requireQueue(queueUuid);
        if (queue.archivedAt != null) {
            return queue;
        }
        archiveSubtree(queue, Instant.now());
        markDirtyUpwards(queue);
        return queue;
    }

    /** Cascader kun til knuder med samme {@code archivedAt}. */
    @Transactional
    public Queue unarchiveQueue(UUID queueUuid) {
        Queue queue = requireQueue(queueUuid);
        if (queue.archivedAt == null) {
            return queue;
        }
        if (queue.parent != null && queue.parent.archivedAt != null) {
            throw new IllegalArgumentException(
                    "Forælderen '" + queue.parent.name + "' er arkiveret — genaktivér den først");
        }
        unarchiveSubtree(queue, queue.archivedAt);
        markDirtyUpwards(queue);
        return queue;
    }

    @Transactional
    public void deleteQueue(UUID queueUuid) {
        Queue queue = requireQueue(queueUuid);
        if (queue.archivedAt == null) {
            throw new IllegalArgumentException(
                    "Køen '" + queue.name + "' er aktiv — arkivér den i stedet (statistik bevares)");
        }
        if (Queue.count("parent", queue) > 0) {
            throw new IllegalArgumentException(
                    "Køen '" + queue.name + "' har underliggende køer — slet dem først");
        }

        UUID parentUuid = queue.parent != null ? queue.parent.uuid : null;

        // Bulk-delete går uden om konteksten; clear, ellers TransientPropertyValueException.
        var em = Queue.getEntityManager();
        em.flush();
        QueueSessionStateChange.delete("session in (select s from QueueSession s where s.queue = ?1)", queue);
        QueueSession.delete("queue", queue);
        Queue.delete("id", queue.id);
        em.clear();

        if (parentUuid != null) {
            markDirtyUpwards(Queue.findByUuid(parentUuid));
        }
    }

    /** Ingen dirty-markering: kapacitetsregnskabet ændrer sig ikke. */
    @Transactional
    public Queue drainQueue(UUID queueUuid) {
        lockQueueCreationAndDraining();
        Queue queue = requireQueue(queueUuid);
        // DB-præcision, da markøren sammenlignes efter refresh.
        drainSubtree(queue, Instant.now().truncatedTo(ChronoUnit.MICROS));
        // Roden låses sidst, så tjekket står her.
        if (queue.archivedAt != null) {
            throw new IllegalArgumentException(
                    "Køen '" + queue.name + "' er arkiveret — der er ikke noget at dræne");
        }
        return queue;
    }

    /**
     * Rydder kun knuder med samme {@code drainingAt}. Strukturlåsen tages først,
     * da opgraderingen kalder {@code moveQueue} bagefter.
     */
    @Transactional
    public Queue stopDraining(UUID queueUuid) {
        lockQueueCreationAndDraining();
        Queue queue = requireQueue(queueUuid);
        if (queue.drainingAt == null) {
            return queue;
        }
        stopDrainingSubtree(queue, queue.drainingAt);
        return queue;
    }

    /**
     * Nedefra og op, låst pr. primærnøgle — en låsende scanning låser alt, den læser.
     * Det låsende børneopslag fanger blade, kalderens read view ikke kender.
     */
    private void drainSubtree(Queue queue, Instant now) {
        drainSubtree(queue, now, false);
    }

    private void drainSubtree(Queue queue, Instant now, boolean locked) {
        if (!locked) {
            for (Queue child : Queue.<Queue>list("parent", queue)) {
                drainSubtree(child, now, false);
            }
        }

        var em = Queue.getEntityManager();

        for (Queue child : Queue.<Queue>find("parent", queue)
                .withLock(LockModeType.PESSIMISTIC_WRITE).list()) {
            if (child.archivedAt == null && child.drainingAt == null) {
                drainSubtree(child, now, true);
            }
        }

        if (!locked) {
            // En låst-læst række ville kaste EntityNotFound her.
            em.refresh(queue, LockModeType.PESSIMISTIC_WRITE);
        }
        if (queue.archivedAt != null || queue.drainingAt != null) {
            return;
        }

        queue.drainingAt = now;
        em.flush();
    }

    /** Kun den kolde sti: oprettelse og dræning deler fejejobbets fler-pod-lås. */
    private void lockQueueCreationAndDraining() {
        Queue.getEntityManager().createNativeQuery(
                "select id from queue_activation_lock where id = :id for update")
                .setParameter("id", QueueDrainingService.LOCK_ID)
                .getSingleResult();
    }

    private void stopDrainingSubtree(Queue queue, Instant drainingAt) {
        for (Queue child : Queue.<Queue>list("parent", queue)) {
            stopDrainingSubtree(child, drainingAt);
        }
        if (drainingAt.equals(queue.drainingAt)) {
            queue.drainingAt = null;
        }
    }

    /** Nedefra og op, ellers AB-BA-deadlock mod enqueue. */
    private void archiveSubtree(Queue queue, Instant now) {
        for (Queue child : Queue.<Queue>list("parent", queue)) {
            archiveSubtree(child, now);
        }

        var em = Queue.getEntityManager();

        em.refresh(queue, LockModeType.PESSIMISTIC_WRITE);
        if (queue.archivedAt != null) {
            return;
        }

        queue.archivedAt = now;
        em.flush();

        closeSessionsForArchive(queue, now);
    }

    /**
     * Bundne bidder med detach: en ubundet liste var OOM ved 200k sessioner og
     * kvadratisk pga. flush pr. session. Flush før detach, ellers tabes INSERT'et.
     */
    private void closeSessionsForArchive(Queue queue, Instant now) {
        var em = Queue.getEntityManager();
        List<QueueSession> batch;

        do {
            batch = QueueSession.<QueueSession>find(
                            "queue = ?1 and state != ?2 order by id",
                            queue, QueueSessionState.CLOSED)
                    .withLock(LockModeType.PESSIMISTIC_WRITE)
                    .page(Page.ofSize(ARCHIVE_BATCH_SIZE))
                    .list();

            for (QueueSession session : batch) {
                if (!SessionLocks.lockAndRefresh(em, session) || session.state == QueueSessionState.CLOSED) {
                    em.detach(session);
                    continue;
                }

                QueueSessionStateChange change =
                        closeInternal(session, now, CloseReason.ARCHIVED);
                em.flush();
                em.detach(change);
                em.detach(session);
            }
        } while (batch.size() == ARCHIVE_BATCH_SIZE);
    }

    private void unarchiveSubtree(Queue queue, Instant archivedAt) {
        queue.archivedAt = null;
        for (Queue child : Queue.<Queue>list("parent = ?1 and archivedAt = ?2", queue, archivedAt)) {
            unarchiveSubtree(child, archivedAt);
        }
    }

    private QueueSessionStateChange closeInternal(QueueSession session, Instant now, CloseReason reason) {
        session.closedAt = now;
        session.closeReason = reason;
        session.state = QueueSessionState.CLOSED;
        Queue.getEntityManager().flush();
        return recordStateChange(session, QueueSessionState.CLOSED, reason);
    }

    private void recordStateChange(QueueSession session, QueueSessionState state) {
        recordStateChange(session, state, null);
    }

    private QueueSessionStateChange recordStateChange(QueueSession session, QueueSessionState state,
                                                      CloseReason reason) {
        return recordStateChange(session, state, reason, null);
    }

    private QueueSessionStateChange recordStateChange(QueueSession session, QueueSessionState state,
                                                      CloseReason reason, Instant at) {
        QueueSessionStateChange change = new QueueSessionStateChange();
        change.session = session;
        change.state = state;
        change.reason = reason;
        change.createdAt = at;
        change.persist();
        return change;
    }

    protected void transitionToQueued(QueueSession session, Instant now) {
        if (session.state != QueueSessionState.INITIAL) {
            return;
        }
        session.state = QueueSessionState.QUEUED;
        recordStateChange(session, QueueSessionState.QUEUED, null, now);
    }

    protected void transitionToOpen(QueueSession session, Instant now) {
        if (session.state != QueueSessionState.INITIAL && session.state != QueueSessionState.QUEUED) {
            return;
        }
        session.state = QueueSessionState.OPEN;
        session.openedAt = now;
        // TODO KPM-77: fornybar levetid med absolut loft i stedet for fast TTL.
        session.expiresAt = now.plus(openTtl);
        recordStateChange(session, QueueSessionState.OPEN, null, now);
    }

    private Queue requireQueue(UUID queueUuid) {
        Queue queue = Queue.findByUuid(queueUuid);
        if (queue == null) {
            throw new IllegalArgumentException("Ukendt kø: " + queueUuid);
        }
        return queue;
    }

    /** Højst én arrangør under NORMAL og ét blad af hver slags under den; gælder også platform-events. */
    private static void requireRoomUnderNormal(Queue parent, QueueLevel level) {
        Queue subscription = switch (level) {
            case ORGANIZER -> parent;
            case EVENT, MEMBERSYSTEM -> parent.parent;
            default -> null;
        };
        if (subscription == null || subscription.queueType != QueueType.NORMAL) {
            return;
        }
        long existing = Queue.count("parent = ?1 and level = ?2 and archivedAt is null", parent, level);
        if (existing > 0) {
            throw new IllegalArgumentException(
                    "Normale kunder deler de generiske køer — der findes allerede en " + level + " under '"
                            + parent.name + "'");
        }
    }

    private static void requireAcceptingNewSessions(Queue queue) {
        if (queue.archivedAt != null || queue.drainingAt != null) {
            throw new QueueNotAcceptingException("Køen '" + queue.name + "' er arkiveret eller er ved at dræne" +
                    " og modtager ikke nye sessioner");
        }

        if (queue.opensAt != null && Instant.now().isBefore(queue.opensAt)) {
            throw new IllegalArgumentException(
                    "Venteværelset for '" + queue.name + "' åbner først " + queue.opensAt);
        }
    }

    private static class QueueNotAcceptingException extends IllegalArgumentException {
        QueueNotAcceptingException(String message) {
            super(message);
        }
    }

    /**
     * Flush pr. række: Hibernate sorterer UPDATEs efter id ved flush, så uden den
     * låses GLOBAL først. Allerede dirty køer springes over og tager ingen lås.
     * Bulk-update duer ikke — en senere flush af entiteten skriver dirty=false tilbage.
     */
    private void markDirtyUpwards(Queue queue) {
        var em = Queue.getEntityManager();
        for (Queue node = queue; node != null; node = node.parent) {
            if (node.dirty) {
                continue;
            }
            node.dirty = true;
            em.flush();
        }
    }

    static QueueLevel expectedParentLevel(QueueLevel level) {
        return switch (level) {
            case GLOBAL -> null;
            case SUBSCRIPTION -> QueueLevel.GLOBAL;
            case ORGANIZER -> QueueLevel.SUBSCRIPTION;
            case EVENT, MEMBERSYSTEM -> QueueLevel.ORGANIZER;
        };
    }

    private static void validateMaxCapacity(Integer maxCapacity) {
        if (maxCapacity != null && maxCapacity < 0) {
            throw new IllegalArgumentException("Loftet skal være nul eller et positivt heltal");
        }
    }
}
