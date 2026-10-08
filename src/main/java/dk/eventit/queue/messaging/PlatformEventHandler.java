package dk.eventit.queue.messaging;

import java.util.Locale;

import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueType;
import dk.eventit.queue.service.QueueService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

/**
 * Idempotent (at-least-once); kontrakten står i PLATFORM_EVENTS.md. Normale
 * kunder får ingen knuder, så deres "created" er no-op.
 */
@ApplicationScoped
public class PlatformEventHandler {

    @Inject
    QueueService queueService;

    @Transactional
    public void organizerCreated(String organizerKey, String name, String subscriptionType) {
        String reference = Queue.organizerReference(organizerKey);
        if (Queue.findActiveByReference(QueueLevel.ORGANIZER, reference) != null) {
            return;
        }
        Queue subscription = requireSubscription(subscriptionType);
        if (subscription.queueType == QueueType.NORMAL) {
            return;
        }
        queueService.createQueue(name, QueueLevel.ORGANIZER, subscription.uuid, null, reference);
    }

    @Transactional
    public void organizerSubscriptionChanged(String organizerKey, String subscriptionType) {
        Queue organizer = requireActive(QueueLevel.ORGANIZER, Queue.organizerReference(organizerKey));
        Queue subscription = requireSubscription(subscriptionType);

        // Dræn frem for arkivering, som ville smide folk ud midt i en booking (KPM-88).
        if (subscription.queueType == QueueType.NORMAL) {
            queueService.drainQueue(organizer.uuid);
            return;
        }

        queueService.stopDraining(organizer.uuid);
        queueService.moveQueue(organizer.uuid, subscription.uuid);
    }

    @Transactional
    public void organizerArchived(String organizerKey) {
        Queue organizer = Queue.findActiveByReference(QueueLevel.ORGANIZER, Queue.organizerReference(organizerKey));
        if (organizer == null) {
            return;
        }
        queueService.archiveQueue(organizer.uuid);
    }

    @Transactional
    public void leafCreated(QueueLevel level, String key, String organizerKey, String name) {
        String reference = Queue.leafReference(level, key);
        if (Queue.findActiveByReference(level, reference) != null) {
            return;
        }
        // Servicen genbekræfter under strukturlåsen, så en samtidig nedgradering giver no-op.
        Queue organizer = Queue.findActiveByReference(QueueLevel.ORGANIZER, Queue.organizerReference(organizerKey));
        if (organizer == null || organizer.drainingAt != null) {
            return;
        }
        queueService.createQueueIfParentAccepts(name, level, organizer.uuid, reference);
    }

    @Transactional
    public void leafArchived(QueueLevel level, String key) {
        Queue leaf = Queue.findActiveByReference(level, Queue.leafReference(level, key));
        if (leaf == null) {
            return;
        }
        queueService.archiveQueue(leaf.uuid);
    }

    private static Queue requireActive(QueueLevel level, String reference) {
        Queue queue = Queue.findActiveByReference(level, reference);
        if (queue == null) {
            throw new IllegalArgumentException("Ingen aktiv " + level + "-kø med reference '" + reference + "'");
        }
        return queue;
    }

    private static Queue requireSubscription(String type) {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("Eventet mangler en abonnementstype");
        }
        // Locale.ROOT: med tyrkisk locale matcher "priority" aldrig (prikløst i).
        QueueType queueType;
        try {
            queueType = QueueType.valueOf(type.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Ukendt abonnementstype: '" + type + "'");
        }
        Queue subscription = Queue.find("queueType = ?1 and archivedAt is null", queueType).firstResult();
        if (subscription == null) {
            throw new IllegalArgumentException("Der findes ingen aktiv " + queueType + "-kø");
        }
        return subscription;
    }
}
