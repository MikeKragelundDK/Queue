package dk.eventit.queue.dev;

import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueType;
import dk.eventit.queue.service.QueueService;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

/**
 * Træets form defineres kun her — migrationer har ingen rækker at omforme.
 * Idempotent. Ingen sessioner: trafik kommer fra {@link DevTrafficSimulator}.
 */
@ApplicationScoped
@IfBuildProperty(name = "queue.dev-tools.enabled", stringValue = "true")
public class DevDataSeeder {

    private static final String[] ORGANIZERS = {
            "København Koncerthus", "Nordhavn Live", "Aarhus Kulturhus", "Odense Sport",
            "Kolding Koncerter", "Roskilde Events", "Aalborg Arena", "Esbjerg Scene",
            "Vejle Oplevelser", "Bornholm Festival"
    };

    @Inject
    QueueService queueService;

    void onStart(@Observes StartupEvent event) {
        seed();
    }

    @Transactional
    void seed() {
        Queue priority = ensureBaseTree();
        ensurePriorityOrganizers(priority);
    }

    private Queue ensureBaseTree() {
        if (Queue.count() > 0) {
            // Opslag på typen, ikke navnet: ellers vælter et omdøb opstarten.
            Queue existing = Queue.find("queueType = ?1 and archivedAt is null", QueueType.PRIORITY).firstResult();
            if (existing == null) {
                throw new IllegalStateException("Dev-seed forventer en eksisterende prioritetskø");
            }
            return existing;
        }

        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        Queue shared = queueService.createQueue("fælleskø", QueueLevel.SUBSCRIPTION, global.uuid, 50, null,
                QueueType.NORMAL);
        Queue priority = queueService.createQueue("prioritet", QueueLevel.SUBSCRIPTION, global.uuid, null, null,
                QueueType.PRIORITY);

        // Loftet sidder på de to blade, så et udsolgt arrangement ikke spærrer medlemsoprettelser.
        Queue genericOrganizer = queueService.createQueue("generisk arrangør", QueueLevel.ORGANIZER, shared.uuid,
                null, null);
        queueService.createQueue("generisk event-kø", QueueLevel.EVENT, genericOrganizer.uuid, 25, null);
        queueService.createQueue("generisk medlems-kø", QueueLevel.MEMBERSYSTEM, genericOrganizer.uuid, 25, null);

        Log.info("Dev-seed: basistræet er oprettet");
        return priority;
    }

    private void ensurePriorityOrganizers(Queue priority) {
        int created = 0;
        for (int i = 0; i < ORGANIZERS.length; i++) {
            String organizerName = ORGANIZERS[i];
            String organizerRef = "OrganizerKey: demo-prioritet-" + (i + 1);
            Queue organizer = Queue.find("parent = ?1 and name = ?2", priority, organizerName).firstResult();
            if (organizer == null) {
                organizer = queueService.createQueue(organizerName, QueueLevel.ORGANIZER, priority.uuid,
                        60 + (i % 4) * 20, organizerRef);
                created++;
            }

            for (int eventNo = 1; eventNo <= 2; eventNo++) {
                String eventName = organizerName + " — event " + eventNo;
                Queue event = Queue.find("parent = ?1 and name = ?2", organizer, eventName).firstResult();
                if (event == null) {
                    queueService.createQueue(eventName, QueueLevel.EVENT, organizer.uuid,
                            12 + ((i + eventNo) % 4) * 4,
                            "EventKey: demo-prioritet-" + (i + 1) + "-" + eventNo);
                    created++;
                }
            }
        }
        if (created > 0) {
            Log.infof("Dev-seed: %d prioritets-knuder oprettet (ingen sessioner — start simulatoren for trafik)",
                    created);
        }
    }
}
