package dk.eventit.queue.messaging;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.service.SqlErrors;
import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Permanente fejl går til DLQ og logges på WARN med payload — connectoren
 * logger selv kun på DEBUG. Forbigående fejl (låsekontention, dubletnøgle fra
 * to pods) prøves igen; en gyldig besked i DLQ'en ville efterlade træet ude af sync.
 */
@ApplicationScoped
public class PlatformEventConsumer {

    private static final int MAX_ATTEMPTS = 3;
    private static final long RETRY_BACKOFF_MILLIS = 50;

    @Inject
    PlatformEventHandler handler;

    @Incoming("platform-events")
    @Blocking
    public void on(JsonObject event) {
        String type = event == null ? null : event.getString("type");
        try {
            for (int attempt = 1; ; attempt++) {
                try {
                    dispatch(event);
                    Log.infof("Platform-event behandlet: %s", type);
                    return;
                } catch (RuntimeException e) {
                    if (attempt >= MAX_ATTEMPTS || !isTransient(e)) {
                        throw e;
                    }
                    Log.debugf("Platform-event '%s' ramte forbigående kontention (forsøg %d/%d) — prøver igen",
                            type, attempt, MAX_ATTEMPTS);
                    backoff();
                }
            }
        } catch (RuntimeException e) {
            Log.warnf(e, "Platform-event afvist til DLQ (type=%s): %s — payload: %s",
                    type, e.getMessage(), event);
            throw e;
        }
    }

    private void dispatch(JsonObject event) {
        if (event == null) {
            throw new IllegalArgumentException("Platform-eventet har ingen payload");
        }
        String type = required(event, "type");
        switch (type) {
            case "organizer.created" -> handler.organizerCreated(
                    required(event, "organizerKey"), required(event, "name"), required(event, "subscription"));

            case "organizer.subscription-changed" -> handler.organizerSubscriptionChanged(
                    required(event, "organizerKey"), required(event, "subscription"));

            case "organizer.archived" -> handler.organizerArchived(required(event, "organizerKey"));

            case "event.created" -> handler.leafCreated(QueueLevel.EVENT,
                    required(event, "eventKey"), required(event, "organizerKey"), required(event, "name"));

            case "event.archived" -> handler.leafArchived(QueueLevel.EVENT, required(event, "eventKey"));

            case "membersystem.created" -> handler.leafCreated(QueueLevel.MEMBERSYSTEM,
                    required(event, "memberSystemKey"), required(event, "organizerKey"), required(event, "name"));

            case "membersystem.archived" -> handler.leafArchived(QueueLevel.MEMBERSYSTEM,
                    required(event, "memberSystemKey"));

            default -> throw new IllegalArgumentException("Ukendt platform-event-type: '" + type + "'");
        }
    }

    private static boolean isTransient(RuntimeException e) {
        return SqlErrors.isLockContention(e) || SqlErrors.isDuplicateKey(e);
    }

    private static void backoff() {
        try {
            Thread.sleep(RETRY_BACKOFF_MILLIS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Afbrudt under retry af platform-event", interrupted);
        }
    }

    private static String required(JsonObject event, String field) {
        String value = event.getString(field);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Platform-eventet mangler feltet '" + field + "': " + event);
        }
        return value;
    }
}
