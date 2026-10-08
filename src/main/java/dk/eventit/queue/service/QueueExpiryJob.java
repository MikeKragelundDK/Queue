package dk.eventit.queue.service;

import java.util.concurrent.atomic.AtomicBoolean;

import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.Scheduled.ConcurrentExecution;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class QueueExpiryJob {

    @Inject
    QueueExpiryService expiryService;

    private final AtomicBoolean failureLogged = new AtomicBoolean();

    @Scheduled(every = "${queue.expiry.interval:30s}", concurrentExecution = ConcurrentExecution.SKIP)
    void sweepExpiredSessions() {
        try {
            QueueExpiryService.ExpiryResult result = expiryService.sweep();
            if (result.total() > 0) {
                Log.infof("Expiry: lukkede %d udløbne (EXPIRED) og %d opgivne (ABANDONED) sessioner%s",
                        result.expired(), result.abandoned(),
                        result.completed() ? "" : " (låsen blev tabt undervejs — en anden pod fejer resten)");
            }
            failureLogged.set(false);
        } catch (Exception e) {
            if (SqlErrors.isLockContention(e)) {
                Log.debug("Expiry-fejningen ramte en deadlock — forventet kontention, næste fejning prøver igen");
            } else if (failureLogged.compareAndSet(false, true)) {
                Log.error("Expiry-jobbets scheduler-tick fejlede", e);
            }
        }
    }
}
