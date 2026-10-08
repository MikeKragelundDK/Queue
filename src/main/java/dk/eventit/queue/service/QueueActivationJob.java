package dk.eventit.queue.service;

import java.util.concurrent.atomic.AtomicBoolean;

import dk.eventit.queue.metrics.QueueActivationMetrics;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.Scheduled.ConcurrentExecution;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class QueueActivationJob {

    @Inject
    QueueActivationService activationService;

    @Inject
    QueueActivationMetrics metrics;

    private final AtomicBoolean failureLogged = new AtomicBoolean();

    @Scheduled(every = "${queue.activation.interval:1s}", concurrentExecution = ConcurrentExecution.SKIP)
    void activateDirtyQueues() {
        long started = System.nanoTime();
        try {
            QueueActivationService.WorkClaim claim = activationService.claimDirtyWork();
            if (claim != QueueActivationService.WorkClaim.CLAIMED) {
                metrics.record(claim, System.nanoTime() - started);
                failureLogged.set(false);
                return;
            }
            QueueActivationService.ActivationResult result = activationService.activateNow();
            metrics.record(result, System.nanoTime() - started);
            failureLogged.set(false);
        } catch (Exception e) {
            metrics.recordFailure(System.nanoTime() - started);
            if (SqlErrors.isLockContention(e)) {
                Log.debug("Aktiveringsmotorens tick ramte en deadlock — forventet kontention, næste tick prøver igen");
            } else if (failureLogged.compareAndSet(false, true)) {
                Log.error("Aktiveringsmotorens scheduler-tick fejlede", e);
            }
        }
    }
}
