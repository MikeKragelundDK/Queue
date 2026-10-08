package dk.eventit.queue.metrics;

import java.util.concurrent.atomic.AtomicBoolean;

import dk.eventit.queue.service.QueueActivationService;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.Scheduled.ConcurrentExecution;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class QueueMetricsJob {

    @Inject
    QueueActivationService activationService;

    @Inject
    QueueActivationMetrics metrics;

    private final AtomicBoolean failureLogged = new AtomicBoolean();

    @Scheduled(every = "${queue.metrics.snapshot-interval:30s}", concurrentExecution = ConcurrentExecution.SKIP)
    void recordBacklog() {
        try {
            metrics.recordBacklog(activationService.backlogSnapshot());
            failureLogged.set(false);
        } catch (Exception e) {
            if (failureLogged.compareAndSet(false, true)) {
                Log.warn("Kø-metrics kunne ikke opdateres", e);
            }
        }
    }
}
