package dk.eventit.queue.service;

import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.Scheduled.ConcurrentExecution;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class QueueRetentionJob {

    @Inject
    QueueRetentionService retentionService;

    @Scheduled(cron = "${queue.retention.cron:0 0 2 * * ?}", timeZone = "Europe/Copenhagen",
            concurrentExecution = ConcurrentExecution.SKIP)
    void nightlyRetention() {
        try {
            QueueRetentionService.RetentionResult result = retentionService.run();
            if (!result.lockAcquired()) {
                return;
            }
            if (result.deletedSessions() > 0 || result.deletedQueues() > 0) {
                Log.infof("Retention: slettede %d gamle sessioner og %d udtjente arkiverede køer%s",
                        result.deletedSessions(), result.deletedQueues(),
                        result.completed() ? "" : " (låsen blev tabt undervejs — en anden pod tager resten)");
            }
            if (!result.completed()) {
                Log.warn("Retention-natjobbet nåede ikke igennem — fler-pod-låsen blev overtaget undervejs");
            }
        } catch (Exception e) {
            Log.error("Retention-natjobbet fejlede", e);
        }
    }
}
