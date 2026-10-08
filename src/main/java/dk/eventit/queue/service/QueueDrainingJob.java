package dk.eventit.queue.service;

import java.util.concurrent.atomic.AtomicBoolean;

import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.Scheduled.ConcurrentExecution;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class QueueDrainingJob {

    @Inject
    QueueDrainingService drainingService;

    private final AtomicBoolean failureLogged = new AtomicBoolean();

    @Scheduled(every = "${queue.draining.interval:5m}", concurrentExecution = ConcurrentExecution.SKIP)
    void archiveDrainedBranches() {
        try {
            QueueDrainingService.DrainingResult result = drainingService.sweep();
            if (result.archived() > 0) {
                Log.infof("Dræning: arkiverede %d tømte gren(e)%s", result.archived(),
                        result.completed() ? "" : " (låsen blev tabt undervejs — en anden pod tager resten)");
            }
            failureLogged.set(false);
        } catch (Exception e) {
            if (SqlErrors.isLockContention(e)) {
                Log.debug("Dræningsjobbet ramte en deadlock — forventet kontention, næste kørsel prøver igen");
            } else if (failureLogged.compareAndSet(false, true)) {
                Log.error("Dræningsjobbets scheduler-tick fejlede", e);
            }
        }
    }
}
