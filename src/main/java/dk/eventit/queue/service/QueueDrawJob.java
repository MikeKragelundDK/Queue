package dk.eventit.queue.service;

import java.util.concurrent.atomic.AtomicBoolean;

import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.Scheduled.ConcurrentExecution;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class QueueDrawJob {

    @Inject
    QueueDrawService drawService;

    private final AtomicBoolean failureLogged = new AtomicBoolean();

    @Scheduled(every = "${queue.draw.interval:5s}", concurrentExecution = ConcurrentExecution.SKIP)
    void drawOpenedQueues() {
        try {
            QueueDrawService.DrawResult result = drawService.run();
            if (result.drawnQueues() > 0) {
                Log.infof("Lodtrækning: blandede %d sessioner i %d kø(er)%s",
                        result.shuffledSessions(), result.drawnQueues(),
                        result.completed() ? "" : " (låsen blev tabt undervejs — en anden pod tager resten)");
            }
            failureLogged.set(false);
        } catch (Exception e) {
            if (SqlErrors.isLockContention(e)) {
                Log.debug("Lodtrækningen ramte en deadlock — forventet kontention, næste tick prøver igen");
            } else if (failureLogged.compareAndSet(false, true)) {
                Log.error("Lodtrækningsjobbets scheduler-tick fejlede", e);
            }
        }
    }
}
