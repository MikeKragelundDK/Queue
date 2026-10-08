package dk.eventit.queue.web;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import dk.eventit.queue.entity.ErrorLogEntry;
import io.quarkus.logging.Log;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Flush-stien må aldrig selv logge WARN+ — fejlen ville lande i bufferen og loope.
 * En fejlet batch lægges tilbage til næste tick.
 */
@ApplicationScoped
public class ErrorLogFlusher {

    @Inject
    ErrorLogBuffer buffer;

    @ConfigProperty(name = "queue.error-log.retention-days", defaultValue = "30")
    int retentionDays;

    private final String pod = resolvePod();

    /** Slukket i tests — dér kaldes {@link #flush()} direkte. */
    @Scheduled(every = "5s")
    void scheduledFlush() {
        flush();
    }

    public int flush() {
        List<ErrorLogBuffer.ErrorEntry> batch = buffer.drain();
        long dropped = buffer.takeDropped();
        try {
            QuarkusTransaction.requiringNew().run(() -> {
                for (ErrorLogBuffer.ErrorEntry entry : batch) {
                    ErrorLogEntry row = new ErrorLogEntry();
                    row.createdAt = entry.timestamp();
                    row.level = entry.level();
                    row.logger = entry.logger();
                    row.message = entry.message();
                    row.pod = pod;
                    row.persist();
                }
                if (dropped > 0) {
                    ErrorLogEntry row = new ErrorLogEntry();
                    row.level = "WARN";
                    row.logger = ErrorLogFlusher.class.getName();
                    row.message = dropped + " fejlposter droppet — bufferen løb over (fejl-storm)";
                    row.pod = pod;
                    row.persist();
                }
                ErrorLogEntry.delete("createdAt < ?1",
                        Instant.now().minus(retentionDays, ChronoUnit.DAYS));
            });
            return batch.size();
        } catch (Exception e) {
            buffer.requeue(batch, dropped);
            // Kun DEBUG — se loop-værnet.
            Log.debugf(e, "Flush af fejllog fejlede — %d poster lagt tilbage til næste forsøg", batch.size());
            return 0;
        }
    }

    private static String resolvePod() {
        String hostname = System.getenv("HOSTNAME");
        if (hostname != null && !hostname.isBlank()) {
            return hostname;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "ukendt";
        }
    }
}
