package dk.eventit.queue.web;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

/**
 * Staging-kø, så logging-tråden aldrig rører databasen. Overløb dropper de
 * ældste og tælles til én opsummeringsrække.
 */
@ApplicationScoped
public class ErrorLogBuffer {

    private static final int MAX_ENTRIES = 500;
    /** Kolonnen er VARCHAR(4000). */
    private static final int MAX_MESSAGE_LENGTH = 4000;

    private final ConcurrentLinkedDeque<ErrorEntry> entries = new ConcurrentLinkedDeque<>();
    private final AtomicLong dropped = new AtomicLong();
    private final SimpleFormatter formatter = new SimpleFormatter();
    private final Handler handler = new Handler() {
        @Override
        public void publish(LogRecord record) {
            if (record.getLevel().intValue() < Level.WARNING.intValue()) {
                return;
            }
            String message = formatter.formatMessage(record);
            if (record.getThrown() != null) {
                message += " — " + record.getThrown();
            }
            if (message.length() > MAX_MESSAGE_LENGTH) {
                message = message.substring(0, MAX_MESSAGE_LENGTH);
            }
            String logger = record.getLoggerName() != null ? record.getLoggerName() : "ukendt";
            add(new ErrorEntry(Instant.ofEpochMilli(record.getMillis()),
                    record.getLevel().getName(), logger, message));
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    };

    void onStart(@Observes StartupEvent event) {
        java.util.logging.Logger.getLogger("").addHandler(handler);
    }

    /** Ellers stabler dev-modes hot reloads handlers op. */
    void onStop(@Observes ShutdownEvent event) {
        java.util.logging.Logger.getLogger("").removeHandler(handler);
    }

    void add(ErrorEntry entry) {
        entries.addFirst(entry);
        trim();
    }

    List<ErrorEntry> drain() {
        List<ErrorEntry> batch = new ArrayList<>();
        ErrorEntry entry;
        while ((entry = entries.pollLast()) != null) {
            batch.add(entry);
        }
        return batch;
    }

    long takeDropped() {
        return dropped.getAndSet(0);
    }

    void requeue(List<ErrorEntry> batch, long droppedCount) {
        for (int i = batch.size() - 1; i >= 0; i--) {
            entries.addLast(batch.get(i));
        }
        dropped.addAndGet(droppedCount);
        trim();
    }

    private void trim() {
        while (entries.size() > MAX_ENTRIES) {
            if (entries.pollLast() != null) {
                dropped.incrementAndGet();
            }
        }
    }

    public record ErrorEntry(Instant timestamp, String level, String logger, String message) {
    }
}
