package dk.eventit.queue.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import dk.eventit.queue.entity.ErrorLogEntry;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;

/** Scheduleren er slukket i tests, så flush kaldes eksplicit; asserts scopes med unikke markører. */
@QuarkusTest
class ErrorLogFlusherTest {

    @Inject
    ErrorLogBuffer buffer;

    @Inject
    ErrorLogFlusher flusher;

    @Test
    void givenLoggedWarning_whenFlushing_thenRowIsPersistedWithPodAndLogTimestamp() {
        // Given: en WARN fanget af rod-logger-handleren
        Instant start = Instant.now().minusSeconds(1);
        String marker = "fejllog-" + UUID.randomUUID();
        Logger.getLogger("fejllog-test").warn(marker);

        // When
        flusher.flush();

        // Then
        ErrorLogEntry row = ErrorLogEntry.<ErrorLogEntry>find("message", marker).firstResult();
        assertNotNull(row, "posten er flushet til error_log-tabellen");
        assertTrue(row.level.startsWith("WARN"));
        assertEquals("fejllog-test", row.logger);
        assertFalse(row.pod.isBlank(), "pod-kolonnen viser hvilken instans fejlen kom fra");
        assertTrue(row.createdAt.isAfter(start), "createdAt er logtidspunktet, ikke flush-tidspunktet");
    }

    @Test
    void givenAlreadyFlushedEntry_whenFlushingAgain_thenNoDuplicateIsWritten() {
        // Given: en post, der allerede er drænet og skrevet
        String marker = "fejllog-" + UUID.randomUUID();
        Logger.getLogger("fejllog-test").warn(marker);
        flusher.flush();

        // When
        flusher.flush();

        // Then
        assertEquals(1, ErrorLogEntry.count("message", marker), "anden flush skriver ikke posten igen");
    }

    @Test
    void givenRowOlderThanRetention_whenFlushing_thenRetentionDeletesIt() {
        // Given: en række ældre end retention (30 dage) via forudfyldt createdAt
        String marker = "fejllog-gammel-" + UUID.randomUUID();
        QuarkusTransaction.requiringNew().run(() -> {
            ErrorLogEntry old = new ErrorLogEntry();
            old.createdAt = Instant.now().minus(40, ChronoUnit.DAYS);
            old.level = "ERROR";
            old.logger = "retention-test";
            old.message = marker;
            old.pod = "gammel-pod";
            old.persist();
        });
        assertEquals(1, ErrorLogEntry.count("message", marker));

        // When
        flusher.flush();

        // Then
        assertEquals(0, ErrorLogEntry.count("message", marker), "retention har slettet den gamle række");
    }

    @Test
    void givenMoreEntriesThanBufferCapacity_whenFlushing_thenOldestAreDroppedAndSummarised() {
        // Given: bufferen tømmes og overfyldes derefter (520 > kapaciteten 500)
        buffer.drain();
        buffer.takeDropped();
        Instant start = Instant.now().minusSeconds(1);
        String marker = "storm-" + UUID.randomUUID();
        for (int i = 0; i < 520; i++) {
            buffer.add(new ErrorLogBuffer.ErrorEntry(Instant.now(), "WARN", "storm-test", marker + "-" + i));
        }

        // When
        int written = flusher.flush();

        // Then: kapaciteten + én opsummeringsrække
        assertEquals(500, written, "bufferen dropper ud over sin kapacitet");
        assertEquals(500, ErrorLogEntry.count("message like ?1", marker + "%"));
        ErrorLogEntry summary = ErrorLogEntry.<ErrorLogEntry>find(
                "logger = ?1 and createdAt >= ?2", ErrorLogFlusher.class.getName(), start).firstResult();
        assertNotNull(summary, "fejl-storme opsummeres i én række frem for at drukne databasen");
        assertTrue(summary.message.contains("20 fejlposter droppet"), summary.message);
    }
}
