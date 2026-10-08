package dk.eventit.queue.service;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Forventede MySQL-fejl under last. Den oprindelige {@link SQLException} kan ligge
 * i cause-, suppressed- eller nextException-kæden, så hele grafen gennemløbes.
 */
public final class SqlErrors {

    private static final int ER_LOCK_DEADLOCK = 1213;
    private static final int ER_LOCK_WAIT_TIMEOUT = 1205;
    private static final int ER_DUP_ENTRY = 1062;

    private static final String SQLSTATE_SERIALIZATION_FAILURE = "40001";

    private SqlErrors() {
    }

    public static boolean isLockContention(Throwable error) {
        return anyMatch(error, SqlErrors::indicatesLockContention);
    }

    public static boolean isDuplicateKey(Throwable error) {
        return anyMatch(error, SqlErrors::indicatesDuplicateKey);
    }

    private static boolean anyMatch(Throwable error, Predicate<Throwable> predicate) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<Throwable> pending = new ArrayDeque<>();
        if (error != null) {
            pending.add(error);
        }

        while (!pending.isEmpty()) {
            Throwable current = pending.poll();
            if (!seen.add(current)) {
                continue;
            }
            if (predicate.test(current)) {
                return true;
            }

            if (current.getCause() != null) {
                pending.add(current.getCause());
            }
            Collections.addAll(pending, current.getSuppressed());
            if (current instanceof SQLException sql && sql.getNextException() != null) {
                pending.add(sql.getNextException());
            }
        }
        return false;
    }

    private static boolean indicatesLockContention(Throwable error) {
        if (error instanceof SQLException sql) {
            if (SQLSTATE_SERIALIZATION_FAILURE.equals(sql.getSQLState())) {
                return true;
            }
            if (sql.getErrorCode() == ER_LOCK_DEADLOCK || sql.getErrorCode() == ER_LOCK_WAIT_TIMEOUT) {
                return true;
            }
        }
        String message = error.getMessage();
        return message != null
                && (message.contains("Deadlock found") || message.contains("Lock wait timeout exceeded"));
    }

    private static boolean indicatesDuplicateKey(Throwable error) {
        if (error instanceof SQLException sql && sql.getErrorCode() == ER_DUP_ENTRY) {
            return true;
        }
        String message = error.getMessage();
        return message != null && message.contains("Duplicate entry");
    }
}
