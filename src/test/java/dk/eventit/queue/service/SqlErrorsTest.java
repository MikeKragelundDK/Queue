package dk.eventit.queue.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

/** For snævert drukner fejlloggen i deadlocks; for bredt sluges rigtige fejl. */
class SqlErrorsTest {

    private static final int ER_LOCK_DEADLOCK = 1213;
    private static final int ER_LOCK_WAIT_TIMEOUT = 1205;
    private static final int ER_DUP_ENTRY = 1062;

    // === Låse-kontention: koderne ===

    @Test
    void givenDeadlockErrorCode_whenCheckedForLockContention_thenItIsRecognised() {
        // Given
        SQLException error = new SQLException("Deadlock", "HY000", ER_LOCK_DEADLOCK);

        // When
        boolean contention = SqlErrors.isLockContention(error);

        // Then
        assertTrue(contention);
    }

    @Test
    void givenLockWaitTimeoutErrorCode_whenCheckedForLockContention_thenItIsRecognised() {
        // Given: lock wait timeout — ikke en deadlock, men samme behandling
        SQLException error = new SQLException("Timeout", "HY000", ER_LOCK_WAIT_TIMEOUT);

        // When
        boolean contention = SqlErrors.isLockContention(error);

        // Then
        assertTrue(contention);
    }

    @Test
    void givenSerializationFailureSqlState_whenCheckedForLockContention_thenItIsRecognised() {
        // Given: SQLState 40001 uden en genkendelig vendor-kode
        SQLException error = new SQLException("Serialization failure", "40001", 0);

        // When
        boolean contention = SqlErrors.isLockContention(error);

        // Then
        assertTrue(contention);
    }

    // === Låse-kontention: besked-fallback ===

    @Test
    void givenDeadlockMessageWithoutErrorCode_whenCheckedForLockContention_thenItIsRecognised() {
        // Given: kun MySQL's tekst er tilbage
        Exception error = new RuntimeException("Deadlock found when trying to get lock; try restarting transaction");

        // When
        boolean contention = SqlErrors.isLockContention(error);

        // Then
        assertTrue(contention);
    }

    @Test
    void givenLockWaitTimeoutMessageWithoutErrorCode_whenCheckedForLockContention_thenItIsRecognised() {
        // Given
        Exception error = new RuntimeException("Lock wait timeout exceeded; try restarting transaction");

        // When
        boolean contention = SqlErrors.isLockContention(error);

        // Then
        assertTrue(contention);
    }

    // === Dubletnøgle ===

    @Test
    void givenDuplicateEntryErrorCode_whenCheckedForDuplicateKey_thenItIsRecognised() {
        // Given
        SQLException error = new SQLException("Duplikat", "23000", ER_DUP_ENTRY);

        // When
        boolean duplicate = SqlErrors.isDuplicateKey(error);

        // Then
        assertTrue(duplicate);
    }

    @Test
    void givenDuplicateEntryMessageWithoutErrorCode_whenCheckedForDuplicateKey_thenItIsRecognised() {
        // Given
        Exception error = new RuntimeException(
                "Duplicate entry 'org-42' for key 'ux_queue_active_external_reference'");

        // When
        boolean duplicate = SqlErrors.isDuplicateKey(error);

        // Then
        assertTrue(duplicate);
    }

    // === De to prædikater må ikke overlappe ===

    @Test
    void givenDeadlock_whenCheckedForDuplicateKey_thenItIsNotRecognised() {
        // Given: blandes de sammen, dropper consumeren et event ved deadlock
        SQLException error = new SQLException("Deadlock found when trying to get lock", "40001", ER_LOCK_DEADLOCK);

        // When
        boolean duplicate = SqlErrors.isDuplicateKey(error);

        // Then
        assertFalse(duplicate);
    }

    @Test
    void givenDuplicateKey_whenCheckedForLockContention_thenItIsNotRecognised() {
        // Given
        SQLException error = new SQLException("Duplicate entry 'org-42' for key 'ux'", "23000", ER_DUP_ENTRY);

        // When
        boolean contention = SqlErrors.isLockContention(error);

        // Then
        assertFalse(contention);
    }

    // === Gennemløb af exception-grafen: alle tre kanter er bærende ===

    @Test
    void givenDeadlockBuriedInCauseChain_whenCheckedForLockContention_thenItIsRecognised() {
        // Given: SQLException flere lag nede i cause-kæden
        Throwable buried = new RuntimeException("Transaktionen kunne ikke gennemføres",
                new IllegalStateException("Kunne ikke udføre batch",
                        new SQLException("Deadlock", "40001", ER_LOCK_DEADLOCK)));

        // When
        boolean contention = SqlErrors.isLockContention(buried);

        // Then
        assertTrue(contention);
    }

    @Test
    void givenDeadlockAsSuppressedException_whenCheckedForLockContention_thenItIsRecognised() {
        // Given: fejl i beforeCompletion hænger som suppressed, ikke som cause
        Throwable rollback = new RuntimeException("Transaktionen blev rullet tilbage");
        rollback.addSuppressed(new SQLException("Deadlock", "40001", ER_LOCK_DEADLOCK));

        // When
        boolean contention = SqlErrors.isLockContention(rollback);

        // Then
        assertTrue(contention);
    }

    @Test
    void givenDeadlockInNextExceptionChain_whenCheckedForLockContention_thenItIsRecognised() {
        // Given: driverens egen kæde via getNextException
        SQLException first = new SQLException("Batch-opdatering fejlede", "HY000", 0);
        first.setNextException(new SQLException("Deadlock", "40001", ER_LOCK_DEADLOCK));

        // When
        boolean contention = SqlErrors.isLockContention(first);

        // Then
        assertTrue(contention);
    }

    // === Cyklusværn ===

    @Test
    void givenCyclicCauseChain_whenCheckedForLockContention_thenItTerminatesWithoutMatch() {
        // Given: to exceptions, der peger på hinanden
        Throwable outer = new RuntimeException("ydre");
        Throwable inner = new RuntimeException("indre", outer);
        outer.initCause(inner);

        // When
        boolean contention = SqlErrors.isLockContention(outer);

        // Then
        assertFalse(contention);
    }

    @Test
    void givenCyclicSuppressedChainWithDeadlock_whenCheckedForLockContention_thenItIsStillRecognised() {
        // Given: deadlocken ligger inde i en suppressed-cyklus
        Throwable first = new RuntimeException("første");
        Throwable second = new RuntimeException("anden");
        first.addSuppressed(second);
        second.addSuppressed(first);
        second.addSuppressed(new SQLException("Deadlock", "40001", ER_LOCK_DEADLOCK));

        // When
        boolean contention = SqlErrors.isLockContention(first);

        // Then
        assertTrue(contention);
    }

    @Test
    void givenCyclicNextExceptionChain_whenCheckedForLockContention_thenItTerminatesWithoutMatch() {
        // Given: cyklus via nextException
        SQLException first = new SQLException("første", "HY000", 0);
        SQLException second = new SQLException("anden", "HY000", 0);
        first.setNextException(second);
        second.setNextException(first);

        // When
        boolean contention = SqlErrors.isLockContention(first);

        // Then
        assertFalse(contention);
    }

    @Test
    void givenDeadlockReachableOnlyThroughAllThreeEdges_whenCheckedForLockContention_thenItIsRecognised() {
        // Given: deadlock bag cause → suppressed → nextException — alle kanter i én søgning
        SQLException batch = new SQLException("Batch-opdatering fejlede", "HY000", 0);
        batch.setNextException(new SQLException("Deadlock", "40001", ER_LOCK_DEADLOCK));

        Throwable jdbcLayer = new IllegalStateException("Kunne ikke udføre batch");
        jdbcLayer.addSuppressed(batch);
        Throwable outer = new RuntimeException("Transaktionen kunne ikke gennemføres", jdbcLayer);

        // When
        boolean contention = SqlErrors.isLockContention(outer);

        // Then
        assertTrue(contention);
    }

    // === Alt andet skal IKKE genkendes ===

    @Test
    void givenNull_whenChecked_thenNeitherPredicateMatches() {
        // Given / When / Then
        assertFalse(SqlErrors.isLockContention(null));
        assertFalse(SqlErrors.isDuplicateKey(null));
    }

    @Test
    void givenUnrelatedException_whenChecked_thenNeitherPredicateMatches() {
        // Given
        Throwable error = new NullPointerException("Cannot invoke \"Queue.getName()\" because \"queue\" is null");

        // When / Then
        assertFalse(SqlErrors.isLockContention(error));
        assertFalse(SqlErrors.isDuplicateKey(error));
    }

    @Test
    void givenUnrelatedSqlException_whenChecked_thenNeitherPredicateMatches() {
        // Given
        SQLException error = new SQLException("Unknown column 'foo' in 'field list'", "42S22", 1054);

        // When / Then
        assertFalse(SqlErrors.isLockContention(error));
        assertFalse(SqlErrors.isDuplicateKey(error));
    }

    @Test
    void givenExceptionWithoutMessage_whenChecked_thenNeitherPredicateMatches() {
        // Given: null-besked må ikke give NPE
        Throwable error = new RuntimeException();

        // When / Then
        assertFalse(SqlErrors.isLockContention(error));
        assertFalse(SqlErrors.isDuplicateKey(error));
    }
}
