package dk.eventit.queue.service;

import dk.eventit.queue.entity.QueueSession;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityNotFoundException;
import jakarta.persistence.LockModeType;

/**
 * Aldrig {@code em.lock}: den genindlæser ikke felterne, så en state-recheck
 * bagefter tester et forældet snapshot og kan genoplive en lukket session.
 */
final class SessionLocks {

    private SessionLocks() {
    }

    /** {@code false} hvis rækken er slettet imens — spring den over. */
    static boolean lockAndRefresh(EntityManager em, QueueSession session) {
        try {
            em.refresh(session, LockModeType.PESSIMISTIC_WRITE);
            return true;
        } catch (EntityNotFoundException e) {
            return false;
        }
    }
}
