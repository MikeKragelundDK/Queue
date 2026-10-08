package dk.eventit.queue.entity;

/** {@code INITIAL → QUEUED → OPEN → CLOSED}. */
public enum QueueSessionState {

    INITIAL,
    QUEUED,
    /** v1's ACTIVE. */
    OPEN,
    CLOSED
}
