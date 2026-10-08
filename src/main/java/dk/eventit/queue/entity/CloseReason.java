package dk.eventit.queue.entity;

/** Bevidst ortogonal til {@link QueueSessionState} — bland aldrig årsager ind i state. */
public enum CloseReason {

    COMPLETED,
    ABANDONED,
    EXPIRED,
    ARCHIVED,
    ADMIN
}
