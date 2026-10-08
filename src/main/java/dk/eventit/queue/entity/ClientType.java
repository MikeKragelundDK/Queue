package dk.eventit.queue.entity;

/**
 * Afgør bladet, når nøglen er ukendt. Fælleskøen har to blade, så et udsolgt
 * arrangement ikke spærrer for medlemsoprettelser (KPM-87).
 */
public enum ClientType {

    EVENT_BOOKING(QueueLevel.EVENT),
    MEMBER_SIGNUP(QueueLevel.MEMBERSYSTEM);

    private final QueueLevel leafLevel;

    ClientType(QueueLevel leafLevel) {
        this.leafLevel = leafLevel;
    }

    public QueueLevel leafLevel() {
        return leafLevel;
    }
}
