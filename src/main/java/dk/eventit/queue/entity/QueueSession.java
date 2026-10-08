package dk.eventit.queue.entity;

import java.time.Instant;
import java.util.UUID;

import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Index;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

/**
 * {@code @DynamicUpdate} er en samtidighedsregel: uden den skriver hver UPDATE
 * også {@code queue_id}, tager S på kø-rækken, og dirty-markeringens X giver
 * lock upgrade-deadlock.
 */
@Entity
@DynamicUpdate
@Table(name = "queue_session", indexes = {
        @Index(name = "ix_session_uuid", columnList = "uuid", unique = true),
        @Index(name = "ix_session_queue_state_seq", columnList = "queue_id, state, sequence_number"),
        @Index(name = "ix_session_state_created", columnList = "state, created_at"),
        @Index(name = "ix_session_state_closed", columnList = "state, closed_at"),
        @Index(name = "ix_session_state_seq", columnList = "state, sequence_number")
})
public class QueueSession extends BaseEntity {

    /** Kontraktens qsession-token. */
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(nullable = false, updatable = false, length = 36)
    public UUID uuid;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "queue_id", nullable = false)
    public Queue queue;

    @JdbcTypeCode(SqlTypes.VARCHAR)
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    public QueueSessionState state = QueueSessionState.INITIAL;

    /**
     * Afledes af {@code id}, men lodtrækningen permuterer — låseorden skal sortere
     * herpå, ikke på {@code id}. Aldrig unikt indeks: batchet trækning giver
     * kortvarige dubletter.
     */
    @Column(name = "sequence_number", nullable = false)
    public long sequenceNumber;

    @Column(name = "last_ping_at")
    public Instant lastPingAt;

    @Column(name = "opened_at")
    public Instant openedAt;

    @Column(name = "expires_at")
    public Instant expiresAt;

    @Column(name = "closed_at")
    public Instant closedAt;

    /** Spejl af CLOSED-transitionens reason, så opslag slipper for et join. */
    @JdbcTypeCode(SqlTypes.VARCHAR)
    @Enumerated(EnumType.STRING)
    @Column(name = "close_reason", length = 20)
    public CloseReason closeReason;

    @Column(name = "visitor_key", length = 64)
    public String visitorKey;

    @Column(name = "active_visitor_key", insertable = false, updatable = false, length = 64)
    public String activeVisitorKey;

    @Column(name = "event_context", updatable = false, nullable = false)
    public String eventContext = "";

    public static QueueSession findByUuid(UUID uuid) {
        return find("uuid", uuid).firstResult();
    }

    public static QueueSession findLiveForVisitor(Queue queue, String visitorKey, String eventContext) {
        return find("select s from QueueSession s join fetch s.queue "
                + "where s.queue = ?1 and s.activeVisitorKey = ?2 and s.eventContext = ?3", queue, visitorKey, eventContext)
                .firstResult();
    }
}
