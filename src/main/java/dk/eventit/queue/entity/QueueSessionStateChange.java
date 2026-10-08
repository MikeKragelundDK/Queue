package dk.eventit.queue.entity;

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

/** Transitionslog; skrives altid i samme transaktion som skiftet. Immutabel. */
@Entity
@Table(name = "queue_session_state_change", indexes = {
        @Index(name = "ix_state_change_session", columnList = "session_id"),
        @Index(name = "ix_state_change_state_time", columnList = "state, created_at")
})
public class QueueSessionStateChange extends BaseEntity {

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "session_id", nullable = false)
    public QueueSession session;

    @JdbcTypeCode(SqlTypes.VARCHAR)
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    public QueueSessionState state;

    /** Kun sat for CLOSED. */
    @JdbcTypeCode(SqlTypes.VARCHAR)
    @Enumerated(EnumType.STRING)
    @Column(length = 20)
    public CloseReason reason;
}
