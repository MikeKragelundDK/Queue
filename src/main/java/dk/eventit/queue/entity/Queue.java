package dk.eventit.queue.entity;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

/** Én knude i kø-træet: GLOBAL → SUBSCRIPTION → ORGANIZER → EVENT/MEMBERSYSTEM. */
@Entity
// Samtidighedsregel: uden den skrives parent_id ved hver UPDATE (S-lås på forælderen → deadlock).
@DynamicUpdate
@Table(name = "queue", indexes = {
        @Index(name = "ix_queue_uuid", columnList = "uuid", unique = true),
        @Index(name = "ix_queue_parent", columnList = "parent_id"),
        @Index(name = "ix_queue_dirty", columnList = "dirty"),
        @Index(name = "ix_queue_external_reference", columnList = "external_reference")
})
public class Queue extends BaseEntity {

    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(nullable = false, updatable = false, length = 36)
    public UUID uuid;

    @Column(nullable = false)
    public String name;

    @JdbcTypeCode(SqlTypes.VARCHAR)
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    public QueueLevel level;

    /** Kun på SUBSCRIPTION. */
    @JdbcTypeCode(SqlTypes.VARCHAR)
    @Enumerated(EnumType.STRING)
    @Column(name = "queue_type", length = 16)
    public QueueType queueType;

    /** Null på de generiske normal-knuder, så de ikke kan findes ved nøgleopslag. */
    @Column(name = "external_reference")
    public String externalReference;

    /** Null = intet loft. */
    @Column(name = "max_capacity")
    public Integer maxCapacity;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "parent_id")
    public Queue parent;

    @OneToMany(mappedBy = "parent")
    public List<Queue> children = new ArrayList<>();

    @Column(nullable = false)
    public boolean dirty;

    @Column(name = "archived_at")
    public Instant archivedAt;

    /** Cascader nedad, så bladet alene afgør routingen (KPM-88). */
    @Column(name = "draining_at")
    public Instant drainingAt;

    @Column(name = "opens_at")
    public Instant opensAt;

    /** Null = ingen pre-queue. */
    @Column(name = "sales_start_at")
    public Instant salesStartAt;

    /** Sættes sidst i egen transaktion, så en afbrudt trækning tages forfra. */
    @Column(name = "drawn_at")
    public Instant drawnAt;

    /** Claimes én gang, så alle pods trækker samme rækkefølge. */
    @Column(name = "draw_seed")
    public String drawSeed;

    public static Queue findByUuid(UUID uuid) {
        return find("uuid", uuid).firstResult();
    }

    public static String organizerReference(String organizerKey) {
        return "OrganizerKey: " + organizerKey;
    }

    public static String leafReference(QueueLevel level, String key) {
        return (level == QueueLevel.MEMBERSYSTEM ? "MemberSystemKey: " : "EventKey: ") + key;
    }

    public static Queue findActiveByReference(QueueLevel level, String externalReference) {
        return find("level = ?1 and externalReference = ?2 and archivedAt is null",
                level, externalReference).firstResult();
    }
}
