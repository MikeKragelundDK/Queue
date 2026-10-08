package dk.eventit.queue.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

/** {@code createdAt} er logtidspunktet, ikke flush-tidspunktet. */
@Entity
@Table(name = "error_log")
public class ErrorLogEntry extends BaseEntity {

    @Column(nullable = false, length = 20)
    public String level;

    @Column(nullable = false)
    public String logger;

    @Column(nullable = false, length = 4000)
    public String message;

    @Column(nullable = false)
    public String pod;
}
