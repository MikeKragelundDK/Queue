package dk.eventit.queue.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** Fler-pod-låse; én række pr. job (id 1–5). */
@Entity
@Table(name = "queue_activation_lock")
public class QueueActivationLock extends PanacheEntityBase {

    @Id
    public Integer id;
}
