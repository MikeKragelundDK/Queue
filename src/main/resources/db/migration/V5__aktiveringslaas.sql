-- Singleton-række til aktiveringsmotorens fler-pod-lås.

CREATE TABLE queue_activation_lock
(
    id INT NOT NULL PRIMARY KEY
) ENGINE = InnoDB;

INSERT INTO queue_activation_lock (id) VALUES (1);
