package dk.eventit.queue.entity;

/**
 * Kun på SUBSCRIPTION (KPM-84). Én aktiv af hver håndhæves af
 * {@code ux_queue_active_type}, ikke koden — to pods kunne begge oprette.
 */
public enum QueueType {

    NORMAL,
    PRIORITY
}
