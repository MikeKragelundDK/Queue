package dk.eventit.queue.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueSession;
import dk.eventit.queue.entity.QueueSessionStateChange;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;

@QuarkusTest
class QueueDrainingServiceTest {

    @Inject
    QueueService queueService;

    @Inject
    QueueDrainingService drainingService;

    private UUID committedRootUuid;

    @org.junit.jupiter.api.AfterEach
    void cleanUp() {
        if (committedRootUuid == null) {
            return;
        }
        QuarkusTransaction.requiringNew().run(() -> {
            Queue root = Queue.findByUuid(committedRootUuid);
            if (root != null) {
                deleteSubtree(root);
            }
        });
        committedRootUuid = null;
    }

    @Test
    @TestTransaction
    void givenBranchWithLeaves_whenDraining_thenFlagCascadesDownwards() {
        // Given
        Branch branch = branch();

        // When
        queueService.drainQueue(branch.organizer().uuid);

        // Then: cascader nedad, så routingen kan afgøre det på bladet alene
        assertNotNull(branch.organizer().drainingAt);
        assertNotNull(branch.event().drainingAt, "dræningen cascader nedad");
        assertNull(branch.priority().drainingAt, "aldrig opad — abonnementet dræner ikke");
    }

    @Test
    @TestTransaction
    void givenSeparatelyDrainedLeaf_whenStoppingDrainingOnBranch_thenLeafKeepsDraining() {
        // Given: bladet er sat til at dræne for sig selv, og grenen bagefter.
        Branch branch = branch();
        queueService.drainQueue(branch.event().uuid);
        Instant leafDrainingAt = branch.event().drainingAt;
        queueService.drainQueue(branch.organizer().uuid);

        // When: kunden opgraderer igen
        queueService.stopDraining(branch.organizer().uuid);

        // Then: kun den ryddede ombæring forsvinder, som ved genaktivering
        assertNull(branch.organizer().drainingAt);
        assertEquals(leafDrainingAt, branch.event().drainingAt, "bladets egen dræning består");
    }

    @Test
    @TestTransaction
    void givenDrainedBranchWithoutLiveSessions_whenSweeping_thenItIsArchived() {
        // Given: tom gren under dræning
        Branch branch = branch();
        queueService.drainQueue(branch.organizer().uuid);

        // When
        int archived = drainingService.archiveIfDrained(branch.organizer().uuid);

        // Then
        assertEquals(1, archived);
        assertNotNull(branch.organizer().archivedAt);
        assertNotNull(branch.event().archivedAt, "arkivering cascader nedad som altid");
    }

    @Test
    @TestTransaction
    void givenDrainingBranchWithLiveSession_whenSweeping_thenItSurvives() {
        // Given: nogen står stadig i grenen
        Branch branch = branch();
        queueService.enqueue(branch.event().uuid);
        queueService.drainQueue(branch.organizer().uuid);

        // When
        int archived = drainingService.archiveIfDrained(branch.organizer().uuid);

        // Then: grenen skal tømmes først
        assertEquals(0, archived);
        assertNull(branch.organizer().archivedAt);
    }

    @Test
    @TestTransaction
    void givenBranchNoLongerDraining_whenSweeping_thenItIsLeftAlone() {
        // Given: kunden opgraderede igen, inden jobbet nåede frem
        Branch branch = branch();
        queueService.drainQueue(branch.organizer().uuid);
        queueService.stopDraining(branch.organizer().uuid);

        // When
        int archived = drainingService.archiveIfDrained(branch.organizer().uuid);

        // Then
        assertEquals(0, archived);
        assertNull(branch.organizer().archivedAt);
    }

    /** Eneste dækning af lås-rækken (id 5) og completed-rapporteringen. */
    @Test
    void givenCommittedDrainedBranch_whenSweepRuns_thenItIsArchivedAndRunIsReportedComplete() {
        // Given
        UUID organizerUuid = commitBranch();

        try {
            // When
            QueueDrainingService.DrainingResult result = drainingService.sweep();

            // Then
            assertTrue(result.lockAcquired(), "kørslen skal have fået fler-pod-låsen");
            assertTrue(result.completed(), "låsen blev holdt hele vejen igennem");
            assertTrue(result.archived() >= 1);
            QuarkusTransaction.requiringNew().run(() -> {
                Queue organizer = Queue.findByUuid(organizerUuid);
                assertNotNull(organizer.archivedAt, "den tømte gren skal være arkiveret");
                assertNotNull(organizer.drainingAt, "dræningen bliver stående som årsagen til arkiveringen");
            });
        } finally {
            cleanUp();
        }
    }

    // === Hjælpere ===

    /** Ingen NORMAL-knude, så committede fixtures ikke rammer det unikke type-indeks. */
    private Branch branch() {
        Queue global = queueService.createQueue("global", QueueLevel.GLOBAL, null, 1000, null);
        Queue priority = queueService.createQueue("prioritet-" + UUID.randomUUID(), QueueLevel.SUBSCRIPTION,
                global.uuid, null, null);
        Queue organizer = queueService.createQueue("kunde", QueueLevel.ORGANIZER, priority.uuid, 60,
                Queue.organizerReference("org-" + UUID.randomUUID()));
        Queue event = queueService.createQueue("kunde-event", QueueLevel.EVENT, organizer.uuid, 20,
                Queue.leafReference(QueueLevel.EVENT, "evt-" + UUID.randomUUID()));
        return new Branch(global, priority, organizer, event);
    }

    private UUID commitBranch() {
        Committed committed = QuarkusTransaction.requiringNew().call(() -> {
            Branch b = branch();
            queueService.drainQueue(b.organizer().uuid);
            return new Committed(b.global().uuid, b.organizer().uuid);
        });
        committedRootUuid = committed.rootUuid();
        return committed.organizerUuid();
    }

    private void deleteSubtree(Queue node) {
        for (Queue child : Queue.<Queue>list("parent", node)) {
            deleteSubtree(child);
        }
        QueueSessionStateChange.delete(
                "session.id in (select s.id from QueueSession s where s.queue = ?1)", node);
        QueueSession.delete("queue", node);
        node.delete();
    }

    private record Branch(Queue global, Queue priority, Queue organizer, Queue event) {
    }

    private record Committed(UUID rootUuid, UUID organizerUuid) {
    }
}
