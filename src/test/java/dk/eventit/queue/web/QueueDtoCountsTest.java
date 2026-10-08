package dk.eventit.queue.web;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import dk.eventit.queue.entity.CloseReason;
import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueSession;
import dk.eventit.queue.service.QueueService;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;

/** Eksakte tal på hvert niveau: implicitte inner joins og sammenblandede tilstande fejler ikke synligt. */
@QuarkusTest
class QueueDtoCountsTest {

    @Inject
    QueueService queueService;

    @Test
    @TestTransaction
    void givenSessionsOnLeaves_whenBuildingDtos_thenCountsAggregateForWholeSubtree() {
        // Given
        Fixture fixture = fixture();
        queueService.enqueue(fixture.eventA.uuid);
        queueService.enqueue(fixture.eventA.uuid);
        queueService.enqueue(fixture.eventB.uuid);

        // When
        List<AdminApiResource.QueueDto> dtos = AdminApiResource.toDtos(
                List.of(fixture.global, fixture.subscription, fixture.organizer, fixture.eventA, fixture.eventB));

        // Then: hvert niveau ser summen af sit undertræ
        assertEquals(3, dtos.get(0).initial(), "GLOBAL ser alle tre");
        assertEquals(3, dtos.get(1).initial(), "SUBSCRIPTION ser alle tre");
        assertEquals(3, dtos.get(2).initial(), "ORGANIZER ser alle tre");
        assertEquals(2, dtos.get(3).initial(), "event A ser sine egne to");
        assertEquals(1, dtos.get(4).initial(), "event B ser sin ene");
    }

    @Test
    @TestTransaction
    void givenSessionDirectlyOnSubscriptionQueue_whenBuildingDtos_thenShortPathIsCountedToo() {
        // Given: session på en ikke-blad-knude — to forfader-kolonner er null
        Fixture fixture = fixture();
        queueService.enqueue(fixture.subscription.uuid);

        // When
        List<AdminApiResource.QueueDto> dtos = AdminApiResource.toDtos(
                List.of(fixture.global, fixture.subscription, fixture.organizer));

        // Then
        assertEquals(1, dtos.get(0).initial(), "GLOBAL ser den korte sti");
        assertEquals(1, dtos.get(1).initial(), "abonnementet ser sin egen");
        assertEquals(0, dtos.get(2).initial(), "arrangøren ligger ved siden af, ikke over");
    }

    @Test
    @TestTransaction
    void givenSessionsInDifferentStates_whenBuildingDtos_thenEachStateIsCountedSeparately() {
        // Given
        Fixture fixture = fixture();
        queueService.enqueue(fixture.eventA.uuid);
        queueService.enqueue(fixture.eventA.uuid);
        QueueSession closed = queueService.enqueue(fixture.eventB.uuid);
        queueService.closeSession(closed.uuid, CloseReason.COMPLETED);

        // When
        AdminApiResource.QueueDto global = AdminApiResource.toDtos(List.of(fixture.global)).getFirst();

        // Then
        assertEquals(2, global.initial());
        assertEquals(0, global.queued());
        assertEquals(0, global.open());
        assertEquals(1, global.closed());
    }

    @Test
    @TestTransaction
    void givenNodesWithAndWithoutChildren_whenBuildingDtos_thenChildCountIsPerNode() {
        // Given
        Fixture fixture = fixture();

        // When: begge slags knuder i én batch
        List<AdminApiResource.QueueDto> dtos = AdminApiResource.toDtos(
                List.of(fixture.subscription, fixture.organizer, fixture.eventA));

        // Then
        assertEquals(1, dtos.get(0).childCount(), "abonnementet har én arrangør");
        assertEquals(2, dtos.get(1).childCount(), "arrangøren har to events");
        assertEquals(0, dtos.get(2).childCount(), "et blad har ingen børn");
    }

    @Test
    @TestTransaction
    void givenNoNodes_whenBuildingDtos_thenNoQueriesAreRunAndResultIsEmpty() {
        // Given / When: tom liste må ikke give en tom IN-liste
        List<AdminApiResource.QueueDto> dtos = AdminApiResource.toDtos(List.of());

        // Then
        assertEquals(List.of(), dtos);
    }

    private Fixture fixture() {
        String suffix = "-dto-" + System.nanoTime();
        Queue global = queueService.createQueue("global" + suffix, QueueLevel.GLOBAL, null, 1000, null);
        Queue subscription = queueService.createQueue("abonnement" + suffix, QueueLevel.SUBSCRIPTION,
                global.uuid, null, null);
        Queue organizer = queueService.createQueue("arrangør" + suffix, QueueLevel.ORGANIZER,
                subscription.uuid, null, null);
        Queue eventA = queueService.createQueue("event a" + suffix, QueueLevel.EVENT, organizer.uuid, null, null);
        Queue eventB = queueService.createQueue("event b" + suffix, QueueLevel.EVENT, organizer.uuid, null, null);
        return new Fixture(global, subscription, organizer, eventA, eventB);
    }

    private record Fixture(Queue global, Queue subscription, Queue organizer, Queue eventA, Queue eventB) {
    }
}
