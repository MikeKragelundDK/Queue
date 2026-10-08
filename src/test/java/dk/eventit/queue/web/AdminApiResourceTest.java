package dk.eventit.queue.web;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import dk.eventit.queue.service.QueueDrawService;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;

/** REST-kald committer data, så alle navne er unikke. */
@QuarkusTest
class AdminApiResourceTest {

    @Inject
    ErrorLogFlusher errorLogFlusher;

    @Inject
    QueueDrawService drawService;

    @Test
    void givenFlushedError_whenFetchingErrorsViaApi_thenEntryAppearsWithPod() {
        // Given: en fanget WARN, flushet til error_log-tabellen
        String marker = "api-fejllog-" + UUID.randomUUID();
        org.jboss.logging.Logger.getLogger("api-fejllog-test").warn(marker);
        errorLogFlusher.flush();

        // When/Then: inkl. afsender-pod
        given().when().get("/api/errors")
                .then().statusCode(200)
                .body("message", hasItem(marker))
                .body("find { it.message == '" + marker + "' }.pod", not(emptyOrNullString()));
    }

    @Test
    void givenQueueCreatedViaApi_whenListingRoots_thenItAppearsWithCounts() {
        // Given
        String name = "api-opret-" + UUID.randomUUID();

        // When
        given().contentType(ContentType.JSON)
                .body("""
                        {"name": "%s", "level": "GLOBAL", "parentUuid": null,
                         "maxCapacity": 25, "externalReference": null}
                        """.formatted(name))
                .when().post("/api/queues")
                .then().statusCode(200)
                .body("name", equalTo(name))
                .body("maxCapacity", equalTo(25))
                .body("dirty", equalTo(false))
                .body("childCount", equalTo(0));

        // Then
        given().when().get("/api/queues/roots")
                .then().statusCode(200)
                .body("name", hasItem(name));
    }

    @Test
    void givenQueue_whenEnqueueingAndClosingViaApi_thenSessionGoesInitialToClosed() {
        // Given
        String queueUuid = createGlobalQueue("api-flow-" + UUID.randomUUID());

        // When: enqueue
        String sessionUuid = given()
                .when().post("/api/queues/" + queueUuid + "/enqueue")
                .then().statusCode(200)
                .body("state", equalTo("INITIAL"))
                .body("sequenceNumber", notNullValue())
                .extract().path("uuid");

        // Then: close er idempotent; uden årsag registreres ADMIN
        given().when().get("/api/queues/" + queueUuid + "/sessions")
                .then().statusCode(200)
                .body("uuid", hasItem(sessionUuid));
        given().when().post("/api/sessions/" + sessionUuid + "/close")
                .then().statusCode(200)
                .body("state", equalTo("CLOSED"))
                .body("closedAt", notNullValue())
                .body("closeReason", equalTo("ADMIN"));
        given().when().post("/api/sessions/" + sessionUuid + "/close")
                .then().statusCode(200)
                .body("state", equalTo("CLOSED"));
    }

    @Test
    void givenQueue_whenUpdatingCapacityViaApi_thenNewLimitIsReturnedAndQueueIsDirty() {
        // Given
        String queueUuid = createGlobalQueue("api-loft-" + UUID.randomUUID());

        // When/Then
        given().contentType(ContentType.JSON)
                .body("{\"maxCapacity\": 37}")
                .when().post("/api/queues/" + queueUuid + "/capacity")
                .then().statusCode(200)
                .body("maxCapacity", equalTo(37))
                .body("dirty", equalTo(true));

        given().contentType(ContentType.JSON)
                .body("{\"maxCapacity\": -1}")
                .when().post("/api/queues/" + queueUuid + "/capacity")
                .then().statusCode(400)
                .body("error", containsString("nul eller et positivt"));
    }

    @Test
    void givenBranch_whenFetchingChildrenAndSearching_thenTreeCanBeNavigatedLazily() {
        // Given
        String rootName = "api-rod-" + UUID.randomUUID();
        String rootUuid = createGlobalQueue(rootName);
        String subName = "api-abo-" + UUID.randomUUID();
        given().contentType(ContentType.JSON)
                .body("""
                        {"name": "%s", "level": "SUBSCRIPTION", "parentUuid": "%s",
                         "maxCapacity": 50, "externalReference": null}
                        """.formatted(subName, rootUuid))
                .when().post("/api/queues")
                .then().statusCode(200);

        // When/Then: lazy børn
        given().when().get("/api/queues/" + rootUuid + "/children")
                .then().statusCode(200)
                .body("name", hasItem(subName));

        // When/Then: søgning med forfadersti
        given().when().get("/api/queues/search?query=" + subName)
                .then().statusCode(200)
                .body("queue.name", hasItem(subName))
                .body("[0].ancestors", hasItem(rootUuid));
    }

    @Test
    void givenQueue_whenArchivingAndDeletingViaApi_thenItLeavesTreeButStaysUntilPermanentDelete() {
        // Given
        String name = "api-slet-" + UUID.randomUUID();
        String queueUuid = createGlobalQueue(name);

        // When/Then
        given().when().delete("/api/queues/" + queueUuid)
                .then().statusCode(400)
                .body("error", containsString("arkivér"));

        // When: arkivér — kun synlig med includeArchived
        given().when().post("/api/queues/" + queueUuid + "/archive")
                .then().statusCode(200)
                .body("archivedAt", notNullValue());
        given().when().get("/api/queues/roots")
                .then().statusCode(200)
                .body("name", not(hasItem(name)));
        given().when().get("/api/queues/roots?includeArchived=true")
                .then().statusCode(200)
                .body("name", hasItem(name));

        // Then
        given().when().delete("/api/queues/" + queueUuid)
                .then().statusCode(204);
        given().when().get("/api/queues/roots?includeArchived=true")
                .then().statusCode(200)
                .body("name", not(hasItem(name)));
    }

    @Test
    void givenSubscriptionWithoutParent_whenCreatingViaApi_thenBadRequestWithMessage() {
        given().contentType(ContentType.JSON)
                .body("""
                        {"name": "api-fejl-%s", "level": "SUBSCRIPTION", "parentUuid": null,
                         "maxCapacity": null, "externalReference": null}
                        """.formatted(UUID.randomUUID()))
                .when().post("/api/queues")
                .then().statusCode(400)
                .body("error", containsString("parent"));
    }

    @Test
    void givenStatsEndpoints_whenFetching_thenSeriesIntervalsAndLiveArePresent() {
        // Default 24 t → time-buckets
        given().when().get("/api/stats")
                .then().statusCode(200)
                .body("bucketHours", equalTo(1))
                .body("arrivals.size()", equalTo(24))
                .body("waitByHourOfDay.size()", equalTo(24))
                .body("waitTime.samples", notNullValue())
                .body("abandonment.ratePct", notNullValue())
                .body("truncated", equalTo(false));

        // 7 d → 6-timers buckets; ugyldigt → 400
        given().when().get("/api/stats?hours=168")
                .then().statusCode(200)
                .body("bucketHours", equalTo(6))
                .body("arrivals.size()", equalTo(28));
        given().when().get("/api/stats?hours=999999")
                .then().statusCode(400);
        given().when().get("/api/stats?queue=" + UUID.randomUUID())
                .then().statusCode(400);

        given().when().get("/api/stats/live")
                .then().statusCode(200)
                .body("waitingNow", notNullValue())
                .body("openNow", notNullValue())
                .body("arrivalsLastHour", notNullValue())
                .body("perSubscription", notNullValue());
    }

    @Test
    void givenLeafQueue_whenSettingAndClearingSalesStartViaApi_thenTheValueIsReturnedAndCanBeRemoved() {
        // Given
        String leafUuid = createLeafQueue("api-salgsstart");
        String salgsstart = "2099-01-01T09:00:00Z";

        // When/Then
        given().contentType(ContentType.JSON)
                .body("{\"salesStartAt\": \"" + salgsstart + "\"}")
                .when().post("/api/queues/" + leafUuid + "/schedule")
                .then().statusCode(200)
                .body("salesStartAt", equalTo(salgsstart))
                .body("drawnAt", emptyOrNullString());

        // When/Then: null rydder
        given().contentType(ContentType.JSON)
                .body("{\"salesStartAt\": null}")
                .when().post("/api/queues/" + leafUuid + "/schedule")
                .then().statusCode(200)
                .body("salesStartAt", emptyOrNullString());
    }

    @Test
    void givenLeafQueue_whenSettingWindowViaApi_thenBothTimesAreReturnedAndWindowAloneIsRejected() {
        // Given
        String leafUuid = createLeafQueue("api-vindue");
        String salgsstart = "2099-01-01T10:00:00Z";
        String aabner = "2099-01-01T08:00:00Z";

        // When/Then
        given().contentType(ContentType.JSON)
                .body("{\"opensAt\": \"" + aabner + "\", \"salesStartAt\": \"" + salgsstart + "\"}")
                .when().post("/api/queues/" + leafUuid + "/schedule")
                .then().statusCode(200)
                .body("opensAt", equalTo(aabner))
                .body("salesStartAt", equalTo(salgsstart));

        // When/Then
        given().contentType(ContentType.JSON)
                .body("{\"opensAt\": \"" + aabner + "\", \"salesStartAt\": null}")
                .when().post("/api/queues/" + leafUuid + "/schedule")
                .then().statusCode(400)
                .body("error", containsString("salgsstart"));
    }

    @Test
    void givenGlobalQueue_whenSettingSalesStartViaApi_thenBadRequestWithMessage() {
        // Given
        String queueUuid = createGlobalQueue("api-salgsstart-niveau-" + UUID.randomUUID());

        // When/Then: 400, ikke 500
        given().contentType(ContentType.JSON)
                .body("{\"salesStartAt\": \"2099-01-01T09:00:00Z\"}")
                .when().post("/api/queues/" + queueUuid + "/schedule")
                .then().statusCode(400)
                .body("error", containsString("event eller et medlemssystem"));
    }

    @Test
    void givenPassedSalesStart_whenDrawHasRun_thenTimestampIsExposedAndFieldIsLocked() {
        // Given: salgsstart i fortiden
        String leafUuid = createLeafQueue("api-trukket");
        given().contentType(ContentType.JSON)
                .body("{\"salesStartAt\": \"2020-01-01T09:00:00Z\"}")
                .when().post("/api/queues/" + leafUuid + "/schedule")
                .then().statusCode(200);

        // When: scheduleren er slukket i tests
        drawService.run();

        // Then
        given().when().get("/api/queues/" + leafUuid)
                .then().statusCode(200)
                .body("drawnAt", notNullValue());

        // og feltet er låst bagefter
        given().contentType(ContentType.JSON)
                .body("{\"salesStartAt\": \"2099-01-01T09:00:00Z\"}")
                .when().post("/api/queues/" + leafUuid + "/schedule")
                .then().statusCode(400)
                .body("error", containsString("lodtrukket"));
    }

    private String createLeafQueue(String prefix) {
        String suffix = "-" + UUID.randomUUID();
        String globalUuid = createGlobalQueue(prefix + "-global" + suffix);
        String subscriptionUuid = createChildQueue(prefix + "-abo" + suffix, "SUBSCRIPTION", globalUuid);
        String organizerUuid = createChildQueue(prefix + "-arrangør" + suffix, "ORGANIZER", subscriptionUuid);
        return createChildQueue(prefix + "-event" + suffix, "EVENT", organizerUuid);
    }

    private String createChildQueue(String name, String level, String parentUuid) {
        return given().contentType(ContentType.JSON)
                .body("""
                        {"name": "%s", "level": "%s", "parentUuid": "%s",
                         "maxCapacity": null, "externalReference": null}
                        """.formatted(name, level, parentUuid))
                .when().post("/api/queues")
                .then().statusCode(200)
                .extract().path("uuid");
    }

    private String createGlobalQueue(String name) {
        return given().contentType(ContentType.JSON)
                .body("""
                        {"name": "%s", "level": "GLOBAL", "parentUuid": null,
                         "maxCapacity": null, "externalReference": null}
                        """.formatted(name))
                .when().post("/api/queues")
                .then().statusCode(200)
                .extract().path("uuid");
    }
}
