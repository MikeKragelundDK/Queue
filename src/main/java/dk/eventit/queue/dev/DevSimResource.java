package dk.eventit.queue.dev;

import java.util.Locale;
import java.util.UUID;

import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

/** Svarer 404 uden {@code queue.dev-tools.enabled} — GUI'et skjuler så panelet. */
@Path("/api/dev/sim")
@Produces(MediaType.APPLICATION_JSON)
@IfBuildProperty(name = "queue.dev-tools.enabled", stringValue = "true")
public class DevSimResource {

    @Inject
    DevTrafficSimulator simulator;

    @GET
    public DevTrafficSimulator.SimStatus status() {
        return simulator.status();
    }

    /** {@code hotEvent} + {@code hotShare} sender en andel af trafikken til én kø (billetslip). */
    @POST
    @Path("/start")
    public DevTrafficSimulator.SimStatus start(@QueryParam("mode") String mode, @QueryParam("rate") String rate,
            @QueryParam("hotEvent") String hotEvent, @QueryParam("hotShare") String hotShare) {
        return simulator.start(parseMode(mode), parseDouble(rate, "rate"),
                parseUuid(hotEvent), parseDouble(hotShare, "hotShare"));
    }

    @POST
    @Path("/stop")
    public DevTrafficSimulator.SimStatus stop() {
        return simulator.stop();
    }

    private static DevTrafficSimulator.Mode parseMode(String mode) {
        if (mode == null || mode.isBlank()) {
            throw new IllegalArgumentException("Angiv mode: FAST, SLOW eller EXTREME");
        }
        try {
            return DevTrafficSimulator.Mode.valueOf(mode.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Ukendt mode '" + mode + "' — brug FAST, SLOW eller EXTREME");
        }
    }

    /** Parses manuelt: JAX-RS giver 404 på konverteringsfejl i en {@code @QueryParam}. */
    private static Double parseDouble(String value, String field) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Double.valueOf(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Ugyldig " + field + " '" + value + "' — angiv et tal");
        }
    }

    private static UUID parseUuid(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Ugyldigt hotEvent '" + value + "' — angiv en kø-uuid");
        }
    }
}
