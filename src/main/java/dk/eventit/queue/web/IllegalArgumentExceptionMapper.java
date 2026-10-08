package dk.eventit.queue.web;

import java.util.Map;
import java.util.Objects;

import io.quarkus.logging.Log;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class IllegalArgumentExceptionMapper implements ExceptionMapper<IllegalArgumentException> {

    @Override
    public Response toResponse(IllegalArgumentException exception) {
        // Map.of afviser null, og en besked-løs exception ville ellers give 500.
        String message = Objects.requireNonNullElse(exception.getMessage(), "Ugyldigt kald");
        Log.warnf("Afvist API-kald: %s", message);
        return Response.status(Response.Status.BAD_REQUEST)
                .type(MediaType.APPLICATION_JSON)
                .entity(Map.of("error", message))
                .build();
    }
}
