package dk.eventit.queue.web;

import java.util.Map;

import dk.eventit.queue.service.SqlErrors;
import io.quarkus.logging.Log;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Uventede fejl → ERROR-log og generisk 500. Låsekontention er forventet drift
 * og giver 409 uden ERROR-støj.
 */
@Provider
public class UnexpectedExceptionMapper implements ExceptionMapper<Throwable> {

    @Override
    public Response toResponse(Throwable exception) {
        if (exception instanceof WebApplicationException webEx) {
            return webEx.getResponse();
        }
        if (SqlErrors.isLockContention(exception)) {
            Log.debug("API-kald ramte låsekontention — klienten bedes prøve igen", exception);
            return Response.status(Response.Status.CONFLICT)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(Map.of("error", "Systemet var optaget et øjeblik — prøv igen"))
                    .build();
        }
        if (SqlErrors.isDuplicateKey(exception)) {
            // Dublet på ekstern nøgle er en valideringsfejl, ikke noget at prøve igen på.
            Log.warnf("Afvist API-kald: ekstern nøgle er allerede i brug (%s)", exception.getMessage());
            return Response.status(Response.Status.BAD_REQUEST)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(Map.of("error", "Der findes allerede en aktiv kø med den eksterne nøgle"))
                    .build();
        }
        Log.error("Uventet fejl i API-kald", exception);
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .type(MediaType.APPLICATION_JSON)
                .entity(Map.of("error", "Intern serverfejl — se Logs-fanen"))
                .build();
    }
}
