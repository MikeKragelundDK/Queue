package dk.eventit.queue.web;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import dk.eventit.queue.entity.CloseReason;
import dk.eventit.queue.entity.ErrorLogEntry;
import dk.eventit.queue.entity.Queue;
import dk.eventit.queue.entity.QueueLevel;
import dk.eventit.queue.entity.QueueSession;
import dk.eventit.queue.entity.QueueSessionState;
import dk.eventit.queue.service.QueueService;
import dk.eventit.queue.service.StatsService;
import io.quarkus.panache.common.Page;
import io.quarkus.panache.common.Sort;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

/** Admin-GUI'ets API. Træet hentes lazy — aldrig et endpoint, der returnerer hele træet. */
@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
public class AdminApiResource {

    private static final int SEARCH_LIMIT = 20;

    @Inject
    QueueService queueService;

    @Inject
    StatsService statsService;

    @GET
    @Path("/queues/roots")
    public List<QueueDto> roots(@QueryParam("includeArchived") boolean includeArchived) {
        String query = includeArchived ? "parent is null" : "parent is null and archivedAt is null";
        return toDtos(Queue.list(query, Sort.ascending("id")));
    }

    @GET
    @Path("/queues/{uuid}")
    public QueueDto queue(@PathParam("uuid") UUID uuid) {
        Queue queue = Queue.findByUuid(uuid);
        if (queue == null) {
            throw new IllegalArgumentException("Ukendt kø: " + uuid);
        }
        return QueueDto.of(queue);
    }

    @GET
    @Path("/queues/{uuid}/children")
    public List<QueueDto> children(@PathParam("uuid") UUID uuid,
            @QueryParam("includeArchived") boolean includeArchived) {
        Queue queue = Queue.findByUuid(uuid);
        if (queue == null) {
            throw new IllegalArgumentException("Ukendt kø: " + uuid);
        }
        String query = includeArchived ? "parent = ?1" : "parent = ?1 and archivedAt is null";
        return toDtos(Queue.list(query, Sort.ascending("id"), queue));
    }

    @GET
    @Path("/queues/{uuid}/sessions")
    public List<SessionDto> queueSessions(@PathParam("uuid") UUID uuid) {
        Queue queue = Queue.findByUuid(uuid);
        if (queue == null) {
            throw new IllegalArgumentException("Ukendt kø: " + uuid);
        }
        return QueueSession.<QueueSession>find("queue", Sort.descending("id"), queue)
                .page(Page.ofSize(100)).list()
                .stream().map(SessionDto::of).toList();
    }

    @GET
    @Path("/queues/search")
    public List<SearchHitDto> search(@QueryParam("query") String query,
            @QueryParam("includeArchived") boolean includeArchived) {
        if (query == null || query.trim().length() < 2) {
            return List.of();
        }
        String pattern = "%" + query.trim().toLowerCase() + "%";
        // Arkiverede filtreres fra som i roots/children.
        String filter = includeArchived
                ? "lower(name) like ?1 or lower(externalReference) like ?1"
                : "(lower(name) like ?1 or lower(externalReference) like ?1) and archivedAt is null";
        List<Queue> hits = Queue.<Queue>find(filter, Sort.ascending("name"), pattern)
                .page(Page.ofSize(SEARCH_LIMIT)).list();

        List<QueueDto> dtos = toDtos(hits);
        List<SearchHitDto> result = new ArrayList<>(hits.size());
        for (int i = 0; i < hits.size(); i++) {
            result.add(new SearchHitDto(dtos.get(i), ancestorUuids(hits.get(i))));
        }
        return result;
    }

    // Mutationer er @Transactional, så DTO-mappingen kan læse lazy-associationer.

    @POST
    @Path("/queues")
    @Consumes(MediaType.APPLICATION_JSON)
    @Transactional
    public QueueDto createQueue(CreateQueueRequest request) {
        return QueueDto.of(queueService.createQueue(request.name(), request.level(), request.parentUuid(),
                request.maxCapacity(), request.externalReference()));
    }

    @POST
    @Path("/queues/{uuid}/capacity")
    @Consumes(MediaType.APPLICATION_JSON)
    @Transactional
    public QueueDto updateCapacity(@PathParam("uuid") UUID uuid, UpdateCapacityRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Der mangler et kapacitetsloft");
        }
        return QueueDto.of(queueService.updateMaxCapacity(uuid, request.maxCapacity()));
    }

    /** Begge felter i ét kald — invarianten mellem dem kræver begge; begge null rydder. */
    @POST
    @Path("/queues/{uuid}/schedule")
    @Consumes(MediaType.APPLICATION_JSON)
    @Transactional
    public QueueDto updateSchedule(@PathParam("uuid") UUID uuid, UpdateScheduleRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Der mangler tidspunkter");
        }
        return QueueDto.of(queueService.updateSchedule(uuid, request.opensAt(), request.salesStartAt()));
    }

    @POST
    @Path("/queues/{uuid}/move")
    @Consumes(MediaType.APPLICATION_JSON)
    @Transactional
    public QueueDto move(@PathParam("uuid") UUID uuid, MoveQueueRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Der mangler en ny parent-kø");
        }
        return QueueDto.of(queueService.moveQueue(uuid, request.newParentUuid()));
    }

    public record MoveQueueRequest(UUID newParentUuid) {
    }

    @POST
    @Path("/queues/{uuid}/enqueue")
    @Transactional
    public SessionDto enqueue(@PathParam("uuid") UUID uuid) {
        return SessionDto.of(queueService.enqueue(uuid));
    }

    @POST
    @Path("/queues/{uuid}/archive")
    @Transactional
    public QueueDto archive(@PathParam("uuid") UUID uuid) {
        return QueueDto.of(queueService.archiveQueue(uuid));
    }

    @POST
    @Path("/queues/{uuid}/unarchive")
    @Transactional
    public QueueDto unarchive(@PathParam("uuid") UUID uuid) {
        return QueueDto.of(queueService.unarchiveQueue(uuid));
    }

    @DELETE
    @Path("/queues/{uuid}")
    public void deleteQueue(@PathParam("uuid") UUID uuid) {
        queueService.deleteQueue(uuid);
    }

    @GET
    @Path("/stats")
    public StatsService.StatsDto stats(@QueryParam("queue") UUID scopeUuid,
            @QueryParam("hours") Integer hours) {
        return statsService.compute(resolveScope(scopeUuid), hours != null ? hours : 24);
    }

    @GET
    @Path("/stats/live")
    public StatsService.LiveStatsDto liveStats(@QueryParam("queue") UUID scopeUuid) {
        return statsService.computeLive(resolveScope(scopeUuid));
    }

    private static Queue resolveScope(UUID scopeUuid) {
        if (scopeUuid == null) {
            return null;
        }
        Queue scope = Queue.findByUuid(scopeUuid);
        if (scope == null) {
            throw new IllegalArgumentException("Ukendt kø: " + scopeUuid);
        }
        return scope;
    }

    @GET
    @Path("/errors")
    public List<ErrorEntryDto> errors() {
        return ErrorLogEntry.<ErrorLogEntry>findAll(Sort.descending("createdAt", "id"))
                .page(Page.ofSize(200)).list()
                .stream().map(e -> new ErrorEntryDto(e.createdAt, e.level, e.logger, e.message, e.pod))
                .toList();
    }

    public record ErrorEntryDto(Instant timestamp, String level, String logger, String message, String pod) {
    }

    @GET
    @Path("/sessions")
    public List<SessionDto> sessions() {
        return QueueSession.<QueueSession>findAll(Sort.descending("id"))
                .page(Page.ofSize(100)).list()
                .stream().map(SessionDto::of).toList();
    }

    /** Uden årsag registreres ADMIN. */
    @POST
    @Path("/sessions/{uuid}/close")
    @Transactional
    public SessionDto close(@PathParam("uuid") UUID uuid, @QueryParam("reason") CloseReason reason) {
        return SessionDto.of(queueService.closeSession(uuid, reason != null ? reason : CloseReason.ADMIN));
    }

    public record CreateQueueRequest(String name, QueueLevel level, UUID parentUuid, Integer maxCapacity,
            String externalReference) {
    }

    public record UpdateCapacityRequest(Integer maxCapacity) {
    }

    public record UpdateScheduleRequest(Instant opensAt, Instant salesStartAt) {
    }

    /** To queries for hele listen — pr. knude i en løkke kostede 1.000 queries for én udfoldet gren. */
    static List<QueueDto> toDtos(List<Queue> nodes) {
        if (nodes.isEmpty()) {
            return List.of();
        }
        List<Long> ids = nodes.stream().map(node -> node.id).toList();
        Map<Long, Long> childCounts = childCounts(ids);
        Map<Long, long[]> sessionCounts = subtreeSessionCounts(ids);

        List<QueueDto> result = new ArrayList<>(nodes.size());
        for (Queue node : nodes) {
            long[] counts = sessionCounts.get(node.id);
            result.add(new QueueDto(node.uuid, node.name, node.level,
                    node.parent != null ? node.parent.name : null,
                    node.maxCapacity, node.dirty, node.externalReference, node.archivedAt,
                    node.opensAt, node.salesStartAt, node.drawnAt,
                    childCounts.getOrDefault(node.id, 0L),
                    stateCount(counts, QueueSessionState.INITIAL),
                    stateCount(counts, QueueSessionState.QUEUED),
                    stateCount(counts, QueueSessionState.OPEN),
                    stateCount(counts, QueueSessionState.CLOSED)));
        }
        return result;
    }

    private static long stateCount(long[] counts, QueueSessionState state) {
        return counts == null ? 0 : counts[state.ordinal()];
    }

    private static Map<Long, Long> childCounts(List<Long> parentIds) {
        Map<Long, Long> result = new HashMap<>();
        List<ChildCountRow> rows = Queue.getEntityManager().createQuery("""
                select p.id, count(q)
                from Queue q
                join q.parent p
                where p.id in :ids
                group by p.id
                """, ChildCountRow.class)
                .setParameter("ids", parentIds)
                .getResultList();
        for (ChildCountRow row : rows) {
            result.put(row.parentId(), row.count());
        }
        return result;
    }

    /**
     * Aggregeret over undertræet via forfaderstien som id-kolonner. Tre eksplicitte
     * left joins — implicitte HQL-joins er inner joins og taber rødderne.
     */
    private static Map<Long, long[]> subtreeSessionCounts(List<Long> nodeIds) {
        List<SubtreeCountRow> rows = QueueSession.getEntityManager().createQuery("""
                select q.id, p1.id, p2.id, p3.id, s.state, count(s)
                from QueueSession s
                join s.queue q
                left join q.parent p1
                left join p1.parent p2
                left join p2.parent p3
                where q.id in :ids or p1.id in :ids or p2.id in :ids or p3.id in :ids
                group by q.id, p1.id, p2.id, p3.id, s.state
                """, SubtreeCountRow.class)
                .setParameter("ids", nodeIds)
                .getResultList();

        Set<Long> wanted = Set.copyOf(nodeIds);
        Map<Long, long[]> result = new HashMap<>();
        for (SubtreeCountRow row : rows) {
            for (Long nodeId : row.pathIds()) {
                if (nodeId != null && wanted.contains(nodeId)) {
                    result.computeIfAbsent(nodeId, id -> new long[QueueSessionState.values().length])
                            [row.state().ordinal()] += row.count();
                }
            }
        }
        return result;
    }

    private static List<UUID> ancestorUuids(Queue queue) {
        List<UUID> ancestors = new ArrayList<>();
        for (Queue node = queue.parent; node != null; node = node.parent) {
            ancestors.addFirst(node.uuid);
        }
        return ancestors;
    }

    private record ChildCountRow(Long parentId, long count) {
    }

    /** Skal være {@code Long} — stier kortere end fire niveauer giver null. */
    private record SubtreeCountRow(long queueId, Long p1Id, Long p2Id, Long p3Id,
            QueueSessionState state, long count) {

        List<Long> pathIds() {
            return Arrays.asList(queueId, p1Id, p2Id, p3Id);
        }
    }

    /** Tællerne dækker hele undertræet. */
    public record QueueDto(UUID uuid, String name, QueueLevel level, String parentName, Integer maxCapacity,
            boolean dirty, String externalReference, Instant archivedAt,
            Instant opensAt, Instant salesStartAt, Instant drawnAt,
            long childCount, long initial, long queued, long open, long closed) {

        static QueueDto of(Queue queue) {
            return toDtos(List.of(queue)).getFirst();
        }
    }

    public record SearchHitDto(QueueDto queue, List<UUID> ancestors) {

    }

    public record SessionDto(UUID uuid, String queueName, QueueSessionState state, long sequenceNumber,
            Instant createdAt, Instant closedAt, CloseReason closeReason) {

        static SessionDto of(QueueSession session) {
            return new SessionDto(session.uuid, session.queue.name, session.state, session.sequenceNumber,
                    session.createdAt, session.closedAt, session.closeReason);
        }
    }
}
