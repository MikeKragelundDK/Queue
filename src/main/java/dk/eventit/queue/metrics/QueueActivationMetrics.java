package dk.eventit.queue.metrics;

import java.util.concurrent.atomic.AtomicLong;

import dk.eventit.queue.service.QueueActivationService.ActivationResult;
import dk.eventit.queue.service.QueueActivationService.BacklogSnapshot;
import dk.eventit.queue.service.QueueActivationService.WorkClaim;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class QueueActivationMetrics {

    private static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("outcome");

    private final LongCounter cycles;
    private final LongCounter scanned;
    private final LongCounter opened;
    private final LongCounter queued;
    private final LongCounter batchLimitHits;
    private final DoubleHistogram duration;
    private final AtomicLong waitingSessions = new AtomicLong();
    private final AtomicLong oldestWaitSeconds = new AtomicLong();
    private final ObservableLongGauge waitingGauge;
    private final ObservableLongGauge oldestWaitGauge;

    /** Injiceret frem for {@code GlobalOpenTelemetry}, der kan fange noop-instansen lydløst. */
    @Inject
    public QueueActivationMetrics(OpenTelemetry openTelemetry) {
        Meter meter = openTelemetry.getMeter("dk.eventit.queue");

        cycles = meter.counterBuilder("queue.activation.cycles").build();
        scanned = meter.counterBuilder("queue.activation.sessions.scanned").build();
        opened = meter.counterBuilder("queue.activation.sessions.opened").build();
        queued = meter.counterBuilder("queue.activation.sessions.queued").build();
        batchLimitHits = meter.counterBuilder("queue.activation.batch.limit_hits").build();
        duration = meter.histogramBuilder("queue.activation.duration").setUnit("s").build();
        waitingGauge = meter.gaugeBuilder("queue.waiting.sessions")
                .ofLongs()
                .buildWithCallback(measurement -> measurement.record(waitingSessions.get()));
        oldestWaitGauge = meter.gaugeBuilder("queue.waiting.oldest.age")
                .setUnit("s")
                .ofLongs()
                .buildWithCallback(measurement -> measurement.record(oldestWaitSeconds.get()));
    }

    public void record(ActivationResult result, long elapsedNanos) {
        String outcome = result.lockAcquired() ? "success" : "lock_busy";
        recordCycle(outcome, elapsedNanos);
        if (!result.lockAcquired()) {
            return;
        }

        add(scanned, result.scanned());
        add(opened, result.opened());
        add(queued, result.queued());
        if (result.batchFull()) {
            batchLimitHits.add(1);
        }
    }

    /** CLAIMED tælles af {@link #record(ActivationResult, long)} — ellers to gange. */
    public void record(WorkClaim claim, long elapsedNanos) {
        if (claim == WorkClaim.CLAIMED) {
            return;
        }
        recordCycle(claim == WorkClaim.IDLE ? "idle" : "lock_busy", elapsedNanos);
    }

    public void recordFailure(long elapsedNanos) {
        recordCycle("error", elapsedNanos);
    }

    public void recordBacklog(BacklogSnapshot snapshot) {
        waitingSessions.set(snapshot.waitingSessions());
        oldestWaitSeconds.set(snapshot.oldestWaitSeconds());
    }

    private void recordCycle(String outcome, long elapsedNanos) {
        Attributes attributes = Attributes.of(OUTCOME, outcome);
        cycles.add(1, attributes);
        duration.record(elapsedNanos / 1_000_000_000.0, attributes);
    }

    private static void add(LongCounter counter, long value) {
        if (value > 0) {
            counter.add(value);
        }
    }
}
