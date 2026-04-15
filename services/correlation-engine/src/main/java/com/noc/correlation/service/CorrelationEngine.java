package com.noc.correlation.service;

import com.noc.correlation.event.AlarmNormalizedEvent;
import com.noc.correlation.event.IncidentUpdatedEvent;
import com.noc.correlation.domain.Incident;
import com.noc.correlation.domain.IncidentAlarmMap;
import com.noc.correlation.repository.IncidentAlarmMapRepository;
import com.noc.correlation.repository.IncidentRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/** Groups normalized alarms into incidents using Redis sliding windows and correlation rules. */
@Service
@Slf4j
public class CorrelationEngine {

    // Alarm types that indicate infrastructure failure (trigger parent classification)
    private static final Set<String> PARENT_ALARM_TYPES = Set.of(
            "FIBER_CUT", "LINK_DOWN", "SITE_DOWN", "POWER_LOSS", "TRANSMISSION_FAILURE"
    );

    // Alarm types that are downstream effects (child alarms)
    private static final Set<String> CHILD_ALARM_TYPES = Set.of(
            "SERVICE_DEGRADED", "PACKET_LOSS_HIGH",
            "CUSTOMER_COMPLAINT_SPIKE", "SERVICE_LATENCY"
    );

    private final RedisCorrelationCache correlationCache;
    private final IncidentRepository incidentRepository;
    private final IncidentAlarmMapRepository alarmMapRepository;
    private final KafkaTemplate<String, IncidentUpdatedEvent> kafkaTemplate;

    // Prometheus metrics
    private final Counter alarmsCorrelatedTotal;
    private final Counter incidentsCreatedTotal;
    private final Timer   correlationDuration;

    // In-memory stats for /correlation/stats endpoint
    private final AtomicInteger totalAlarmsProcessed = new AtomicInteger(0);
    private final AtomicInteger totalIncidentsCreated = new AtomicInteger(0);
    private final AtomicInteger cacheHits  = new AtomicInteger(0);
    private final AtomicInteger cacheMisses = new AtomicInteger(0);

    @Value("${noc.kafka.topics.incident-updated:incident.updated}")
    private String incidentUpdatedTopic;

    public CorrelationEngine(
            RedisCorrelationCache correlationCache,
            IncidentRepository incidentRepository,
            IncidentAlarmMapRepository alarmMapRepository,
            KafkaTemplate<String, IncidentUpdatedEvent> kafkaTemplate,
            MeterRegistry meterRegistry) {

        this.correlationCache    = correlationCache;
        this.incidentRepository  = incidentRepository;
        this.alarmMapRepository  = alarmMapRepository;
        this.kafkaTemplate       = kafkaTemplate;

        this.alarmsCorrelatedTotal = Counter.builder("alarms_correlated_total")
                .description("Total alarms successfully correlated to an incident")
                .register(meterRegistry);

        this.incidentsCreatedTotal = Counter.builder("incidents_created_total")
                .description("Total incidents created by correlation engine")
                .register(meterRegistry);

        this.correlationDuration = Timer.builder("correlation_processing_duration_seconds")
                .description("Time taken to correlate one alarm")
                .register(meterRegistry);
    }

    /** Correlate a single alarm event; delegates to doCorrelate inside a timer. */
    @Transactional
    public void correlate(AlarmNormalizedEvent event) {
        correlationDuration.record(() -> doCorrelate(event));
    }

    private void doCorrelate(AlarmNormalizedEvent event) {
        totalAlarmsProcessed.incrementAndGet();

        log.info("Correlating alarm: id={}, type={}, severity={}, site={}",
                event.alarmId(), event.alarmType(), event.severity(), event.siteId());

        String siteId = event.siteId();

        // Rule 1: check Redis sliding window
        Optional<UUID> cachedIncidentId = correlationCache.getActiveIncidentId(siteId);

        if (cachedIncidentId.isPresent()) {
            // Cache HIT: existing incident window — map alarm
            cacheHits.incrementAndGet();
            UUID incidentId = cachedIncidentId.get();

            Incident incident = incidentRepository.findById(incidentId)
                    .orElseGet(() -> {
                        // Edge case: Redis has stale key but DB record deleted
                        log.warn("Redis key exists but incident not found in DB: {}. Creating new.", incidentId);
                        correlationCache.evict(siteId);
                        return null;
                    });

            if (incident != null) {
                mapAlarmToExistingIncident(event, incident);
                return;
            }
        }

        // Cache MISS: no active window — create new incident
        cacheMisses.incrementAndGet();
        createNewIncident(event);
    }

    /** Maps an alarm to an existing incident and refreshes the Redis TTL. */
    private void mapAlarmToExistingIncident(AlarmNormalizedEvent event, Incident incident) {
        // Rule 3: determine relation type (CHILD vs CORRELATED)
        String relationType = resolveRelationType(event.alarmType(), incident);

        IncidentAlarmMap mapping = IncidentAlarmMap.builder()
                .incidentId(incident.getId())
                .alarmId(event.alarmId())
                .relationType(relationType)
                .build();
        alarmMapRepository.save(mapping);

        incident.setLastSeenAt(Instant.now());
        incident.setServiceImpactCount(incident.getServiceImpactCount() + 1);
        incidentRepository.save(incident);

        // Refresh Redis TTL (sliding window stays alive)
        correlationCache.refreshTtl(event.siteId());

        int alarmCount = alarmMapRepository.countByIncidentId(incident.getId());

        log.info("Alarm {} ({}) → correlated to existing incident {} [{}] (total alarms: {})",
                event.alarmId(), event.alarmType(), incident.getId(), relationType, alarmCount);

        IncidentUpdatedEvent updatedEvent = IncidentUpdatedEvent.alarmCorrelated(
                incident.getId(), event.alarmId(),
                incident.getSeverity(), incident.getRegion(), incident.getSiteId(),
                incident.getProbableRootCause(), incident.getAssignedTeam(), alarmCount
        );
        publishIncidentUpdated(updatedEvent);
        alarmsCorrelatedTotal.increment();
    }

    /** Creates a new incident from the first alarm in a site window. */
    private void createNewIncident(AlarmNormalizedEvent event) {
        String rootCause    = deriveRootCause(event.alarmType());
        String assignedTeam = deriveAssignedTeam(event.alarmType());
        String title        = buildIncidentTitle(event);
        Instant now         = Instant.now();

        Incident incident = Incident.builder()
                .title(title)
                .probableRootCause(rootCause)
                .status("OPEN")
                .severity(event.severity())
                .region(event.region())
                .siteId(event.siteId())
                .serviceImpactCount(1)
                .firstSeenAt(event.occurredAt() != null ? event.occurredAt() : now)
                .lastSeenAt(now)
                .assignedTeam(assignedTeam)
                .improvementTicketCreated(false)
                .build();

        Incident saved = incidentRepository.save(incident);

        // First alarm is always ROOT_CAUSE
        IncidentAlarmMap rootMapping = IncidentAlarmMap.builder()
                .incidentId(saved.getId())
                .alarmId(event.alarmId())
                .relationType("ROOT_CAUSE")
                .build();
        alarmMapRepository.save(rootMapping);

        // Write to Redis with configured TTL
        correlationCache.putIncidentId(event.siteId(), saved.getId());

        log.info("NEW INCIDENT created: id={}, title='{}', rootCause='{}', team='{}', site={}",
                saved.getId(), title, rootCause, assignedTeam, event.siteId());

        IncidentUpdatedEvent createdEvent = IncidentUpdatedEvent.incidentCreated(
                saved.getId(), event.alarmId(),
                event.severity(), event.region(), event.siteId(),
                rootCause, assignedTeam
        );
        publishIncidentUpdated(createdEvent);

        incidentsCreatedTotal.increment();
        totalIncidentsCreated.incrementAndGet();
        alarmsCorrelatedTotal.increment();
    }

    /** Rule 3: returns CHILD for downstream alarm types, CORRELATED otherwise. */
    private String resolveRelationType(String alarmType, Incident incident) {
        if (CHILD_ALARM_TYPES.contains(alarmType)) {
            return "CHILD";
        }
        if (PARENT_ALARM_TYPES.contains(alarmType)) {
            return "CORRELATED";
        }
        return "CORRELATED";
    }

    /** Maps alarm type to a human-readable root cause string. */
    private String deriveRootCause(String alarmType) {
        return switch (alarmType) {
            case "FIBER_CUT"               -> "Fiber/Transmission Failure";
            case "TRANSMISSION_FAILURE"    -> "Fiber/Transmission Failure";
            case "SITE_DOWN"               -> "Site/Power Outage";
            case "POWER_LOSS"              -> "Power Outage";
            case "CPU_HIGH"                -> "Overloaded Network Function";
            case "LINK_DOWN"               -> "Link/Transmission Failure";
            case "PACKET_LOSS_HIGH"        -> "Overloaded Network Function";
            case "CUSTOMER_COMPLAINT_SPIKE"-> "Service Degradation (Root TBD)";
            case "SERVICE_DEGRADED"        -> "Service Degradation";
            case "SERVICE_LATENCY"         -> "Service Latency Issue";
            default                        -> "Under Investigation";
        };
    }

    /** Maps alarm type to the responsible NOC team. */
    private String deriveAssignedTeam(String alarmType) {
        return switch (alarmType) {
            case "FIBER_CUT",
                 "TRANSMISSION_FAILURE",
                 "LINK_DOWN"            -> "Transmission L2 Team";
            case "POWER_LOSS",
                 "SITE_DOWN"            -> "Field Operations Team";
            case "CPU_HIGH",
                 "PACKET_LOSS_HIGH"     -> "Core Network L2 Team";
            case "SERVICE_DEGRADED",
                 "SERVICE_LATENCY",
                 "CUSTOMER_COMPLAINT_SPIKE" -> "Service Assurance Team";
            default                     -> "NOC L1 Team";
        };
    }

    /** Builds a "[SEVERITY] AlarmType on site - region" incident title. */
    private String buildIncidentTitle(AlarmNormalizedEvent event) {
        String type   = event.alarmType() != null ? event.alarmType().replace("_", " ") : "UNKNOWN";
        String site   = event.siteId()    != null ? event.siteId()   : "UNKNOWN-SITE";
        String region = event.region()    != null ? event.region()   : "UNKNOWN-REGION";
        String sev    = event.severity()  != null ? event.severity() : "UNKNOWN";
        return String.format("[%s] %s on %s - %s", sev, type, site, region);
    }

    private void publishIncidentUpdated(IncidentUpdatedEvent event) {
        String key = event.incidentId().toString();
        kafkaTemplate.send(incidentUpdatedTopic, key, event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish IncidentUpdatedEvent for incident={}: {}",
                                event.incidentId(), ex.getMessage());
                    } else {
                        log.debug("IncidentUpdatedEvent published: incidentId={}, action={}",
                                event.incidentId(), event.action());
                    }
                });
    }

    /** Returns runtime stats used by the /correlation/stats endpoint. */
    public CorrelationStats getStats() {
        long totalOpen     = incidentRepository.countOpenIncidents();
        long criticalOpen  = incidentRepository.countOpenCriticalIncidents();
        double hitRate = (cacheHits.get() + cacheMisses.get()) == 0 ? 0.0
                : (double) cacheHits.get() / (cacheHits.get() + cacheMisses.get()) * 100.0;

        return new CorrelationStats(
                totalAlarmsProcessed.get(),
                totalIncidentsCreated.get(),
                cacheHits.get(),
                cacheMisses.get(),
                Math.round(hitRate * 10.0) / 10.0,
                (int) totalOpen,
                (int) criticalOpen
        );
    }

    public record CorrelationStats(
            int totalAlarmsProcessed,
            int totalIncidentsCreated,
            int cacheHits,
            int cacheMisses,
            double cacheHitRatePct,
            int openIncidents,
            int criticalOpenIncidents
    ) {}
}
