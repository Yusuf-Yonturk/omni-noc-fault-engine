package com.noc.ingestion.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.noc.ingestion.domain.NormalizedAlarm;
import com.noc.ingestion.domain.RawAlarm;
import com.noc.ingestion.domain.enums.AlarmType;
import com.noc.ingestion.domain.enums.ProcessingStatus;
import com.noc.ingestion.domain.enums.Severity;
import com.noc.ingestion.dto.AlarmIngestRequest;
import com.noc.ingestion.dto.NormalizedAlarmDto;
import com.noc.ingestion.event.AlarmNormalizedEvent;
import com.noc.ingestion.mapper.NormalizedAlarmMapper;
import com.noc.ingestion.repository.NormalizedAlarmRepository;
import com.noc.ingestion.repository.RawAlarmRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** Normalizes raw alarms to canonical format and publishes to Kafka. */
@Service
@Slf4j
public class AlarmNormalizationService {

    // Severity ordering for boost logic
    private static final Map<Severity, Integer> SEVERITY_ORDER = Map.of(
            Severity.INFO,     0,
            Severity.WARNING,  1,
            Severity.MINOR,    2,
            Severity.MAJOR,    3,
            Severity.CRITICAL, 4
    );

    // Device types that trigger severity boost (Correlation Rule 4)
    private static final java.util.Set<String> BOOST_DEVICE_TYPES = java.util.Set.of(
            "CORE_ROUTER", "TRANSMISSION_NODE"
    );

    private final NormalizedAlarmRepository normalizedAlarmRepository;
    private final RawAlarmRepository rawAlarmRepository;
    private final KafkaTemplate<String, AlarmNormalizedEvent> kafkaTemplate;
    private final NormalizedAlarmMapper mapper;
    private final ObjectMapper objectMapper;
    private final Counter alarmsNormalizedCounter;
    private final Counter normalizationFailureCounter;

    @Value("${noc.kafka.topics.alarm-normalized:alarm.normalized}")
    private String alarmNormalizedTopic;

    public AlarmNormalizationService(
            NormalizedAlarmRepository normalizedAlarmRepository,
            RawAlarmRepository rawAlarmRepository,
            KafkaTemplate<String, AlarmNormalizedEvent> kafkaTemplate,
            NormalizedAlarmMapper mapper,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {

        this.normalizedAlarmRepository = normalizedAlarmRepository;
        this.rawAlarmRepository = rawAlarmRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
        this.objectMapper = objectMapper;

        // Prometheus counters
        this.alarmsNormalizedCounter = Counter.builder("alarms_ingested_total")
                .description("Total number of alarms successfully normalized")
                .tag("service", "alarm-ingestion-service")
                .register(meterRegistry);

        this.normalizationFailureCounter = Counter.builder("alarms_normalization_failure_total")
                .description("Total number of alarm normalization failures")
                .tag("service", "alarm-ingestion-service")
                .register(meterRegistry);
    }

    /** Normalizes a raw alarm, persists it, and publishes an AlarmNormalizedEvent to Kafka. */
    @Transactional
    public NormalizedAlarm normalize(RawAlarm rawAlarm, AlarmIngestRequest request) {
        try {
            AlarmType  alarmType = resolveAlarmType(request.alarmType());
            Severity   severity  = resolveSeverity(request.severity(), request.deviceType());

            // Fallback: if occurredAt missing, use receivedAt
            Instant occurredAt = (request.occurredAt() != null)
                    ? request.occurredAt().toInstant()
                    : rawAlarm.getReceivedAt();

            NormalizedAlarm normalized = NormalizedAlarm.builder()
                    .rawAlarmId(rawAlarm.getId())
                    .alarmType(alarmType)
                    .severity(severity)
                    .siteId(request.siteId())
                    .region(request.region())
                    .deviceType(normalizeDeviceType(request.deviceType()))
                    .deviceId(request.deviceId())
                    .serviceAffected(request.serviceAffected())
                    .description(request.description())
                    .occurredAt(occurredAt)
                    .build();

            NormalizedAlarm saved = normalizedAlarmRepository.save(normalized);

            // Mark the raw alarm as PROCESSED
            rawAlarm.setProcessingStatus(ProcessingStatus.PROCESSED);
            rawAlarmRepository.save(rawAlarm);

            // Publish Kafka event (fire-and-forget with async callback)
            publishAlarmNormalizedEvent(saved);

            alarmsNormalizedCounter.increment();
            log.debug("Alarm normalized: id={}, type={}, severity={}, siteId={}",
                    saved.getId(), alarmType, severity, request.siteId());

            return saved;

        } catch (Exception ex) {
            normalizationFailureCounter.increment();
            rawAlarm.setProcessingStatus(ProcessingStatus.FAILED);
            rawAlarmRepository.save(rawAlarm);
            log.error("Normalization failed for rawAlarmId={}: {}", rawAlarm.getId(), ex.getMessage(), ex);
            throw new RuntimeException("Alarm normalization failed", ex);
        }
    }

    /** Returns paginated normalized alarms, optionally filtered by site, region, and time range. */
    @Transactional(readOnly = true)
    public Page<NormalizedAlarmDto> queryNormalizedAlarms(
            String siteId, String region,
            OffsetDateTime from, OffsetDateTime to,
            Pageable pageable) {

        Instant fromInstant = (from != null) ? from.toInstant() : Instant.EPOCH;
        Instant toInstant   = (to   != null) ? to.toInstant()   : Instant.now().plusSeconds(60);

        return normalizedAlarmRepository
                .findByFilters(siteId, region, fromInstant, toInstant, pageable)
                .map(mapper::toDto);
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /** Maps raw alarmType string to canonical AlarmType enum; defaults to SERVICE_DEGRADED. */
    private AlarmType resolveAlarmType(String rawType) {
        if (rawType == null || rawType.isBlank()) {
            log.warn("Missing alarmType in request, defaulting to SERVICE_DEGRADED");
            return AlarmType.SERVICE_DEGRADED;
        }
        String normalized = rawType.trim().toUpperCase().replace("-", "_").replace(" ", "_");
        try {
            return AlarmType.valueOf(normalized);
        } catch (IllegalArgumentException ex) {
            log.warn("Unknown alarmType '{}', defaulting to SERVICE_DEGRADED", rawType);
            return AlarmType.SERVICE_DEGRADED;
        }
    }

    /** Maps raw severity string to canonical Severity enum; applies boost for critical device types. */
    private Severity resolveSeverity(String rawSeverity, String deviceType) {
        Severity base;
        if (rawSeverity == null || rawSeverity.isBlank()) {
            base = Severity.MINOR;
        } else {
            try {
                base = Severity.valueOf(rawSeverity.trim().toUpperCase());
            } catch (IllegalArgumentException ex) {
                log.warn("Unknown severity '{}', defaulting to MINOR", rawSeverity);
                base = Severity.MINOR;
            }
        }

        // Severity boost for critical device types
        if (deviceType != null && BOOST_DEVICE_TYPES.contains(deviceType.toUpperCase())) {
            Severity boosted = boostSeverity(base);
            if (boosted != base) {
                log.debug("Severity boosted from {} to {} for deviceType={}", base, boosted, deviceType);
            }
            return boosted;
        }

        return base;
    }

    /** Boosts severity by one level; CRITICAL cannot be boosted further. */
    private Severity boostSeverity(Severity current) {
        return switch (current) {
            case INFO    -> Severity.WARNING;
            case WARNING -> Severity.MINOR;
            case MINOR   -> Severity.MAJOR;
            case MAJOR   -> Severity.CRITICAL;
            case CRITICAL -> Severity.CRITICAL; // already maximum
        };
    }

    /** Normalizes device type to uppercase for consistent storage and matching. */
    private String normalizeDeviceType(String deviceType) {
        return (deviceType != null) ? deviceType.trim().toUpperCase() : null;
    }

    /** Publishes an AlarmNormalizedEvent to Kafka, keyed by siteId for partition affinity. */
    private void publishAlarmNormalizedEvent(NormalizedAlarm alarm) {
        AlarmNormalizedEvent event = AlarmNormalizedEvent.from(alarm);
        String key = alarm.getSiteId() != null ? alarm.getSiteId() : alarm.getId().toString();

        CompletableFuture<SendResult<String, AlarmNormalizedEvent>> future =
                kafkaTemplate.send(alarmNormalizedTopic, key, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish AlarmNormalizedEvent for alarmId={}: {}",
                        alarm.getId(), ex.getMessage(), ex);
            } else {
                log.debug("AlarmNormalizedEvent published: alarmId={}, partition={}, offset={}",
                        alarm.getId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
