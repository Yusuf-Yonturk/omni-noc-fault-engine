package com.noc.correlation.service;

import com.noc.correlation.domain.Incident;
import com.noc.correlation.domain.IncidentAlarmMap;
import com.noc.correlation.event.AlarmNormalizedEvent;
import com.noc.correlation.event.IncidentUpdatedEvent;
import com.noc.correlation.repository.IncidentAlarmMapRepository;
import com.noc.correlation.repository.IncidentRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Unit tests for all 4 correlation rules — no Spring context, no Kafka, no Redis. */
@ExtendWith(MockitoExtension.class)
@DisplayName("CorrelationEngine Unit Tests")
class CorrelationEngineTest {

    @Mock private RedisCorrelationCache correlationCache;
    @Mock private IncidentRepository incidentRepository;
    @Mock private IncidentAlarmMapRepository alarmMapRepository;
    @Mock private KafkaTemplate<String, IncidentUpdatedEvent> kafkaTemplate;

    private CorrelationEngine engine;

    @BeforeEach
    void setUp() {
        engine = new CorrelationEngine(
                correlationCache,
                incidentRepository,
                alarmMapRepository,
                kafkaTemplate,
                new SimpleMeterRegistry()
        );

        // Default: Kafka publish always succeeds
        @SuppressWarnings("unchecked")
        CompletableFuture<SendResult<String, IncidentUpdatedEvent>> future =
                CompletableFuture.completedFuture(mock(SendResult.class));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(future);
    }

    @Nested
    @DisplayName("Rule 1 — Time + Site Window")
    class Rule1Tests {

        @Test
        @DisplayName("Cache MISS → should create a new incident")
        void cacheMiss_createsNewIncident() {
            // Given: No active window for this site
            when(correlationCache.getActiveIncidentId("IST-TRX-001"))
                    .thenReturn(Optional.empty());

            Incident savedIncident = buildIncident("CRITICAL", "Istanbul", "IST-TRX-001");
            when(incidentRepository.save(any(Incident.class))).thenReturn(savedIncident);
            when(alarmMapRepository.save(any(IncidentAlarmMap.class))).thenReturn(new IncidentAlarmMap());

            AlarmNormalizedEvent event = fiberCutEvent("IST-TRX-001", "Istanbul");

            // When
            engine.correlate(event);

            // Then: A new incident should be created
            verify(incidentRepository, times(1)).save(any(Incident.class));
            // Redis should be populated with new incidentId
            verify(correlationCache, times(1)).putIncidentId(eq("IST-TRX-001"), any(UUID.class));
        }

        @Test
        @DisplayName("Cache HIT → should map alarm to existing incident, NOT create new")
        void cacheHit_mapsToExistingIncident() {
            // Given: Active window exists
            UUID existingIncidentId = UUID.randomUUID();
            when(correlationCache.getActiveIncidentId("IST-TRX-001"))
                    .thenReturn(Optional.of(existingIncidentId));

            Incident existingIncident = buildIncident("CRITICAL", "Istanbul", "IST-TRX-001");
            existingIncident = spy(existingIncident);
            when(incidentRepository.findById(existingIncidentId))
                    .thenReturn(Optional.of(existingIncident));
            when(incidentRepository.save(any())).thenReturn(existingIncident);
            when(alarmMapRepository.save(any())).thenReturn(new IncidentAlarmMap());
            when(alarmMapRepository.countByIncidentId(any())).thenReturn(2);

            AlarmNormalizedEvent secondAlarm = linkDownEvent("IST-TRX-001", "Istanbul");

            // When
            engine.correlate(secondAlarm);

            // Then: No NEW incident should be saved
            verify(incidentRepository, never()).save(argThat(i ->
                    i.getId() == null)); // new entity has null id before persist
            // Redis TTL should be refreshed
            verify(correlationCache, times(1)).refreshTtl("IST-TRX-001");
            // Alarm should be mapped
            verify(alarmMapRepository, times(1)).save(any(IncidentAlarmMap.class));
        }

        @Test
        @DisplayName("8 alarms from same site → exactly 1 incident created")
        void eightAlarms_samesite_oneIncident() {
            UUID incidentId = UUID.randomUUID();
            Incident incident = buildIncident("CRITICAL", "Istanbul", "IST-TRX-001");

            // First alarm → cache miss → create incident
            when(correlationCache.getActiveIncidentId("IST-TRX-001"))
                    .thenReturn(Optional.empty())         // 1st call: miss
                    .thenReturn(Optional.of(incidentId))  // 2nd-8th: hit
                    .thenReturn(Optional.of(incidentId))
                    .thenReturn(Optional.of(incidentId))
                    .thenReturn(Optional.of(incidentId))
                    .thenReturn(Optional.of(incidentId))
                    .thenReturn(Optional.of(incidentId))
                    .thenReturn(Optional.of(incidentId));

            when(incidentRepository.save(any())).thenReturn(incident);
            when(incidentRepository.findById(incidentId)).thenReturn(Optional.of(incident));
            when(alarmMapRepository.save(any())).thenReturn(new IncidentAlarmMap());
            when(alarmMapRepository.countByIncidentId(any())).thenReturn(2, 3, 4, 5, 6, 7, 8);

            // Send 8 alarms (Fiber Cut scenario)
            engine.correlate(fiberCutEvent("IST-TRX-001", "Istanbul"));
            engine.correlate(linkDownEvent("IST-TRX-001", "Istanbul"));
            engine.correlate(packetLossEvent("IST-TRX-001", "Istanbul"));
            engine.correlate(serviceDegradedEvent("IST-SRV-002", "Istanbul", "IST-TRX-001"));
            engine.correlate(serviceDegradedEvent("IST-SRV-003", "Istanbul", "IST-TRX-001"));
            engine.correlate(complaintSpikeEvent("IST-SRV-002", "Istanbul", "IST-TRX-001"));
            engine.correlate(latencyEvent("IST-SRV-004", "Istanbul", "IST-TRX-001"));
            engine.correlate(transmissionFailureEvent("IST-TRX-001", "Istanbul"));

            // Then: exactly 1 new incident created (first alarm only)
            verify(incidentRepository, times(1)).save(argThat(i -> i.getId() == null));
        }
    }

    @Nested
    @DisplayName("Rule 2 — Root Cause Decision Table")
    class RootCauseTests {

        @Test
        @DisplayName("FIBER_CUT → root cause = 'Fiber/Transmission Failure'")
        void fiberCut_rootCause() {
            when(correlationCache.getActiveIncidentId(any())).thenReturn(Optional.empty());
            ArgumentCaptor<Incident> captor = ArgumentCaptor.forClass(Incident.class);
            Incident saved = buildIncident("CRITICAL", "Istanbul", "IST-TRX-001");
            when(incidentRepository.save(captor.capture())).thenReturn(saved);
            when(alarmMapRepository.save(any())).thenReturn(new IncidentAlarmMap());

            engine.correlate(fiberCutEvent("IST-TRX-001", "Istanbul"));

            Incident created = captor.getValue();
            assertThat(created.getProbableRootCause()).isEqualTo("Fiber/Transmission Failure");
            assertThat(created.getAssignedTeam()).isEqualTo("Transmission L2 Team");
        }

        @Test
        @DisplayName("POWER_LOSS → root cause = 'Power Outage', team = 'Field Operations Team'")
        void powerLoss_rootCause() {
            when(correlationCache.getActiveIncidentId(any())).thenReturn(Optional.empty());
            ArgumentCaptor<Incident> captor = ArgumentCaptor.forClass(Incident.class);
            Incident saved = buildIncident("CRITICAL", "Ankara", "ANK-SITE-007");
            when(incidentRepository.save(captor.capture())).thenReturn(saved);
            when(alarmMapRepository.save(any())).thenReturn(new IncidentAlarmMap());

            AlarmNormalizedEvent event = new AlarmNormalizedEvent(
                    UUID.randomUUID(), UUID.randomUUID(),
                    "POWER_LOSS", "CRITICAL",
                    "ANK-SITE-007", "Ankara", "POWER_UNIT", "PWR-007",
                    null, "Power loss detected", Instant.now(), Instant.now(), "OSS"
            );
            engine.correlate(event);

            assertThat(captor.getValue().getProbableRootCause()).isEqualTo("Power Outage");
            assertThat(captor.getValue().getAssignedTeam()).isEqualTo("Field Operations Team");
        }

        @Test
        @DisplayName("CPU_HIGH → root cause = 'Overloaded Network Function', team = 'Core Network L2 Team'")
        void cpuHigh_rootCause() {
            when(correlationCache.getActiveIncidentId(any())).thenReturn(Optional.empty());
            ArgumentCaptor<Incident> captor = ArgumentCaptor.forClass(Incident.class);
            Incident saved = buildIncident("CRITICAL", "Istanbul", "IST-CORE-003");
            when(incidentRepository.save(captor.capture())).thenReturn(saved);
            when(alarmMapRepository.save(any())).thenReturn(new IncidentAlarmMap());

            AlarmNormalizedEvent event = new AlarmNormalizedEvent(
                    UUID.randomUUID(), UUID.randomUUID(),
                    "CPU_HIGH", "CRITICAL",
                    "IST-CORE-003", "Istanbul", "CORE_ROUTER", "IST-CORE-003",
                    null, "CPU threshold exceeded", Instant.now(), Instant.now(), "NMS"
            );
            engine.correlate(event);

            assertThat(captor.getValue().getProbableRootCause()).isEqualTo("Overloaded Network Function");
            assertThat(captor.getValue().getAssignedTeam()).isEqualTo("Core Network L2 Team");
        }

        @Test
        @DisplayName("Unknown alarm type → root cause = 'Under Investigation', team = 'NOC L1 Team'")
        void unknownType_defaultRootCause() {
            when(correlationCache.getActiveIncidentId(any())).thenReturn(Optional.empty());
            ArgumentCaptor<Incident> captor = ArgumentCaptor.forClass(Incident.class);
            Incident saved = buildIncident("MINOR", "Izmir", "IZM-SITE-001");
            when(incidentRepository.save(captor.capture())).thenReturn(saved);
            when(alarmMapRepository.save(any())).thenReturn(new IncidentAlarmMap());

            AlarmNormalizedEvent event = new AlarmNormalizedEvent(
                    UUID.randomUUID(), UUID.randomUUID(),
                    "SERVICE_LATENCY", "MINOR",
                    "IZM-SITE-001", "Izmir", "SWITCH", "SW-001",
                    null, "Latency increase", Instant.now(), Instant.now(), "OSS"
            );
            engine.correlate(event);

            assertThat(captor.getValue().getProbableRootCause()).isEqualTo("Service Latency Issue");
            assertThat(captor.getValue().getAssignedTeam()).isEqualTo("Service Assurance Team");
        }
    }

    @Nested
    @DisplayName("Rule 3 — Parent-Child Detection")
    class ParentChildTests {

        @Test
        @DisplayName("SERVICE_DEGRADED after FIBER_CUT → relation_type = CHILD")
        void serviceDegraded_afterFiberCut_isChild() {
            UUID incidentId = UUID.randomUUID();
            when(correlationCache.getActiveIncidentId("IST-TRX-001"))
                    .thenReturn(Optional.of(incidentId));

            Incident incident = buildIncident("CRITICAL", "Istanbul", "IST-TRX-001");
            when(incidentRepository.findById(incidentId)).thenReturn(Optional.of(incident));
            when(incidentRepository.save(any())).thenReturn(incident);
            when(alarmMapRepository.countByIncidentId(any())).thenReturn(2);

            ArgumentCaptor<IncidentAlarmMap> mapCaptor =
                    ArgumentCaptor.forClass(IncidentAlarmMap.class);
            when(alarmMapRepository.save(mapCaptor.capture())).thenReturn(new IncidentAlarmMap());

            // SERVICE_DEGRADED is a child alarm type
            engine.correlate(serviceDegradedEvent("IST-SRV-002", "Istanbul", "IST-TRX-001"));

            IncidentAlarmMap saved = mapCaptor.getValue();
            assertThat(saved.getRelationType()).isEqualTo("CHILD");
        }

        @Test
        @DisplayName("FIBER_CUT is first alarm → relation_type = ROOT_CAUSE")
        void fiberCut_firstAlarm_isRootCause() {
            when(correlationCache.getActiveIncidentId("IST-TRX-001"))
                    .thenReturn(Optional.empty());

            Incident saved = buildIncident("CRITICAL", "Istanbul", "IST-TRX-001");
            when(incidentRepository.save(any())).thenReturn(saved);

            ArgumentCaptor<IncidentAlarmMap> mapCaptor =
                    ArgumentCaptor.forClass(IncidentAlarmMap.class);
            when(alarmMapRepository.save(mapCaptor.capture())).thenReturn(new IncidentAlarmMap());

            engine.correlate(fiberCutEvent("IST-TRX-001", "Istanbul"));

            IncidentAlarmMap rootMap = mapCaptor.getValue();
            assertThat(rootMap.getRelationType()).isEqualTo("ROOT_CAUSE");
        }
    }

    @Nested
    @DisplayName("Stats")
    class StatsTests {

        @Test
        @DisplayName("Stats reflect correct counts after processing")
        void stats_areCorrectAfterProcessing() {
            when(correlationCache.getActiveIncidentId(any())).thenReturn(Optional.empty());
            when(incidentRepository.save(any())).thenReturn(buildIncident("MAJOR", "Ankara", "ANK-001"));
            when(alarmMapRepository.save(any())).thenReturn(new IncidentAlarmMap());
            when(incidentRepository.countOpenIncidents()).thenReturn(1L);
            when(incidentRepository.countOpenCriticalIncidents()).thenReturn(0L);

            engine.correlate(fiberCutEvent("ANK-001", "Ankara"));

            CorrelationEngine.CorrelationStats stats = engine.getStats();
            assertThat(stats.totalAlarmsProcessed()).isEqualTo(1);
            assertThat(stats.totalIncidentsCreated()).isEqualTo(1);
            assertThat(stats.cacheMisses()).isEqualTo(1);
            assertThat(stats.cacheHits()).isEqualTo(0);
        }
    }

    // Test fixtures

    private AlarmNormalizedEvent fiberCutEvent(String siteId, String region) {
        return new AlarmNormalizedEvent(UUID.randomUUID(), UUID.randomUUID(),
                "FIBER_CUT", "CRITICAL", siteId, region,
                "TRANSMISSION_NODE", siteId, null,
                "Fiber cut detected", Instant.now(), Instant.now(), "OSS");
    }

    private AlarmNormalizedEvent linkDownEvent(String siteId, String region) {
        return new AlarmNormalizedEvent(UUID.randomUUID(), UUID.randomUUID(),
                "LINK_DOWN", "MAJOR", siteId, region,
                "TRANSMISSION_NODE", siteId, null,
                "Link down", Instant.now(), Instant.now(), "OSS");
    }

    private AlarmNormalizedEvent packetLossEvent(String siteId, String region) {
        return new AlarmNormalizedEvent(UUID.randomUUID(), UUID.randomUUID(),
                "PACKET_LOSS_HIGH", "MAJOR", siteId, region,
                "SWITCH", siteId, null,
                "High packet loss", Instant.now(), Instant.now(), "NMS");
    }

    private AlarmNormalizedEvent serviceDegradedEvent(String deviceId, String region, String siteId) {
        return new AlarmNormalizedEvent(UUID.randomUUID(), UUID.randomUUID(),
                "SERVICE_DEGRADED", "MAJOR", siteId, region,
                "SERVICE_NODE", deviceId, "4G_DATA",
                "Service degraded", Instant.now(), Instant.now(), "OSS");
    }

    private AlarmNormalizedEvent complaintSpikeEvent(String deviceId, String region, String siteId) {
        return new AlarmNormalizedEvent(UUID.randomUUID(), UUID.randomUUID(),
                "CUSTOMER_COMPLAINT_SPIKE", "MINOR", siteId, region,
                "SERVICE_NODE", deviceId, "4G_DATA",
                "Complaint spike", Instant.now(), Instant.now(), "CRM");
    }

    private AlarmNormalizedEvent latencyEvent(String deviceId, String region, String siteId) {
        return new AlarmNormalizedEvent(UUID.randomUUID(), UUID.randomUUID(),
                "SERVICE_LATENCY", "WARNING", siteId, region,
                "SERVICE_NODE", deviceId, "INTERNET",
                "Latency increase", Instant.now(), Instant.now(), "OSS");
    }

    private AlarmNormalizedEvent transmissionFailureEvent(String siteId, String region) {
        return new AlarmNormalizedEvent(UUID.randomUUID(), UUID.randomUUID(),
                "TRANSMISSION_FAILURE", "CRITICAL", siteId, region,
                "TRANSMISSION_NODE", siteId, null,
                "Transmission failure", Instant.now(), Instant.now(), "OSS");
    }

    private Incident buildIncident(String severity, String region, String siteId) {
        return Incident.builder()
                .id(UUID.randomUUID())
                .title("[" + severity + "] Test Incident - " + siteId)
                .probableRootCause("Fiber/Transmission Failure")
                .status("OPEN")
                .severity(severity)
                .region(region)
                .siteId(siteId)
                .serviceImpactCount(1)
                .firstSeenAt(Instant.now())
                .lastSeenAt(Instant.now())
                .assignedTeam("Transmission L2 Team")
                .build();
    }
}
