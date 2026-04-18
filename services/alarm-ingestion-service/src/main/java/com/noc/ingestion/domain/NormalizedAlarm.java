package com.noc.ingestion.domain;

import com.noc.ingestion.domain.enums.AlarmType;
import com.noc.ingestion.domain.enums.Severity;
import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/** JPA entity for a normalized alarm; incidentId is populated post-correlation. */
@Entity
@Table(name = "normalized_alarms")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NormalizedAlarm {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "raw_alarm_id")
    private UUID rawAlarmId;

    @Enumerated(EnumType.STRING)
    @Column(name = "alarm_type", length = 100, nullable = false)
    private AlarmType alarmType;

    @Enumerated(EnumType.STRING)
    @Column(name = "severity", length = 20, nullable = false)
    private Severity severity;

    @Column(name = "site_id", length = 100)
    private String siteId;

    @Column(name = "region", length = 100)
    private String region;

    @Column(name = "device_type", length = 100)
    private String deviceType;

    @Column(name = "device_id", length = 100)
    private String deviceId;

    @Column(name = "service_affected", length = 200)
    private String serviceAffected;

    @Column(name = "description", columnDefinition = "TEXT")
    private String description;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    /** Set by correlation-engine when this alarm is assigned to an incident. */
    @Column(name = "correlated_at")
    private Instant correlatedAt;

    /** FK to incidents.id — populated post-correlation. */
    @Column(name = "incident_id")
    private UUID incidentId;

    /** True if this alarm is identified as the root cause in its incident. */
    @Column(name = "is_root_cause", nullable = false)
    private boolean rootCause;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @PrePersist
    protected void onCreate() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }
}
