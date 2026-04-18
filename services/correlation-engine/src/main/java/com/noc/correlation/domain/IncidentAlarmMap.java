package com.noc.correlation.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/** Maps a normalized alarm to an incident (ROOT_CAUSE, CORRELATED, or CHILD). */
@Entity
@Table(name = "incident_alarm_map")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IncidentAlarmMap {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "incident_id", nullable = false)
    private UUID incidentId;

    @Column(name = "alarm_id", nullable = false)
    private UUID alarmId;

    @Column(name = "relation_type", nullable = false, length = 50)
    private String relationType;

    @Column(name = "mapped_at", nullable = false)
    private Instant mappedAt;

    @PrePersist
    protected void onCreate() {
        if (mappedAt == null) mappedAt = Instant.now();
        if (relationType == null) relationType = "CORRELATED";
    }
}
