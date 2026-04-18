package com.noc.correlation.event;

import java.time.Instant;
import java.util.UUID;

/** Outbound Kafka event published to incident.updated after correlation. */
public record IncidentUpdatedEvent(
        UUID incidentId,
        UUID alarmId,
        String action,              // INCIDENT_CREATED | ALARM_CORRELATED
        String status,              // OPEN, ACKNOWLEDGED, etc.
        String severity,
        String region,
        String siteId,
        String probableRootCause,
        String assignedTeam,
        int    correlatedAlarmCount,
        Instant eventTime
) {
    public static IncidentUpdatedEvent incidentCreated(
            UUID incidentId, UUID alarmId, String severity,
            String region, String siteId,
            String rootCause, String team) {
        return new IncidentUpdatedEvent(
                incidentId, alarmId, "INCIDENT_CREATED",
                "OPEN", severity, region, siteId,
                rootCause, team, 1, Instant.now()
        );
    }

    public static IncidentUpdatedEvent alarmCorrelated(
            UUID incidentId, UUID alarmId, String severity,
            String region, String siteId,
            String rootCause, String team, int alarmCount) {
        return new IncidentUpdatedEvent(
                incidentId, alarmId, "ALARM_CORRELATED",
                "OPEN", severity, region, siteId,
                rootCause, team, alarmCount, Instant.now()
        );
    }
}
