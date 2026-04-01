package com.noc.ingestion.event;

import com.noc.ingestion.domain.NormalizedAlarm;
import com.noc.ingestion.domain.enums.AlarmType;
import com.noc.ingestion.domain.enums.Severity;

import java.time.Instant;
import java.util.UUID;

/**
 * Kafka event published to {@code alarm.normalized} after successful normalization.
 * Keyed by siteId to guarantee ordered processing per site in the correlation engine.
 */
public record AlarmNormalizedEvent(

        UUID alarmId,
        UUID rawAlarmId,
        AlarmType alarmType,
        /** Severity after boost rules applied. */
        Severity severity,
        /** Used as Kafka partition key. */
        String siteId,
        String region,
        String deviceType,
        String deviceId,
        String serviceAffected,
        String description,
        Instant occurredAt,
        Instant publishedAt,
        String sourceSystem

) {

    /** Creates an event from a persisted NormalizedAlarm entity. */
    public static AlarmNormalizedEvent from(NormalizedAlarm alarm) {
        return new AlarmNormalizedEvent(
                alarm.getId(),
                alarm.getRawAlarmId(),
                alarm.getAlarmType(),
                alarm.getSeverity(),
                alarm.getSiteId(),
                alarm.getRegion(),
                alarm.getDeviceType(),
                alarm.getDeviceId(),
                alarm.getServiceAffected(),
                alarm.getDescription(),
                alarm.getOccurredAt(),
                Instant.now(),
                null  // sourceSystem resolved from RawAlarm if needed downstream
        );
    }
}
