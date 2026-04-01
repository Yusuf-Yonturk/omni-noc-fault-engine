package com.noc.ingestion.dto;

import com.noc.ingestion.domain.enums.AlarmType;
import com.noc.ingestion.domain.enums.Severity;

import java.time.Instant;
import java.util.UUID;

/** DTO for normalized alarm query responses. */
public record NormalizedAlarmDto(
        UUID id,
        UUID rawAlarmId,
        AlarmType alarmType,
        Severity severity,
        String siteId,
        String region,
        String deviceType,
        String deviceId,
        String serviceAffected,
        String description,
        Instant occurredAt,
        Instant correlatedAt,
        UUID incidentId,
        boolean rootCause,
        Instant createdAt
) {}
