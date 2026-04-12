package com.noc.correlation.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

/** Inbound Kafka event from alarm.normalized topic, produced by ingestion service. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AlarmNormalizedEvent(
        UUID alarmId,
        UUID rawAlarmId,
        String alarmType,
        String severity,
        String siteId,
        String region,
        String deviceType,
        String deviceId,
        String serviceAffected,
        String description,
        Instant occurredAt,
        Instant publishedAt,
        String sourceSystem
) {}
