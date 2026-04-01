package com.noc.ingestion.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.time.OffsetDateTime;

/** Request DTO for ingesting a new alarm. */
public record AlarmIngestRequest(

        @NotBlank(message = "sourceSystem is required")
        @Size(max = 100)
        String sourceSystem,

        @NotBlank(message = "alarmType is required")
        @Size(max = 100)
        String alarmType,

        @NotBlank(message = "severity is required")
        @Size(max = 20)
        String severity,

        @Size(max = 100)
        String siteId,

        @Size(max = 100)
        String region,

        @Size(max = 100)
        String deviceType,

        @Size(max = 100)
        String deviceId,

        @Size(max = 200)
        String serviceAffected,

        String description,

        /** When the fault occurred on the network; null falls back to receivedAt. */
        OffsetDateTime occurredAt
) {}
