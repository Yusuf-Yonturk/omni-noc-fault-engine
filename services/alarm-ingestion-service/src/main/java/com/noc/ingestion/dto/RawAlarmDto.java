package com.noc.ingestion.dto;

import com.noc.ingestion.domain.enums.ProcessingStatus;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** DTO for raw alarm query responses. */
public record RawAlarmDto(
        UUID id,
        String sourceSystem,
        Map<String, Object> rawPayload,
        Instant receivedAt,
        ProcessingStatus processingStatus
) {}
