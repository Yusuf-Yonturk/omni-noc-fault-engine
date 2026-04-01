package com.noc.ingestion.dto;

import java.util.UUID;

/** Response returned after successful alarm ingestion. */
public record AlarmIngestResponse(
        UUID rawAlarmId,
        String status
) {
    public static AlarmIngestResponse accepted(UUID rawAlarmId) {
        return new AlarmIngestResponse(rawAlarmId, "ACCEPTED");
    }
}
