package com.noc.ingestion.mapper;

import com.noc.ingestion.domain.NormalizedAlarm;
import com.noc.ingestion.domain.RawAlarm;
import com.noc.ingestion.dto.NormalizedAlarmDto;
import com.noc.ingestion.dto.RawAlarmDto;
import org.springframework.stereotype.Component;

/** Manual entity-to-DTO mapper (hand-written to keep deps minimal). */
@Component
public class NormalizedAlarmMapper {

    public NormalizedAlarmDto toDto(NormalizedAlarm entity) {
        return new NormalizedAlarmDto(
                entity.getId(),
                entity.getRawAlarmId(),
                entity.getAlarmType(),
                entity.getSeverity(),
                entity.getSiteId(),
                entity.getRegion(),
                entity.getDeviceType(),
                entity.getDeviceId(),
                entity.getServiceAffected(),
                entity.getDescription(),
                entity.getOccurredAt(),
                entity.getCorrelatedAt(),
                entity.getIncidentId(),
                entity.isRootCause(),
                entity.getCreatedAt()
        );
    }

    public RawAlarmDto toDto(RawAlarm entity) {
        return new RawAlarmDto(
                entity.getId(),
                entity.getSourceSystem(),
                entity.getRawPayload(),
                entity.getReceivedAt(),
                entity.getProcessingStatus()
        );
    }
}
