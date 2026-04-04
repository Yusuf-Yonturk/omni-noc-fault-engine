package com.noc.ingestion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.noc.ingestion.domain.RawAlarm;
import com.noc.ingestion.domain.enums.ProcessingStatus;
import com.noc.ingestion.dto.AlarmIngestRequest;
import com.noc.ingestion.dto.AlarmIngestResponse;
import com.noc.ingestion.dto.RawAlarmDto;
import com.noc.ingestion.mapper.NormalizedAlarmMapper;
import com.noc.ingestion.repository.RawAlarmRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;
import java.util.UUID;

/** Orchestrates raw alarm persistence and delegates to AlarmNormalizationService. */
@Service
@RequiredArgsConstructor
@Slf4j
public class AlarmIngestionService {

    private final RawAlarmRepository rawAlarmRepository;
    private final AlarmNormalizationService normalizationService;
    private final NormalizedAlarmMapper mapper;
    private final ObjectMapper objectMapper;

    /** Persists a raw alarm and triggers normalization + Kafka publish. */
    @Transactional
    public AlarmIngestResponse ingestAndNormalize(AlarmIngestRequest request) {
        // Serialize entire request as JSON for raw JSONB storage
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = objectMapper.convertValue(request, Map.class);

        RawAlarm rawAlarm = RawAlarm.builder()
                .sourceSystem(request.sourceSystem())
                .rawPayload(payload)
                .processingStatus(ProcessingStatus.PROCESSING)
                .build();

        RawAlarm saved = rawAlarmRepository.save(rawAlarm);
        log.debug("Raw alarm persisted: id={}", saved.getId());

        // Normalize and publish to Kafka
        normalizationService.normalize(saved, request);

        return AlarmIngestResponse.accepted(saved.getId());
    }

    /** Returns paginated raw alarms, optionally filtered by status. */
    @Transactional(readOnly = true)
    public Page<RawAlarmDto> getRawAlarms(String statusStr, Pageable pageable) {
        if (statusStr != null && !statusStr.isBlank()) {
            try {
                ProcessingStatus status = ProcessingStatus.valueOf(statusStr.toUpperCase());
                return rawAlarmRepository.findByProcessingStatus(status, pageable)
                        .map(mapper::toDto);
            } catch (IllegalArgumentException ex) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "Invalid processing status: " + statusStr);
            }
        }
        return rawAlarmRepository.findAll(pageable).map(mapper::toDto);
    }

    /** Returns a raw alarm by UUID, or 404 if not found. */
    @Transactional(readOnly = true)
    public RawAlarmDto getRawAlarmById(UUID id) {
        return rawAlarmRepository.findById(id)
                .map(mapper::toDto)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "Raw alarm not found: " + id));
    }
}
