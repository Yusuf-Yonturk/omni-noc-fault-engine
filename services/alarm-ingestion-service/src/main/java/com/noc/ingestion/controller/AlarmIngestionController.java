package com.noc.ingestion.controller;

import com.noc.ingestion.dto.AlarmIngestRequest;
import com.noc.ingestion.dto.AlarmIngestResponse;
import com.noc.ingestion.dto.ApiResponse;
import com.noc.ingestion.dto.NormalizedAlarmDto;
import com.noc.ingestion.dto.RawAlarmDto;
import com.noc.ingestion.service.AlarmIngestionService;
import com.noc.ingestion.service.AlarmNormalizationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.util.UUID;

/** REST controller for alarm ingestion and query operations. */
@Tag(name = "Alarm Ingestion", description = "APIs for ingesting and querying alarms")
@RestController
@RequestMapping("/api/v1/alarms")
@RequiredArgsConstructor
@Slf4j
public class AlarmIngestionController {

    private final AlarmIngestionService alarmIngestionService;
    private final AlarmNormalizationService alarmNormalizationService;

    /** Ingest a raw alarm, normalize it, and publish to Kafka. */
    @Operation(
        summary = "Ingest a new alarm",
        description = "Accepts an alarm from any source system, stores it raw, normalizes it, " +
                      "and publishes an AlarmNormalizedEvent to Kafka."
    )
    @PostMapping("/ingest")
    public ResponseEntity<ApiResponse<AlarmIngestResponse>> ingestAlarm(
            @Valid @RequestBody AlarmIngestRequest request) {

        log.info("Alarm ingest received: sourceSystem={}, alarmType={}, siteId={}, severity={}",
                request.sourceSystem(), request.alarmType(), request.siteId(), request.severity());

        AlarmIngestResponse response = alarmIngestionService.ingestAndNormalize(request);

        log.info("Alarm accepted: rawAlarmId={}", response.rawAlarmId());

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(ApiResponse.success(response));
    }

    /** Returns paginated raw alarms, optionally filtered by status. */
    @Operation(
        summary = "List raw alarms",
        description = "Returns paginated raw alarms. Filterable by processing status."
    )
    @GetMapping("/raw")
    public ResponseEntity<ApiResponse<Page<RawAlarmDto>>> getRawAlarms(
            @Parameter(description = "Processing status filter: PENDING, PROCESSING, PROCESSED, FAILED")
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        size = Math.min(size, 100);
        PageRequest pageable = PageRequest.of(page, size, Sort.by("receivedAt").descending());
        Page<RawAlarmDto> result = alarmIngestionService.getRawAlarms(status, pageable);

        return ResponseEntity.ok(ApiResponse.success(result));
    }

    /** Returns a single raw alarm by UUID. */
    @Operation(summary = "Get raw alarm by ID")
    @GetMapping("/raw/{id}")
    public ResponseEntity<ApiResponse<RawAlarmDto>> getRawAlarmById(
            @PathVariable UUID id) {

        RawAlarmDto alarm = alarmIngestionService.getRawAlarmById(id);
        return ResponseEntity.ok(ApiResponse.success(alarm));
    }

    /** Returns paginated normalized alarms, optionally filtered by site, region, and time range. */
    @Operation(
        summary = "Query normalized alarms",
        description = "Returns paginated normalized alarms. Filterable by siteId, region, and time range."
    )
    @GetMapping("/normalized")
    public ResponseEntity<ApiResponse<Page<NormalizedAlarmDto>>> getNormalizedAlarms(
            @Parameter(description = "Site ID filter") @RequestParam(required = false) String siteId,
            @Parameter(description = "Region filter")  @RequestParam(required = false) String region,
            @Parameter(description = "From timestamp (ISO 8601)")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from,
            @Parameter(description = "To timestamp (ISO 8601)")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime to,
            @RequestParam(defaultValue = "0")  int page,
            @RequestParam(defaultValue = "20") int size) {

        size = Math.min(size, 100);
        PageRequest pageable = PageRequest.of(page, size, Sort.by("occurredAt").descending());
        Page<NormalizedAlarmDto> result =
                alarmNormalizationService.queryNormalizedAlarms(siteId, region, from, to, pageable);

        return ResponseEntity.ok(ApiResponse.success(result));
    }
}
