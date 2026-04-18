package com.noc.correlation.controller;

import com.noc.correlation.service.CorrelationEngine;
import com.noc.correlation.service.CorrelationEngine.CorrelationStats;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** REST API for inspecting correlation rules and real-time engine statistics. */
@Tag(name = "Correlation Engine", description = "Correlation rules and runtime statistics")
@RestController
@RequestMapping("/api/v1/correlation")
@RequiredArgsConstructor
public class CorrelationController {

    private final CorrelationEngine correlationEngine;

    /** Returns the configured correlation rules as a human-readable list. */
    @Operation(summary = "List all active correlation rules")
    @GetMapping("/rules")
    public ResponseEntity<Map<String, Object>> getRules() {
        List<Map<String, String>> rules = List.of(
                Map.of(
                        "ruleId",      "RULE_1",
                        "name",        "Time + Site Window",
                        "description", "Alarms from the same siteId within a 10-minute sliding window are grouped into one incident.",
                        "implementation", "Redis key: correlation:{siteId}, TTL: 600 seconds"
                ),
                Map.of(
                        "ruleId",      "RULE_2",
                        "name",        "Severity Boost",
                        "description", "Alarms from CORE_ROUTER or TRANSMISSION_NODE devices are boosted one severity level (e.g. MAJOR → CRITICAL).",
                        "implementation", "Applied by alarm-ingestion-service during normalization"
                ),
                Map.of(
                        "ruleId",      "RULE_3",
                        "name",        "Parent-Child Detection",
                        "description", "If a FIBER_CUT or LINK_DOWN alarm opens an incident, subsequent SERVICE_DEGRADED, PACKET_LOSS_HIGH, CUSTOMER_COMPLAINT_SPIKE from the same site within the window are mapped as CHILD alarms.",
                        "implementation", "relation_type = CHILD in incident_alarm_map"
                ),
                Map.of(
                        "ruleId",      "RULE_4",
                        "name",        "Root Cause Identification",
                        "description", "The first alarm in a new incident window is classified as ROOT_CAUSE. It determines the incident title, probable_root_cause, and assigned_team.",
                        "implementation", "relation_type = ROOT_CAUSE, is_root_cause = true"
                )
        );

        return ResponseEntity.ok(Map.of(
                "rules",     rules,
                "timestamp", Instant.now(),
                "status",    "SUCCESS"
        ));
    }

    /** Returns real-time statistics from the correlation engine. */
    @Operation(summary = "Correlation engine runtime statistics")
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        CorrelationStats stats = correlationEngine.getStats();

        return ResponseEntity.ok(Map.of(
                "data",      stats,
                "timestamp", Instant.now(),
                "status",    "SUCCESS"
        ));
    }
}
