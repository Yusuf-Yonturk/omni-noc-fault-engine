package com.noc.ingestion.domain.enums;

/**
 * Alarm severity levels in ascending order of criticality.
 * CRITICAL is the highest — triggers immediate escalation rules.
 */
public enum Severity {
    INFO,
    WARNING,
    MINOR,
    MAJOR,
    CRITICAL
}
