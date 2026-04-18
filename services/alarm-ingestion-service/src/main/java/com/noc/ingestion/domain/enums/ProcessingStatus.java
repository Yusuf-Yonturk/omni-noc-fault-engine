package com.noc.ingestion.domain.enums;

/**
 * Processing lifecycle status for raw alarm records.
 */
public enum ProcessingStatus {
    PENDING,
    PROCESSING,
    PROCESSED,
    FAILED
}
