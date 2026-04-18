package com.noc.ingestion.domain.enums;

/**
 * Canonical alarm types recognized by the NOC platform.
 * All incoming alarms are normalized to one of these types.
 */
public enum AlarmType {
    SITE_DOWN,
    FIBER_CUT,
    PACKET_LOSS_HIGH,
    SERVICE_DEGRADED,
    CPU_HIGH,
    POWER_LOSS,
    LINK_DOWN,
    CUSTOMER_COMPLAINT_SPIKE,
    TRANSMISSION_FAILURE,
    SERVICE_LATENCY
}
