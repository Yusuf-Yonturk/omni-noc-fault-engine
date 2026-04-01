package com.noc.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

/** Unified API response wrapper: {data, timestamp, status, message}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ApiResponse<T>(
        T data,
        Instant timestamp,
        String status,
        String message
) {
    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>(data, Instant.now(), "SUCCESS", null);
    }

    public static <T> ApiResponse<T> error(String message) {
        return new ApiResponse<>(null, Instant.now(), "ERROR", message);
    }
}
