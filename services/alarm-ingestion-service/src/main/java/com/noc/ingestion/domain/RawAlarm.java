package com.noc.ingestion.domain;

import com.noc.ingestion.domain.enums.ProcessingStatus;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** JPA entity for a verbatim raw alarm stored as JSONB for full audit replay. */
@Entity
@Table(name = "raw_alarms")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RawAlarm {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "source_system", length = 100)
    private String sourceSystem;

    /** Full original payload stored as JSONB for immutable audit trail. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "raw_payload", nullable = false, columnDefinition = "jsonb")
    private Map<String, Object> rawPayload;

    @Column(name = "received_at", nullable = false, updatable = false)
    private Instant receivedAt;

    @Enumerated(EnumType.STRING)
    @Column(name = "processing_status", length = 50, nullable = false)
    private ProcessingStatus processingStatus;

    @PrePersist
    protected void onCreate() {
        if (receivedAt == null) {
            receivedAt = Instant.now();
        }
        if (processingStatus == null) {
            processingStatus = ProcessingStatus.PENDING;
        }
    }
}
