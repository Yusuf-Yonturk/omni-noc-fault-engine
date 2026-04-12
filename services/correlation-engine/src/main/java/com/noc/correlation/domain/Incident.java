package com.noc.correlation.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/** JPA entity representing a correlated network incident. */
@Entity
@Table(name = "incidents")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Incident {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "title", nullable = false, length = 500)
    private String title;

    @Column(name = "probable_root_cause", columnDefinition = "TEXT")
    private String probableRootCause;

    @Column(name = "status", nullable = false, length = 50)
    private String status;

    @Column(name = "severity", nullable = false, length = 20)
    private String severity;

    @Column(name = "region", length = 100)
    private String region;

    @Column(name = "site_id", length = 100)
    private String siteId;

    @Column(name = "service_impact_count", nullable = false)
    private int serviceImpactCount;

    @Column(name = "first_seen_at", nullable = false)
    private Instant firstSeenAt;

    @Column(name = "last_seen_at")
    private Instant lastSeenAt;

    @Column(name = "assigned_team", length = 200)
    private String assignedTeam;

    @Column(name = "acknowledged_by", length = 200)
    private String acknowledgedBy;

    @Column(name = "acknowledged_at")
    private Instant acknowledgedAt;

    @Column(name = "resolved_at")
    private Instant resolvedAt;

    @Column(name = "closed_at")
    private Instant closedAt;

    @Column(name = "improvement_ticket_created", nullable = false)
    private boolean improvementTicketCreated;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @PrePersist
    protected void onCreate() {
        Instant now = Instant.now();
        createdAt = now;
        updatedAt = now;
        if (status == null) status = "OPEN";
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now();
    }
}
