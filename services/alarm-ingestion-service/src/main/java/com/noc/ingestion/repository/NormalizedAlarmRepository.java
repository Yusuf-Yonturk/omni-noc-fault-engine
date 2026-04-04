package com.noc.ingestion.repository;

import com.noc.ingestion.domain.NormalizedAlarm;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.UUID;

/**
 * Spring Data JPA repository for {@link NormalizedAlarm} entities.
 */
@Repository
public interface NormalizedAlarmRepository extends JpaRepository<NormalizedAlarm, UUID> {

    /**
     * Flexible query supporting optional filters for siteId, region, and time range.
     * Null parameters are treated as "no filter" (match-all for that dimension).
     */
    @Query("""
            SELECT n FROM NormalizedAlarm n
            WHERE (:siteId IS NULL OR n.siteId = :siteId)
              AND (:region IS NULL OR n.region = :region)
              AND n.occurredAt >= :from
              AND n.occurredAt <= :to
            """)
    Page<NormalizedAlarm> findByFilters(
            @Param("siteId")  String siteId,
            @Param("region")  String region,
            @Param("from")    Instant from,
            @Param("to")      Instant to,
            Pageable pageable
    );
}
