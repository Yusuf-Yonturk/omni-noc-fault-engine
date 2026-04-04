package com.noc.ingestion.repository;

import com.noc.ingestion.domain.RawAlarm;
import com.noc.ingestion.domain.enums.ProcessingStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

/**
 * Spring Data JPA repository for {@link RawAlarm} entities.
 */
@Repository
public interface RawAlarmRepository extends JpaRepository<RawAlarm, UUID> {

    Page<RawAlarm> findByProcessingStatus(ProcessingStatus status, Pageable pageable);
}
