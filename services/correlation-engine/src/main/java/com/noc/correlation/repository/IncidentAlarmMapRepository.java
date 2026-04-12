package com.noc.correlation.repository;

import com.noc.correlation.domain.IncidentAlarmMap;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface IncidentAlarmMapRepository extends JpaRepository<IncidentAlarmMap, UUID> {

    @Query("SELECT COUNT(m) FROM IncidentAlarmMap m WHERE m.incidentId = :incidentId")
    int countByIncidentId(@Param("incidentId") UUID incidentId);
}
