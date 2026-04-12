package com.noc.correlation.repository;

import com.noc.correlation.domain.Incident;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface IncidentRepository extends JpaRepository<Incident, UUID> {

    @Query("SELECT i FROM Incident i WHERE i.siteId = :siteId AND i.status = 'OPEN' ORDER BY i.firstSeenAt DESC LIMIT 1")
    Optional<Incident> findLatestOpenBySiteId(@Param("siteId") String siteId);

    @Query("SELECT COUNT(i) FROM Incident i WHERE i.status = 'OPEN'")
    long countOpenIncidents();

    @Query("SELECT COUNT(i) FROM Incident i WHERE i.status = 'OPEN' AND i.severity = 'CRITICAL'")
    long countOpenCriticalIncidents();
}
