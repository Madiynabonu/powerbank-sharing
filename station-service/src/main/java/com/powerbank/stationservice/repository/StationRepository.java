package com.powerbank.stationservice.repository;

import com.powerbank.stationservice.domain.Station;
import com.powerbank.stationservice.domain.StationStatus;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface StationRepository extends JpaRepository<Station, UUID> {

    /**
     * Haversine approximation in pure JPQL — no PostGIS required.
     * The formula gives the great-circle distance between two points in metres.
     * For the scale of a city (< 100 km) the flat-earth error is < 0.1%.
     * The composite index on (lat, lng) helps the planner narrow the candidate
     * set before the ORDER BY — see DECISIONS.md for index rationale.
     */
    @Query("""
            select s from Station s
            where s.status = :status
              and (:radiusM <= 0 or
                (6371000 * acos(
                    cos(radians(:lat)) * cos(radians(s.lat))
                    * cos(radians(s.lng) - radians(:lng))
                    + sin(radians(:lat)) * sin(radians(s.lat))
                )) <= :radiusM)
            order by (6371000 * acos(
                    cos(radians(:lat)) * cos(radians(s.lat))
                    * cos(radians(s.lng) - radians(:lng))
                    + sin(radians(:lat)) * sin(radians(s.lat))
                )) asc
            """)
    List<Station> findNearby(
            @Param("lat") double lat,
            @Param("lng") double lng,
            @Param("radiusM") int radiusM,
            @Param("status") StationStatus status,
            Pageable pageable);
}
