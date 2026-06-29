package com.powerbank.rentalservice.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "rentals")
@Getter
@Setter
@NoArgsConstructor
public class Rental {

    @Id
    private UUID id;

    @Column(name = "user_id", nullable = false)
    private UUID userId;

    @Column(name = "station_id", nullable = false)
    private UUID stationId;

    @Column(name = "return_station_id")
    private UUID returnStationId;

    @Column(name = "card_id", nullable = false)
    private UUID cardId;

    @Column(name = "powerbank_id")
    private UUID powerbankId;

    @Column(name = "slot_number")
    private Integer slotNumber;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private RentalStatus status = RentalStatus.WAITING;

    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "started_at")
    private OffsetDateTime startedAt;

    @Column(name = "finished_at")
    private OffsetDateTime finishedAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    public static Rental create(UUID userId, UUID stationId, UUID cardId) {
        Rental r = new Rental();
        r.setId(UUID.randomUUID());
        r.setUserId(userId);
        r.setStationId(stationId);
        r.setCardId(cardId);
        r.setStatus(RentalStatus.WAITING);
        return r;
    }

    public void transitionTo(RentalStatus next) {
        this.status = next;
    }

    public void fail(String reason) {
        this.status = RentalStatus.FAILED;
        this.failureReason = reason;
    }

    @PrePersist
    void onCreate() {
        OffsetDateTime now = OffsetDateTime.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    @PreUpdate
    void onUpdate() {
        this.updatedAt = OffsetDateTime.now();
    }
}
