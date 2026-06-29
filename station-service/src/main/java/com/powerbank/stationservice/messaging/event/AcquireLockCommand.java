package com.powerbank.stationservice.messaging.event;

import java.util.UUID;

/** Command from rental-service: lock the cabinet at a station. */
public record AcquireLockCommand(
        String correlationId,  // == rentalId — used as the Kafka key
        UUID rentalId,
        UUID stationId
) {
}
