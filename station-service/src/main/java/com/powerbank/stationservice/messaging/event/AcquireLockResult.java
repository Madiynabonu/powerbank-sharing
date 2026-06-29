package com.powerbank.stationservice.messaging.event;

import java.util.UUID;

/** Reply to rental-service after simulating the cabinet lock. */
public record AcquireLockResult(
        UUID rentalId,
        UUID stationId,
        UUID powerbankId,
        Integer slotNumber,
        boolean success,
        String errorReason
) {
}
