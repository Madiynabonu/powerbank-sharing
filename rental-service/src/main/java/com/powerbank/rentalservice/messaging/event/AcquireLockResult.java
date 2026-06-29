package com.powerbank.rentalservice.messaging.event;

import java.util.UUID;

public record AcquireLockResult(
        UUID rentalId,
        UUID stationId,
        UUID powerbankId,
        Integer slotNumber,
        boolean success,
        String errorReason
) {
}
