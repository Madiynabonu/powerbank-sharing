package com.powerbank.rentalservice.messaging.event;

import java.util.UUID;

public record AcquireLockCommand(
        String correlationId,
        UUID rentalId,
        UUID stationId
) {
}
