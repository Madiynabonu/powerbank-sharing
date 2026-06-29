package com.powerbank.rentalservice.messaging.event;

import java.util.UUID;

public record EjectResult(
        UUID rentalId,
        UUID stationId,
        UUID powerbankId,
        Integer slotNumber,
        boolean success,
        String errorReason
) {
}
