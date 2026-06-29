package com.powerbank.stationservice.messaging.event;

import java.util.UUID;

/** Reply to rental-service after the powerbank is dispensed (or failed). */
public record EjectResult(
        UUID rentalId,
        UUID stationId,
        UUID powerbankId,
        Integer slotNumber,
        boolean success,
        String errorReason
) {
}
