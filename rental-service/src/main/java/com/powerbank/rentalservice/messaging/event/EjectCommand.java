package com.powerbank.rentalservice.messaging.event;

import java.util.UUID;

public record EjectCommand(
        String correlationId,
        UUID rentalId,
        UUID stationId,
        UUID powerbankId,
        Integer slotNumber
) {
}
