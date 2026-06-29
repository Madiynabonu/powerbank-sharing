package com.powerbank.stationservice.messaging.event;

import java.util.UUID;

/** Command from rental-service: physically eject the powerbank from its slot. */
public record EjectCommand(
        String correlationId,
        UUID rentalId,
        UUID stationId,
        UUID powerbankId,
        Integer slotNumber
) {
}
