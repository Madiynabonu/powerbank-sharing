package com.powerbank.stationservice.messaging.event;

import java.util.UUID;

/** Published by rental-service when a rental is finished; station-service docks the powerbank back. */
public record ReturnPowerbankCommand(
        UUID rentalId,
        UUID powerbankId,
        UUID returnStationId
) {}
