package com.powerbank.rentalservice.messaging.event;

import java.util.UUID;

public record ReturnPowerbankCommand(
        UUID rentalId,
        UUID powerbankId,
        UUID returnStationId
) {}
