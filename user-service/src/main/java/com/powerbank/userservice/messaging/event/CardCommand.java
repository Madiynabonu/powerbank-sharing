package com.powerbank.userservice.messaging.event;

import java.math.BigDecimal;
import java.util.UUID;

public record CardCommand(
        String commandType,     // BIND
        UUID cardId,
        UUID userId,
        String maskedPan,
        BigDecimal initialBalance,
        String currency
) {}
