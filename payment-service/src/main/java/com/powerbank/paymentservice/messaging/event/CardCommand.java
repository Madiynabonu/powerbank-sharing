package com.powerbank.paymentservice.messaging.event;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Command consumed from {@code card-command} to bind (register) a card.
 * Models "card binding" without a real acquirer — see DECISIONS.md.
 */
public record CardCommand(
        String commandType,   // BIND
        UUID cardId,
        UUID userId,
        String maskedPan,
        BigDecimal initialBalance,
        String currency
) {
}
