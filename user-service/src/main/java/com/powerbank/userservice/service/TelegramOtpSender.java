package com.powerbank.userservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Sends OTP codes via Telegram Bot API (or logs them in dev mode).
 *
 * In production, the phone number must be linked to a Telegram chat ID via a
 * prior registration step (out of scope for this MVP). Here we log the code
 * and optionally call the Telegram API if a token is configured.
 */
@Component
@Slf4j
public class TelegramOtpSender {

    private final String botToken;
    private final boolean enabled;
    private final WebClient webClient;

    public TelegramOtpSender(
            @Value("${app.otp.telegram-bot-token:}") String botToken,
            @Value("${app.otp.telegram-enabled:false}") boolean enabled,
            WebClient.Builder builder) {
        this.botToken = botToken;
        this.enabled = enabled;
        this.webClient = builder.baseUrl("https://api.telegram.org").build();
    }

    public void send(String phone, String code) {
        // Always log — in dev the log IS the OTP delivery channel
        log.info("[OTP] phone={} code={}", maskPhone(phone), code);

        if (!enabled || botToken == null || botToken.isBlank()) {
            return;
        }
        // Note: a real implementation would look up the Telegram chat_id for this
        // phone number from a prior /start registration flow.
        // Kept as a stub to show the integration point without a full bot.
        log.debug("Telegram OTP delivery is enabled but chat_id lookup not implemented in MVP");
    }

    private String maskPhone(String phone) {
        if (phone.length() < 4) return "****";
        return phone.substring(0, phone.length() - 4) + "****";
    }
}
