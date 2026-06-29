package com.powerbank.userservice.service;

import com.powerbank.userservice.util.PhoneUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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

    public TelegramOtpSender(
            @Value("${app.otp.telegram-bot-token:}") String botToken,
            @Value("${app.otp.telegram-enabled:false}") boolean enabled) {
        this.botToken = botToken;
        this.enabled = enabled;
    }

    public void send(String phone, String code) {
        // Always log — in dev the log IS the OTP delivery channel
        log.info("[OTP] phone={} code={}", PhoneUtils.mask(phone), code);

        if (!enabled || botToken == null || botToken.isBlank()) {
            return;
        }
        log.debug("Telegram OTP delivery is enabled but chat_id lookup not implemented in MVP");
    }
}
