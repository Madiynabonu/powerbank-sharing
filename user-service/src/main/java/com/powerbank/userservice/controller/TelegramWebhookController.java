package com.powerbank.userservice.controller;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.powerbank.userservice.domain.TelegramRegistration;
import com.powerbank.userservice.repository.TelegramRegistrationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Receives Telegram bot updates via webhook.
 *
 * Registration flow:
 *   1. User messages the bot with their phone number: "+998901234567"
 *   2. This endpoint saves phone → chat_id
 *   3. Subsequent OTP requests for that phone are delivered to this chat_id
 *
 * Set webhook once:
 *   GET https://api.telegram.org/bot{TOKEN}/setWebhook?url=https://your-host/telegram/webhook
 */
@RestController
@RequestMapping("/telegram/webhook")
@RequiredArgsConstructor
@Slf4j
public class TelegramWebhookController {

    private final TelegramRegistrationRepository telegramRegistrationRepository;

    @PostMapping
    public ResponseEntity<Void> handleUpdate(@RequestBody TelegramUpdate update) {
        if (update.message() == null || update.message().text() == null) {
            return ResponseEntity.ok().build();
        }

        String text = update.message().text().trim();
        String chatId = String.valueOf(update.message().from().id());

        if (!text.startsWith("+") && !text.matches("\\d{9,15}")) {
            log.debug("Telegram message from chat_id={} is not a phone number: {}", chatId, text);
            return ResponseEntity.ok().build();
        }

        String phone = text.startsWith("+") ? text : "+" + text;

        telegramRegistrationRepository.findByPhone(phone).ifPresentOrElse(
                reg -> {
                    reg.setChatId(chatId);
                    telegramRegistrationRepository.save(reg);
                    log.info("Updated Telegram chat_id for phone={}", phone);
                },
                () -> {
                    telegramRegistrationRepository.save(TelegramRegistration.of(phone, chatId));
                    log.info("Registered Telegram chat_id for phone={}", phone);
                }
        );

        return ResponseEntity.ok().build();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record TelegramUpdate(Long updateId, TelegramMessage message) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record TelegramMessage(Long messageId, TelegramUser from, String text) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record TelegramUser(Long id, String username) {}
}
