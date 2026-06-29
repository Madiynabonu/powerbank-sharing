package com.powerbank.userservice.service;

import com.powerbank.userservice.repository.TelegramRegistrationRepository;
import com.powerbank.userservice.util.PhoneUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@Component
@Slf4j
public class TelegramOtpSender {

    private final WebClient webClient;
    private final TelegramRegistrationRepository telegramRegistrationRepository;
    private final String botToken;

    public TelegramOtpSender(
            WebClient.Builder builder,
            TelegramRegistrationRepository telegramRegistrationRepository,
            @Value("${app.otp.telegram-bot-token:}") String botToken) {
        this.webClient = builder.baseUrl("https://api.telegram.org").build();
        this.telegramRegistrationRepository = telegramRegistrationRepository;
        this.botToken = botToken;
    }

    public void send(String phone, String code) {
        log.info("[OTP] phone={} code={}", PhoneUtils.mask(phone), code);

        if (botToken.isBlank()) {
            log.warn("Bot token not configured — OTP logged only");
            return;
        }

        String chatId = telegramRegistrationRepository.findByPhone(phone)
                .map(r -> r.getChatId())
                .orElse(null);

        if (chatId == null) {
            log.warn("No Telegram chat_id for phone={} — OTP logged only. " +
                    "User must send their phone number to the bot first.", PhoneUtils.mask(phone));
            return;
        }

        String text = "Sizning OTP kodingiz: *" + code + "*";
        try {
            webClient.get()
                    .uri("/bot{token}/sendMessage?chat_id={chatId}&text={text}&parse_mode=Markdown",
                            botToken, chatId, text)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();
            log.info("OTP sent via Telegram to chat_id={}", chatId);
        } catch (Exception e) {
            log.error("Telegram send failed: {}", e.getMessage());
        }
    }
}
