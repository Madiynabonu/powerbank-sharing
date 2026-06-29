package com.powerbank.userservice.service;

import com.powerbank.userservice.domain.OtpCode;
import com.powerbank.userservice.repository.OtpCodeRepository;
import com.powerbank.userservice.util.PhoneUtils;
import java.security.SecureRandom;
import java.time.OffsetDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Generates and validates OTP codes.
 *
 * OTP delivery channel: production would use a Telegram bot; in dev mode the
 * code is logged. Set TELEGRAM_BOT_TOKEN + TELEGRAM_ENABLED=true to enable
 * real delivery via the Telegram Bot API.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class OtpService {

    private final OtpCodeRepository otpCodeRepository;
    private final TelegramOtpSender telegramOtpSender;

    @Value("${app.otp.length:6}")
    private int otpLength;

    @Value("${app.otp.ttl-minutes:5}")
    private int ttlMinutes;

    private static final SecureRandom RANDOM = new SecureRandom();

    @Transactional
    public void sendOtp(String phone) {
        otpCodeRepository.expireByPhone(phone);
        String code = generateCode();
        OtpCode otp = OtpCode.create(phone, code, ttlMinutes);
        otpCodeRepository.save(otp);

        telegramOtpSender.send(phone, code);
        log.info("OTP sent to phone={} (masked)", PhoneUtils.mask(phone));
    }

    @Transactional
    public boolean verifyOtp(String phone, String submittedCode) {
        return otpCodeRepository.findLatestValid(phone, OffsetDateTime.now())
                .map(otp -> {
                    if (!otp.getCode().equals(submittedCode)) {
                        return false;
                    }
                    otp.setUsed(true);
                    otpCodeRepository.save(otp);
                    return true;
                })
                .orElse(false);
    }

    private String generateCode() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < otpLength; i++) {
            sb.append(RANDOM.nextInt(10));
        }
        return sb.toString();
    }

}
