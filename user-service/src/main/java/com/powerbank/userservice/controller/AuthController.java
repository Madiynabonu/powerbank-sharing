package com.powerbank.userservice.controller;

import com.powerbank.userservice.service.KeycloakService;
import com.powerbank.userservice.service.OtpService;
import com.powerbank.userservice.service.UserService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import java.util.Locale;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.MessageSource;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequiredArgsConstructor
public class AuthController {

    private final OtpService otpService;
    private final KeycloakService keycloakService;
    private final UserService userService;
    private final MessageSource messageSource;

    @PostMapping("/auth/phone")
    public ResponseEntity<?> requestOtp(@Valid @RequestBody PhoneRequest req, Locale locale) {
        otpService.sendOtp(req.phone());
        return ResponseEntity.ok(Map.of("message", messageSource.getMessage("otp.sent", null, locale)));
    }

    @PostMapping("/auth/verify")
    public ResponseEntity<?> verify(@Valid @RequestBody VerifyRequest req, Locale locale) {
        boolean valid = otpService.verifyOtp(req.phone(), req.code());
        if (!valid) {
            return ResponseEntity.status(401)
                    .body(Map.of("error", messageSource.getMessage("auth.otp.invalid", null, locale)));
        }
        KeycloakService.TokenResponse tokens = userService.registerAndIssueToken(req.phone());
        return ResponseEntity.ok(tokens);
    }

    @PostMapping("/v1/auth/refresh")
    public ResponseEntity<?> refresh(@Valid @RequestBody RefreshRequest req) {
        KeycloakService.TokenResponse tokens = keycloakService.refreshToken(req.refreshToken());
        return ResponseEntity.ok(tokens);
    }

    @GetMapping("/v1/me")
    public ResponseEntity<?> me(@AuthenticationPrincipal Jwt jwt) {
        String phone = jwt.getClaimAsString("preferred_username");
        return userService.findByPhone(phone)
                .map(u -> ResponseEntity.ok(Map.of(
                        "id", u.getId(),
                        "phone", u.getPhone(),
                        "keycloakId", u.getKeycloakId() != null ? u.getKeycloakId() : ""
                )))
                .orElse(ResponseEntity.notFound().build());
    }

    record PhoneRequest(@NotBlank String phone) {}

    record VerifyRequest(@NotBlank String phone, @NotBlank String code) {}

    record RefreshRequest(@NotBlank String refreshToken) {}
}
