package com.powerbank.userservice.controller;

import com.powerbank.userservice.domain.User;
import com.powerbank.userservice.repository.UserRepository;
import com.powerbank.userservice.service.KeycloakService;
import com.powerbank.userservice.service.OtpService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.transaction.annotation.Transactional;
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
    private final UserRepository userRepository;

    /** POST /auth/phone — request OTP */
    @PostMapping("/auth/phone")
    @Transactional
    public ResponseEntity<?> requestOtp(@Valid @RequestBody PhoneRequest req) {
        otpService.sendOtp(req.phone());
        return ResponseEntity.ok(Map.of("message", "OTP sent"));
    }

    /** POST /auth/verify — verify OTP, receive JWT */
    @PostMapping("/auth/verify")
    @Transactional
    public ResponseEntity<?> verify(@Valid @RequestBody VerifyRequest req) {
        boolean valid = otpService.verifyOtp(req.phone(), req.code());
        if (!valid) {
            return ResponseEntity.status(401).body(Map.of("error", "Invalid or expired OTP"));
        }

        String keycloakId = keycloakService.getOrCreateUser(req.phone());
        userRepository.findByPhone(req.phone()).orElseGet(() -> {
            User u = User.create(req.phone());
            u.setKeycloakId(keycloakId);
            return userRepository.save(u);
        });

        KeycloakService.TokenResponse tokens = keycloakService.issueToken(req.phone());
        return ResponseEntity.ok(tokens);
    }

    /** POST /v1/auth/refresh — refresh JWT */
    @PostMapping("/v1/auth/refresh")
    public ResponseEntity<?> refresh(@Valid @RequestBody RefreshRequest req) {
        KeycloakService.TokenResponse tokens = keycloakService.refreshToken(req.refreshToken());
        return ResponseEntity.ok(tokens);
    }

    /** GET /v1/me — authenticated user profile */
    @GetMapping("/v1/me")
    public ResponseEntity<?> me(@AuthenticationPrincipal Jwt jwt) {
        String phone = jwt.getSubject();
        return userRepository.findByPhone(phone)
                .map(u -> ResponseEntity.ok(Map.of(
                        "id", u.getId(),
                        "phone", u.getPhone(),
                        "keycloakId", u.getKeycloakId() != null ? u.getKeycloakId() : ""
                )))
                .orElse(ResponseEntity.notFound().build());
    }

    record PhoneRequest(@NotBlank String phone) {
    }

    record VerifyRequest(@NotBlank String phone, @NotBlank String code) {
    }

    record RefreshRequest(@NotBlank String refreshToken) {
    }
}
