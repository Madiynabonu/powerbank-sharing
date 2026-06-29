package com.powerbank.userservice.service;

import com.powerbank.userservice.domain.User;
import com.powerbank.userservice.repository.UserRepository;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
@RequiredArgsConstructor
public class UserService {

    private final UserRepository userRepository;
    private final KeycloakService keycloakService;

    /**
     * Find or create the local user record after OTP verification, then issue tokens.
     * Keycloak user creation happens first (external); local DB is updated within a
     * transaction so a failed save does not leave an orphaned Keycloak account visible
     * to the app.
     */
    @Transactional
    public KeycloakService.TokenResponse registerAndIssueToken(String phone) {
        String keycloakId = keycloakService.getOrCreateUser(phone);
        userRepository.findByPhone(phone).orElseGet(() -> {
            User u = User.create(phone);
            u.setKeycloakId(keycloakId);
            return userRepository.save(u);
        });
        return keycloakService.issueToken(phone);
    }

    public Optional<User> findByPhone(String phone) {
        return userRepository.findByPhone(phone);
    }
}
