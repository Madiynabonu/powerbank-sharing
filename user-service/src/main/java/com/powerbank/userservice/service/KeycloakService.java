package com.powerbank.userservice.service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.keycloak.representations.idm.CredentialRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

@Service
@Slf4j
public class KeycloakService {

    private final String serverUrl;
    private final String realm;
    private final String userClientId;
    private final String userClientSecret;
    private final String passwordSalt;
    private final Keycloak adminClient;
    private final WebClient webClient;

    public KeycloakService(
            @Value("${keycloak.server-url}") String serverUrl,
            @Value("${keycloak.realm}") String realm,
            @Value("${keycloak.admin-client-id}") String adminClientId,
            @Value("${keycloak.admin-client-secret}") String adminClientSecret,
            @Value("${keycloak.user-client-id}") String userClientId,
            @Value("${keycloak.user-client-secret}") String userClientSecret,
            @Value("${keycloak.auth-password-salt}") String passwordSalt,
            WebClient.Builder builder) {
        this.serverUrl = serverUrl;
        this.realm = realm;
        this.userClientId = userClientId;
        this.userClientSecret = userClientSecret;
        this.passwordSalt = passwordSalt;
        this.adminClient = KeycloakBuilder.builder()
                .serverUrl(serverUrl)
                .realm("master")
                .clientId(adminClientId)
                .clientSecret(adminClientSecret)
                .grantType("client_credentials")
                .build();
        this.webClient = builder
                .baseUrl(serverUrl)
                .build();
    }

    /**
     * Find or create the Keycloak user by phone (stored as username).
     * Uses a deterministic password derived from phone + salt so we can later
     * issue tokens via ROPC without storing the credential ourselves.
     */
    public String getOrCreateUser(String phone) {
        List<UserRepresentation> existing = adminClient.realm(realm)
                .users()
                .searchByUsername(phone, true);

        if (!existing.isEmpty()) {
            return existing.get(0).getId();
        }

        UserRepresentation user = new UserRepresentation();
        user.setUsername(phone);
        user.setEnabled(true);
        user.setAttributes(Map.of("phone", Collections.singletonList(phone)));

        CredentialRepresentation cred = new CredentialRepresentation();
        cred.setType(CredentialRepresentation.PASSWORD);
        cred.setValue(derivePassword(phone));
        cred.setTemporary(false);
        user.setCredentials(Collections.singletonList(cred));

        jakarta.ws.rs.core.Response resp = adminClient.realm(realm).users().create(user);
        if (resp.getStatus() == 409) {
            return adminClient.realm(realm).users().searchByUsername(phone, true)
                    .stream().findFirst()
                    .map(UserRepresentation::getId)
                    .orElseThrow(() -> new RuntimeException("Concurrent user creation conflict for phone=" + maskPhone(phone)));
        }
        if (resp.getStatus() != 201) {
            throw new RuntimeException("Keycloak user creation failed: HTTP " + resp.getStatus());
        }
        String location = resp.getHeaderString("Location");
        String newId = location.substring(location.lastIndexOf('/') + 1);
        log.info("Created Keycloak user {} for phone={}", newId, maskPhone(phone));
        return newId;
    }

    public TokenResponse issueToken(String phone) {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", "password");
        form.add("client_id", userClientId);
        form.add("client_secret", userClientSecret);
        form.add("username", phone);
        form.add("password", derivePassword(phone));
        form.add("scope", "openid");
        return callTokenEndpoint(form);
    }

    public TokenResponse refreshToken(String refreshToken) {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", "refresh_token");
        form.add("client_id", userClientId);
        form.add("client_secret", userClientSecret);
        form.add("refresh_token", refreshToken);
        return callTokenEndpoint(form);
    }

    private TokenResponse callTokenEndpoint(MultiValueMap<String, String> form) {
        try {
            return webClient.post()
                    .uri("/realms/" + realm + "/protocol/openid-connect/token")
                    .body(BodyInserters.fromFormData(form))
                    .retrieve()
                    .bodyToMono(TokenResponse.class)
                    .timeout(Duration.ofSeconds(10))
                    .block();
        } catch (WebClientResponseException e) {
            log.error("Keycloak token endpoint error {}: {}", e.getStatusCode(), e.getResponseBodyAsString());
            throw new RuntimeException("Authentication service unavailable", e);
        } catch (Exception e) {
            log.error("Keycloak token endpoint unreachable", e);
            throw new RuntimeException("Authentication service unavailable", e);
        }
    }

    private String derivePassword(String phone) {
        return UUID.nameUUIDFromBytes((phone + passwordSalt).getBytes(StandardCharsets.UTF_8)).toString();
    }

    private String maskPhone(String phone) {
        if (phone.length() < 4) return "****";
        return phone.substring(0, phone.length() - 4) + "****";
    }

    public record TokenResponse(
            String access_token,
            String refresh_token,
            Integer expires_in,
            String token_type
    ) {}
}
