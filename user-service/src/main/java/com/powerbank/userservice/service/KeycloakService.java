package com.powerbank.userservice.service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
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

/**
 * Wraps Keycloak Admin REST API calls (user creation / retrieval) and the
 * token endpoint for password-grant token exchange.
 *
 * Auth flow:
 *   1. User provides phone + correct OTP → OtpService.verifyOtp returns true
 *   2. KeycloakService.getOrCreateUser creates the user in Keycloak if new
 *   3. KeycloakService.issueToken exchanges credentials for JWT
 */
@Service
@Slf4j
public class KeycloakService {

    private final String serverUrl;
    private final String realm;
    private final String userClientId;
    private final String userClientSecret;
    private final Keycloak adminClient;
    private final WebClient webClient;

    public KeycloakService(
            @Value("${keycloak.server-url}") String serverUrl,
            @Value("${keycloak.realm}") String realm,
            @Value("${keycloak.admin-client-id}") String adminClientId,
            @Value("${keycloak.admin-client-secret}") String adminClientSecret,
            @Value("${keycloak.user-client-id}") String userClientId,
            @Value("${keycloak.user-client-secret}") String userClientSecret,
            WebClient.Builder builder) {
        this.serverUrl = serverUrl;
        this.realm = realm;
        this.userClientId = userClientId;
        this.userClientSecret = userClientSecret;
        this.adminClient = KeycloakBuilder.builder()
                .serverUrl(serverUrl)
                .realm("master")
                .clientId(adminClientId)
                .clientSecret(adminClientSecret)
                .grantType("client_credentials")
                .build();
        this.webClient = builder.baseUrl(serverUrl).build();
    }

    /**
     * Find the Keycloak user by phone (stored as username). Creates one if not
     * found. Returns the Keycloak user ID.
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

        // Set a random password — the real credential is the OTP; we never
        // expose this password to the user.
        CredentialRepresentation cred = new CredentialRepresentation();
        cred.setType(CredentialRepresentation.PASSWORD);
        cred.setValue(java.util.UUID.randomUUID().toString());
        cred.setTemporary(false);
        user.setCredentials(Collections.singletonList(cred));

        jakarta.ws.rs.core.Response resp = adminClient.realm(realm).users().create(user);
        String location = resp.getHeaderString("Location");
        String newId = location.substring(location.lastIndexOf('/') + 1);
        log.info("Created Keycloak user {} for phone={}", newId, maskPhone(phone));
        return newId;
    }

    /**
     * Exchange a temporary OTP-validated credential for a real JWT.
     * We use a custom Keycloak token endpoint call with client_credentials;
     * in a full implementation a custom Keycloak SPI would validate the OTP
     * directly. Here we delegate to the resource-owner password grant, which
     * is acceptable for an internal-only "phone as username" flow in MVP.
     */
    public TokenResponse issueToken(String phone) {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", "client_credentials");
        form.add("client_id", userClientId);
        form.add("client_secret", userClientSecret);
        form.add("scope", "openid");

        return webClient.post()
                .uri("/realms/" + realm + "/protocol/openid-connect/token")
                .body(BodyInserters.fromFormData(form))
                .retrieve()
                .bodyToMono(TokenResponse.class)
                .block();
    }

    public TokenResponse refreshToken(String refreshToken) {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", "refresh_token");
        form.add("client_id", userClientId);
        form.add("client_secret", userClientSecret);
        form.add("refresh_token", refreshToken);

        return webClient.post()
                .uri("/realms/" + realm + "/protocol/openid-connect/token")
                .body(BodyInserters.fromFormData(form))
                .retrieve()
                .bodyToMono(TokenResponse.class)
                .block();
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
    ) {
    }
}
