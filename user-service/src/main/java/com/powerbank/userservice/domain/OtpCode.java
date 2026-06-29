package com.powerbank.userservice.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "otp_codes")
@Getter
@Setter
@NoArgsConstructor
public class OtpCode {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String phone;

    @Column(nullable = false)
    private String code;

    @Column(name = "expires_at", nullable = false)
    private OffsetDateTime expiresAt;

    @Column(nullable = false)
    private boolean used = false;

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    public static OtpCode create(String phone, String code, int ttlMinutes) {
        OtpCode otp = new OtpCode();
        otp.setId(UUID.randomUUID());
        otp.setPhone(phone);
        otp.setCode(code);
        otp.setExpiresAt(OffsetDateTime.now().plusMinutes(ttlMinutes));
        return otp;
    }

    public boolean isValid() {
        return !used && OffsetDateTime.now().isBefore(expiresAt);
    }

    @PrePersist
    void onCreate() {
        this.createdAt = OffsetDateTime.now();
    }
}
