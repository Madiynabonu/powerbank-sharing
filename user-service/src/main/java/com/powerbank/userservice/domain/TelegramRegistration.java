package com.powerbank.userservice.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "telegram_registrations")
@Getter
@Setter
@NoArgsConstructor
public class TelegramRegistration {

    @Id
    private UUID id;

    @Column(nullable = false, unique = true)
    private String phone;

    @Column(name = "chat_id", nullable = false)
    private String chatId;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt = OffsetDateTime.now();

    public static TelegramRegistration of(String phone, String chatId) {
        TelegramRegistration r = new TelegramRegistration();
        r.id = UUID.randomUUID();
        r.phone = phone;
        r.chatId = chatId;
        return r;
    }
}
