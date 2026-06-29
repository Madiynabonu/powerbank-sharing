package com.powerbank.userservice.repository;

import com.powerbank.userservice.domain.OtpCode;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OtpCodeRepository extends JpaRepository<OtpCode, UUID> {

    @Query("""
            select o from OtpCode o
            where o.phone = :phone
              and o.used = false
              and o.expiresAt > :now
            order by o.createdAt desc
            """)
    Optional<OtpCode> findLatestValid(@Param("phone") String phone, @Param("now") OffsetDateTime now);

    /** Expire all unused codes for a phone (issued before a new OTP request). */
    @Modifying
    @Query("update OtpCode o set o.used = true where o.phone = :phone and o.used = false")
    int expireByPhone(@Param("phone") String phone);
}
