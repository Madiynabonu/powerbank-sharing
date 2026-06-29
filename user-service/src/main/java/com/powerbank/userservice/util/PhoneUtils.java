package com.powerbank.userservice.util;

public final class PhoneUtils {

    private PhoneUtils() {}

    public static String mask(String phone) {
        if (phone == null || phone.length() < 4) return "****";
        return phone.substring(0, phone.length() - 4) + "****";
    }
}
