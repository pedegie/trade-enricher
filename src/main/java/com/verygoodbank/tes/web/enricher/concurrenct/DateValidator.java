package com.verygoodbank.tes.web.enricher.concurrenct;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class DateValidator {

    /**
     * Checks if the provided date is in the following format: yyyyMMdd
     *
     * @param dateStr text date representation
     * @return true if valid or false otherwise
     */
    public static boolean validate(ByteBuffer dateStr) {
        if (dateStr.remaining() < 8) {
            return false;
        }

        var from = dateStr.position();
        int year = parseInt(dateStr, from, 4);
        int month = parseInt(dateStr, from + 4, 2);
        int day = parseInt(dateStr, from + 4 + 2, 2);

        if (month < 1 || month > 12) {
            return false;
        }
        if (day < 1 || day > 31) {
            return false;
        }

        if (month == 2) {
            if (isLeapYear(year)) {
                return day <= 29;
            } else {
                return day <= 28;
            }
        }

        if (month == 4 || month == 6 || month == 9 || month == 11) {
            return day <= 30;
        }

        return true;
    }

    private static int parseInt(ByteBuffer buffer, int from, int end) {
        int value = 0;
        for (int i = from; i < from + end; i++) {
            char c = (char) buffer.get(i);
            if (!Character.isDigit(c)) {
                return -1;
            }
            value = value * 10 + (c - '0');
        }
        return value;
    }

    private static boolean isLeapYear(int year) {
        if (year % 4 == 0) {
            if (year % 100 == 0) {
                return year % 400 == 0;
            } else {
                return true;
            }
        }
        return false;
    }
}