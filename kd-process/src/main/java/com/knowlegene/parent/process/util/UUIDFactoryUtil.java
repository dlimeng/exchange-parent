package com.knowlegene.parent.process.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.UUID;

/**
 * uuid
 * @Author: limeng
 * @Date: 2019/8/13 17:37
 */
public class UUIDFactoryUtil {
    public UUIDFactoryUtil() {
    }

    public static String getUUIDStr() {
        UUID uuid = UUID.randomUUID();
        String s = uuid.toString();
        s = s.replaceAll("[-]", "");
        return s.toUpperCase();
    }

    public static String getNumberUUIDStr() {
        Calendar c = Calendar.getInstance();
        Calendar date = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            date.setTime(sdf.parse("2013-01-01"));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Long seconds = c.getTimeInMillis() - date.getTimeInMillis();
        return seconds + "";
    }

}
