package com.yugabyte.demo;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Random;

class HelperRandomUserAudit {
    static String[] deviceOptions = {"Mobile", "Tablet", "Desktop"};
    static String[] countryOptions = {"Brunei", "Cambodia", "Indonesia", "Laos", "Malaysia", "Myanmar", "Philippines", "Singapore", "Thailand", "Vietnam"};

    public static String getAction(int userProfileId) {
        return "login";
    }

    public static String getDescription(int userProfileId) { return "Login activity"; }

    public static Timestamp getTransactionTime(int userProfileId, int SeqNo) {
        //Return a time within one year from the start time (2020-01-01 00:00:00.000)
        Calendar cal = Calendar.getInstance();
        cal.set(2020, 0, 1, 0, 0, 0);
        cal.add(Calendar.DAY_OF_YEAR, userProfileId % 365);
        cal.add(Calendar.DAY_OF_YEAR, new Random().nextInt(SeqNo * 10));
        cal.add(Calendar.SECOND, userProfileId % (24 * 3600));
        Timestamp transactionTime = new Timestamp(cal.getTime().getTime());
        return transactionTime;
    }

    public static String getDevice(int userProfileId) {
        return deviceOptions[userProfileId % 3];
    }

    public static String getClientIp(int userProfileId) {
        Random random = new Random();
        String clientIp = random.nextInt(256) + "."
                        + random.nextInt(256) + "."
                        + random.nextInt(256) + "."
                        + random.nextInt(256);
        return clientIp;
    }

    public static String getLocation(int userProfileId) {
        return countryOptions[new Random().nextInt(10)];
    }
}
