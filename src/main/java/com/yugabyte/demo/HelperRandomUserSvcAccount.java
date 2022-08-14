package com.yugabyte.demo;

import java.sql.Timestamp;
import java.util.Calendar;

class HelperRandomUserSvcAccount {

    public static Integer getAccountId(int accountId) { return accountId; }

    public static String getUserId(int userProfileId) { return String.format("%08d", userProfileId); }

    public static Timestamp getCreateTime(int userProfileId) {
        //Return a time within one year from the start time (2020-01-01 00:00:00.000)
        Calendar cal = Calendar.getInstance();
        cal.set(2020, 0, 1, 0, 0, 0);
        cal.add(Calendar.DAY_OF_YEAR, userProfileId % 365);
        cal.add(Calendar.SECOND, userProfileId % (24 * 3600));
        Timestamp createTime = new Timestamp(cal.getTime().getTime());
        return createTime;
    }

    public static Timestamp getLastAccessTime(int userProfileId) {
        //Return a time within 180 days of the createTime (2022-08-01 00:00:00.000)
        Calendar cal = Calendar.getInstance();
        cal.set(2022, 7, 1, 0, 0, 0);
        cal.add(Calendar.DAY_OF_YEAR, userProfileId % 180 * -1);
        cal.add(Calendar.SECOND, userProfileId % (24 * 3600));
        Timestamp lastAccessTime = new Timestamp(cal.getTime().getTime());
        return lastAccessTime;
    }

    public static String getUserVerified(int userProfileId) {
        return userProfileId % 1000 != 0 ? "Y" : "N";
    }

    public static String getSvcName(int accountId) { return "Service " + accountId; }

    public static String getSvcDesription(int accountId) { return "Service description " + accountId; }

}
