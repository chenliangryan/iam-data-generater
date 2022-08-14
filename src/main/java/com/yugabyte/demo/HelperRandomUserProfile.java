package com.yugabyte.demo;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

class HelperRandomUserProfile {
    static ArrayList<String> firstNames;
    static ArrayList<String> lastNames;
    static Date startDate = Date.valueOf("1940-01-01");
    static Calendar startTime1;

    public HelperRandomUserProfile() {
        InputStream fileFirstNames = getClass().getClassLoader().getResourceAsStream("firstnames.txt");
        InputStream fileLastNames = getClass().getClassLoader().getResourceAsStream("lastnames.txt");
        firstNames = new ArrayList<String>();
        lastNames = new ArrayList<String>();

        try (BufferedReader bf = new BufferedReader(new InputStreamReader(fileFirstNames, StandardCharsets.UTF_8))) {
            String name;
            while ((name = bf.readLine()) != null) {
                firstNames.add(name);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        try (BufferedReader bf = new BufferedReader(new InputStreamReader(fileLastNames, StandardCharsets.UTF_8))) {
            String name = bf.readLine();
            while (name != null) {
                lastNames.add(name);
                name = bf.readLine();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static String getFirstName(int userProfileId) {
        String firstName = firstNames.get(userProfileId % firstNames.size());
        return String.format(firstName);
    }

    public static String getLastName(int userProfileId) {
        String lastName = lastNames.get(userProfileId % lastNames.size());
        return String.format(lastName);
    }

    public static String getGender(int userProfileId) {
        return userProfileId % 2 == 0 ? "M" : "F";
    }

    public static Date getBirthday(int userProfileId) {
        //Return a date in the past 100 years of 2010-12-31
        Calendar cal = Calendar.getInstance();
        cal.set(2010, 11, 31);
        cal.add(Calendar.DAY_OF_YEAR, userProfileId % 36500 * -1);
        Date birthday = new Date(cal.getTime().getTime());
        return birthday;
    }

    public static String getMobileNo(int userProfileId) {
        String mobile = (userProfileId % 2 == 0 ? "8" : "9") + String.format("%07d", new Random().nextInt(9999999));
        return mobile;
    }

    public static String getEmail(int userProfileId) {
        String firstName = firstNames.get(userProfileId % firstNames.size());
        String lastName = lastNames.get(userProfileId % lastNames.size());
        String email = firstName.toLowerCase() + "." + lastName.toLowerCase() + "@gmail.com";
        return email;
    }

    public static String getEmailVerified(int userProfileId) {
        return userProfileId % 10000 != 0 ? "Y" : "N";
    }

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

    public static String getSalutation(int userProfileId) {
        return userProfileId % 2 == 0 ? "Mr." : "Ms.";
    }
}
