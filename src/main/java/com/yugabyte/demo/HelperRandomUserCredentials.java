package com.yugabyte.demo;

import java.math.BigInteger;
import java.security.MessageDigest;

class HelperRandomUserCredentials {
    public static String getUserId(int userProfileId) { return String.format("%08d", userProfileId); }

    public static String getPasswordHash(int userProfileId) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(String.format("%08d", userProfileId).getBytes());
            BigInteger no = new BigInteger(1, messageDigest);
            String hashtext = no.toString(16);

            while (hashtext.length() < 32) {
                hashtext = "0" + hashtext;
            }

            return hashtext;
        } catch (Exception ex) {
            ex.printStackTrace();
            return "Failed to generate MD5 hash.";
        }
    }
}
