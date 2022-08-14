package com.yugabyte.demo;

import com.yugabyte.ysql.ClusterAwareLoadBalancer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.*;
import java.sql.*;
import java.util.*;

public class IAMDataGenerator
{
    protected static HikariDataSource hikariDataSource;
    protected static Properties properties;
    private static HelperRandomUserProfile helperRandomUserProfile;
    private static HelperRandomUserCredentials helperRandomUserCredentials;
    private static HelperRandomUserSvcAccount helperRandomUserSvcAccount;
    private static HelperRandomUserAudit helperRandomUserAudit;

    public static void main( String[] args ) {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        System.out.println(timestamp);

        String configFile = "";
        if (args.length >= 1) configFile = args[0];
        loadConfig(configFile);
        if (properties == null) {
            System.out.println("Failed to load properties!");
            return;
        }

        if (Integer.parseInt(properties.getProperty("NumberOfUsers")) >= 100000000) {
            System.out.println("The application can generate up to 99,999,999 users.");
            return;
        }

        helperRandomUserProfile = new HelperRandomUserProfile();
        helperRandomUserCredentials = new HelperRandomUserCredentials();
        helperRandomUserSvcAccount = new HelperRandomUserSvcAccount();
        helperRandomUserAudit = new HelperRandomUserAudit();

        getHikariDataSource();
        if (hikariDataSource == null) {
            System.out.println("Failed to create HikariDataSource!");
            return;
        }

        try {
            generateDataConcurrent();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            ex.printStackTrace();
        }

        timestamp = new Timestamp(System.currentTimeMillis());
        System.out.println(timestamp);
    }

    private static void loadConfig(String configFile) {
        properties = new Properties();

        if (!configFile.isEmpty() && new File(configFile).exists()) {
            try (InputStream input = new FileInputStream(configFile)) {
                properties.load(input);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        //Overwrite configurations with environments
        if (System.getenv("IAMDG_YB_HOST") != null) {
            properties.setProperty("Host", System.getenv("IAMDG_YB_HOST"));
        }

        if (System.getenv("IAMDG_YB_PORT") != null) {
            properties.setProperty("Port", System.getenv("IAMDG_YB_PORT"));
        }

        if (System.getenv("IAMDG_YB_USER") != null) {
            properties.setProperty("User", System.getenv("IAMDG_YB_USER"));
        }

        if (System.getenv("IAMDG_YB_PWD") != null) {
            properties.setProperty("Password", System.getenv("IAMDG_YB_PWD"));
        }

        if (System.getenv("IAMDG_YB_DB") != null) {
            properties.setProperty("Database", System.getenv("IAMDG_YB_DB"));
        }

        if (System.getenv("IAMDG_YB_AE") != null) {
            properties.setProperty("AdditionalEndpoints", System.getenv("IAMDG_YB_AE"));
        }

        if (System.getenv("IAMDG_CP_SIZE") != null) {
            properties.setProperty("ConnectionPoolSize", System.getenv("IAMDG_CP_SIZE"));
        }

        if (System.getenv("IAMDG_THEADS") != null) {
            properties.setProperty("NumberOfThreads", System.getenv("IAMDG_THEADS"));
        }


        if (System.getenv("IAMDG_NO_OF_USERS") != null) {
            properties.setProperty("NumberOfUsers", System.getenv("IAMDG_NO_OF_USERS"));
        }

        if (System.getenv("IAMDG_USERS_PER_BATCH") != null) {
            properties.setProperty("NumberOfUsersPerBatch", System.getenv("IAMDG_USERS_PER_BATCH"));
        }

        if (System.getenv("IAMDG_SVC_PER_USER") != null) {
            properties.setProperty("NumberOfServicesPerUser", System.getenv("IAMDG_SVC_PER_USER"));
        }

        if (System.getenv("IAMDG_ACT_PER_USER") != null) {
            properties.setProperty("NumberOfActivitiesPerUser", System.getenv("IAMDG_ACT_PER_USER"));
        }
    }

    private static void getHikariDataSource() {
        try {
            String host = properties.getProperty("Host", "127.0.0.1");
            String port = properties.getProperty("Port", "5433");
            String user = properties.getProperty("User", "yugabyte");
            String password = properties.getProperty("Password", "");
            String database = properties.getProperty("Database", "iam");
//            String enableLoadBalancer = properties.getProperty("EnableLoadBalancer", "true");
            String additionalEndpoints = properties.getProperty("AdditionalEndpoints", "");
            String connPoolSize = properties.getProperty("ConnectionPoolSize", "6");

            //This is just for demo purpose because right now default time for refresh is 5min
            //and we don't want the user to wait that much in this app
            ClusterAwareLoadBalancer.forceRefresh = true;

            Properties poolProperties = new Properties();
            poolProperties.setProperty("poolName", "data-generator-pool");
            poolProperties.setProperty("dataSourceClassName", "com.yugabyte.ysql.YBClusterAwareDataSource");
            poolProperties.setProperty("maximumPoolSize", connPoolSize);
            poolProperties.setProperty("connectionTimeout", "1000000");
            poolProperties.setProperty("autoCommit", "true");
            poolProperties.setProperty("dataSource.serverName", host);
            poolProperties.setProperty("dataSource.portNumber", port);
            poolProperties.setProperty("dataSource.databaseName", database);
            poolProperties.setProperty("dataSource.user", user);
            poolProperties.setProperty("dataSource.password", password);
//            poolProperties.setProperty("dataSource.loadBalance", enableLoadBalancer);
            poolProperties.setProperty("dataSource.additionalEndpoints", additionalEndpoints);

            HikariConfig hikariConfig = new HikariConfig(poolProperties);
            hikariConfig.validate();
            hikariDataSource = new HikariDataSource(hikariConfig);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void generateDataConcurrent() throws InterruptedException {
        int numberOfThreads = Integer.parseInt(properties.getProperty("NumberOfThreads", "10"));
        int numberOfUsers = Integer.parseInt(properties.getProperty("NumberOfUsers", "1000"));

        Thread[] threads = new Thread[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            int startUserProfileId = 1000000 + numberOfUsers / numberOfThreads * i;
            int endUserProfileId = 1000000 + numberOfUsers / numberOfThreads * (i + 1);
            threads[i] = new Thread(new IAMDataGenerator.ConcurrentGeneratorClass(startUserProfileId, endUserProfileId));
        }

        for (int i = 0; i < numberOfThreads; i++) {
            threads[i].start();
        }

        for (int i = 0; i < numberOfThreads; i++) {
            threads[i].join();
        }
    }

    static class ConcurrentGeneratorClass implements Runnable {
        private int startUserProfileId;
        private int endUserProfileId;

        public ConcurrentGeneratorClass (int startUserProfileId, int endUserProfileId){
            this.startUserProfileId = startUserProfileId;
            this.endUserProfileId = endUserProfileId;
        }

        @Override
        public void run() {
            try (Connection connection = hikariDataSource.getConnection()) {
                generateData(connection, startUserProfileId, endUserProfileId);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    protected static void generateData(Connection connection, int startUserProfileId, int endUserProfileId) {
        int numberOfUsersPerBatch = Integer.parseInt(properties.getProperty("NumberOfUsersPerBatch", "100"));
        int svcPerUser = Integer.parseInt(properties.getProperty("NumberOfServicesPerUser", "3"));
        int activitiesPerUser = Integer.parseInt(properties.getProperty("NumberOfActivitiesPerUser", "10"));

        String sqlUserProfile = "INSERT INTO public.user_profile"
                + " (id, first_name, last_name, gender, birthday, mobile_no, email, email_verified, create_time, last_access_time, salutation)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
        String sqlUserCredentials = "INSERT INTO public.user_credentials"
                + " (user_id, password_hash, profile_id)"
                + " VALUES (?, ?, ?);";
        String sqlUserSvcAccount ="INSERT INTO public.user_svc_account"
                + " (account_id, user_id, create_time, last_access_time, user_verified, svc_name, svc_description)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?);";
        String sqlUserAudit = "INSERT INTO public.user_audit"
                + " (account_id, user_id, action, description, transaction_time, device, client_ip, location)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?);";

        try (PreparedStatement stmtUserProfile = connection.prepareStatement(sqlUserProfile);
             PreparedStatement stmtUserCredential = connection.prepareStatement(sqlUserCredentials);
             PreparedStatement stmtUserSvcAccount = connection.prepareStatement(sqlUserSvcAccount);
             PreparedStatement stmtUserAudit = connection.prepareStatement(sqlUserAudit)) {

            connection.setAutoCommit(false);

            for (int currentUserProfileId = startUserProfileId; currentUserProfileId < endUserProfileId; currentUserProfileId++) {
                stmtUserProfile.setInt(1, currentUserProfileId);
                stmtUserProfile.setString(2, helperRandomUserProfile.getFirstName(currentUserProfileId));
                stmtUserProfile.setString(3, helperRandomUserProfile.getLastName(currentUserProfileId));
                stmtUserProfile.setString(4, helperRandomUserProfile.getGender(currentUserProfileId));
                stmtUserProfile.setDate(5, helperRandomUserProfile.getBirthday(currentUserProfileId));
                stmtUserProfile.setString(6, helperRandomUserProfile.getMobileNo(currentUserProfileId));
                stmtUserProfile.setString(7, helperRandomUserProfile.getEmail(currentUserProfileId));
                stmtUserProfile.setString(8, helperRandomUserProfile.getEmailVerified(currentUserProfileId));
                stmtUserProfile.setTimestamp(9, helperRandomUserProfile.getCreateTime(currentUserProfileId));
                stmtUserProfile.setTimestamp(10, helperRandomUserProfile.getLastAccessTime(currentUserProfileId));
                stmtUserProfile.setString(11, helperRandomUserProfile.getSalutation(currentUserProfileId));
                stmtUserProfile.executeUpdate();

                stmtUserCredential.setString(1, helperRandomUserCredentials.getUserId(currentUserProfileId));
                stmtUserCredential.setString(2, helperRandomUserCredentials.getPasswordHash(currentUserProfileId));
                stmtUserCredential.setInt(3, currentUserProfileId);
                stmtUserCredential.executeUpdate();

                for (int accountId = 1; accountId <= svcPerUser; accountId++ ) {
                    stmtUserSvcAccount.setInt(1, accountId);
                    stmtUserSvcAccount.setString(2, helperRandomUserCredentials.getUserId(currentUserProfileId));
                    stmtUserSvcAccount.setTimestamp(3, helperRandomUserSvcAccount.getCreateTime(currentUserProfileId));
                    stmtUserSvcAccount.setTimestamp(4, helperRandomUserSvcAccount.getLastAccessTime(currentUserProfileId));
                    stmtUserSvcAccount.setString(5, helperRandomUserSvcAccount.getUserVerified(currentUserProfileId));
                    stmtUserSvcAccount.setString(6, helperRandomUserSvcAccount.getSvcName(accountId));
                    stmtUserSvcAccount.setString(7, helperRandomUserSvcAccount.getSvcDesription(accountId));
                    stmtUserSvcAccount.executeUpdate();
                }

                for (int activityId = 1; activityId <= activitiesPerUser; activityId++) {
                    stmtUserAudit.setInt(1, activityId % svcPerUser + 1);
                    stmtUserAudit.setString(2, helperRandomUserCredentials.getUserId(currentUserProfileId));
                    stmtUserAudit.setString(3, helperRandomUserAudit.getAction(currentUserProfileId));
                    stmtUserAudit.setString(4, helperRandomUserAudit.getDescription(currentUserProfileId));
                    stmtUserAudit.setTimestamp(5, helperRandomUserAudit.getTransactionTime(currentUserProfileId, activityId));
                    stmtUserAudit.setString(6, helperRandomUserAudit.getDevice(currentUserProfileId));
                    stmtUserAudit.setString(7, helperRandomUserAudit.getClientIp(currentUserProfileId));
                    stmtUserAudit.setString(8, helperRandomUserAudit.getLocation(currentUserProfileId));
                    stmtUserAudit.executeUpdate();
                }

                if (((currentUserProfileId - startUserProfileId + 1) % numberOfUsersPerBatch == 0) || (currentUserProfileId + 1 == endUserProfileId)) {
                    connection.commit();
                }
            }
            connection.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
