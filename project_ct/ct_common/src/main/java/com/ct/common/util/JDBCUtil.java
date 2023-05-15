package com.ct.common.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JDBCUtil {
    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306/ct?useUnicode=true&characterEncoding=utf-8";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "root";

    public static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(MYSQL_DRIVER_CLASS);
            conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return conn;
    }

    public static void main(String[] args) throws SQLException {
        Connection connection = getConnection();

        int year = 2023;
        int[] d = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

        String sql = "insert into ct_date(year, month, day) values(?, ?, ?)";

        PreparedStatement p = null;
        p = connection.prepareStatement(sql);
        p.setString(1, String.valueOf(year));
        p.setString(2, "");
        p.setString(3, "");
        p.executeUpdate();

        for (int month = 1; month <= 12; month++) {
            try {
                p = connection.prepareStatement(sql);
                p.setString(1, String.valueOf(year));
                p.setString(2, String.valueOf(month));
                p.setString(3, "");
                p.executeUpdate();

                for (int day = 1; day <= d[month]; day++) {
                    p = connection.prepareStatement(sql);
                    p.setString(1, String.valueOf(year));
                    p.setString(2, String.valueOf(month));
                    p.setString(3, String.valueOf(day));
                    p.executeUpdate();
                }

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
