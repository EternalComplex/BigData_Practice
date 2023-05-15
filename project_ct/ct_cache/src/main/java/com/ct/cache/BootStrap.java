package com.ct.cache;

import com.ct.common.util.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 启动缓存客户端, 向redis中增加缓存数据
 */
public class BootStrap {
    public static void main(String[] args) {
        // TODO 读取mysql中的数据
        Connection conn = null;
        PreparedStatement pstat = null;
        ResultSet rs = null;

        // 读取用户，时间数据
        Map<String, Integer> userMap = new HashMap<>();
        Map<String, Integer> dateMap = new HashMap<>();

        try {
            conn = JDBCUtil.getConnection();

            String queryUserSql = "select id, tel from ct_user";
            pstat = conn.prepareStatement(queryUserSql);
            rs = pstat.executeQuery();
            while (rs.next()) {
                Integer id = rs.getInt(1);
                String tel = rs.getString(2);
                userMap.put(tel, id);
            }

            String queryDateSql = "select id, year, month, day from ct_date";
            pstat = conn.prepareStatement(queryDateSql);
            rs = pstat.executeQuery();
            while (rs.next()) {
                Integer id = rs.getInt(1);
                String year = rs.getString(2);
                String month = rs.getString(3);
                if (month.length() == 1) month = "0" + month;
                String day = rs.getString(4);
                if (day.length() == 1) day = "0" + day;
                dateMap.put(year + month + day, id);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (pstat != null) {
                try {
                    pstat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

//        System.out.println(userMap.size());
//        System.out.println(dateMap.size());

        // TODO 向redis中存储数据
        Jedis jedis = new Jedis("192.168.10.199", 6379);
        jedis.auth("redis");

        for (String key : userMap.keySet()) {
            Integer value = userMap.get(key);
            jedis.hset("ct_user", key, "" + value);
        }

        for (String key : dateMap.keySet()) {
            Integer value = dateMap.get(key);
            jedis.hset("ct_date", key, "" + value);
        }
    }
}
