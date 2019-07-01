package com.hivedemo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @Author: jiajunp
 * @Date: 2019/6/28 8:20
 * @Version 1.0
 */
public class App {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection("jdbc:hive2://192.168.100.20:10000/mydb");
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select exchanage ,symbol,price_open from stocks");
        while(rs.next()){
            System.out.println(rs.getString(1) + "," + rs.getString(2) + "," + rs.getInt(3)) ;
        }
        rs.close();
        st.close();
        conn.close();
    }
}
