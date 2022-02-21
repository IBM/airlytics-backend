package com.ibm.airlytics.utilities.validationService.queryservice.mySql;

import java.sql.*;

public class MySqlSampleLoader {

    public static void main(String[] args) {
        try {
            new MySqlSampleLoader().loadSampleData();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
    public void loadSampleData() throws SQLException {
        Connection con = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/analytics", "root", "1234");
        Statement stmt = con.createStatement();
        String sql = "CREATE DATABASE ANALYTICS1";
        stmt.executeUpdate(sql);
//        ResultSet rs = stmt.executeQuery("select * from emp");
//        while (rs.next())
//            System.out.println(rs.getInt(1) + "  " + rs.getString(2) + "  " + rs.getString(3));
//        con.close();
    }
}
