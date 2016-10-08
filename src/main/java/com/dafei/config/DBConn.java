package com.dafei.config;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 10-12-20
 * Time: 1:50pm
 * To change this template use File | Settings | File Templates.
 */

import java.io.UnsupportedEncodingException;
import java.sql.*;

public class DBConn {

    private Connection conn;
    private Statement stmt;
    private ResultSet rs;
    private PreparedStatement prepstmt;
    private static DBConn instance;

    public static synchronized DBConn getInstance() {
        if (instance == null)
            instance = new DBConn();
        return instance;
    }

    public Connection getConn() {
        conn = ConnectionFactory.getInstance().getConnection();
        return conn;
    }

    public int openDB() throws Exception {

        try {
            conn = getConn();
            stmt = conn.createStatement();
            return 1;
        } catch (Exception exception) {
            System.out.println((new StringBuilder()).append("naming:").append(
                    exception.getMessage()).toString());
        }
        return -1;
    }

    public void closeDB() {
        try {
            if (rs != null) {
                rs.close();
                rs = null;
            }
            if (stmt != null) {
                stmt.close();
                stmt = null;
            }
            if (prepstmt != null) {
                prepstmt.close();
                prepstmt = null;
            }
            if (conn != null) {
                conn.close();
                conn = null;
            }
        } catch (SQLException sqlexception) {
        }
    }

    public ResultSet doQuery(String s) {
        try {
            rs = stmt.executeQuery(s);
            return rs;
        } catch (SQLException sqlexception) {
            sqlexception.printStackTrace();
        }
        return null;
    }

    public int doUpdate(String s) {
        try {
            return stmt.executeUpdate(s);

        } catch (SQLException sqlexception) {
            return -1;
        }
    }

    public void prepareStatement(String s) throws SQLException {
        prepstmt = conn.prepareStatement(s, 1004, 1007);
    }

    public void setObject(int j, Object s) throws SQLException {
        prepstmt.setObject(j, s);
    }

    public void setString(int j, String s) throws SQLException {
        prepstmt.setString(j, s);
    }

    public void setInt(int j, int k) throws SQLException {
        prepstmt.setInt(j, k);
    }

    public void setBoolean(int j, boolean flag) throws SQLException {
        prepstmt.setBoolean(j, flag);
    }

    public void setDate(int j, Date date) throws SQLException {
        prepstmt.setDate(j, date);
    }

    public void setNull(int j, int i) throws SQLException {
        prepstmt.setNull(j, i);
    }

    public void setLong(int j, long l) throws SQLException {
        prepstmt.setLong(j, l);
    }

    public void setFloat(int j, float f) throws SQLException {
        prepstmt.setFloat(j, f);
    }

    public void setBytes(int j, byte abyte0[]) throws SQLException {
        prepstmt.setBytes(j, abyte0);
    }

    public ResultSet doQuery() {
        try {
            if (prepstmt != null)
                return prepstmt.executeQuery();
        } catch (SQLException sqlexception) {
            return null;
        }
        return null;
    }

    public int doUpdate() {
        try {
            if (prepstmt != null)
                prepstmt.executeUpdate();
            return 1;
        } catch (SQLException sqlexception) {
            sqlexception.printStackTrace();
            return -1;
        }
    }

    public boolean supportsTransactions() {
        boolean flag = false;
        try {
            flag = conn.getMetaData().supportsTransactions();
        } catch (SQLException sqlexception) {
        }
        return flag;
    }

    public void setAutoCommit(boolean flag) {
        try {
            conn.setAutoCommit(flag);
        } catch (SQLException sqlexception) {
        }
    }

    public void rollBack() {
        try {
            conn.rollback();
        } catch (SQLException sqlexception) {
        }
    }

    public void commit() {
        try {
            conn.commit();
        } catch (SQLException sqlexception) {
        }
    }

    public static String getstr(String s) {
        try {
            String s1 = s;
            byte abyte0[] = s1.getBytes("ISO8859-1");
            String s2 = new String(abyte0);
            return s2;
        } catch (UnsupportedEncodingException unsupportedencodingexception) {
            System.out.println(unsupportedencodingexception);
        }
        return "";
    }


    public PreparedStatement getPrepstmt() {
        return prepstmt;
    }

    public void setPrepstmt(PreparedStatement prepstmt) {
        this.prepstmt = prepstmt;
    }


}
