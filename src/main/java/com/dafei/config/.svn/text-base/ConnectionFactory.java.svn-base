package com.dafei.config;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 10-12-20
 * Time: 1:41pm
 * To change this template use File | Settings | File Templates.
 */

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class ConnectionFactory {
    private static ConnectionFactory instance;
    private static String configFilePath = "com/dafei/config/db.properties";
    private Properties properties;

    private ConnectionFactory() {
        try {
            File file;
            file = new File(this.getClass().getClassLoader().getResource(configFilePath).toURI());
            //file = new File("/home/xjtudlc/dafei/config.properties");
            properties = new Properties();
            properties.load(new FileInputStream(file));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static synchronized ConnectionFactory getInstance() {
        if (instance == null)
            instance = new ConnectionFactory();
        return instance;
    }

    public Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(properties.getProperty("driver"));
            conn = DriverManager.getConnection(
                    properties.getProperty("connectionURL"),
                    properties.getProperty("user"), properties.getProperty("password"));

        } catch (Exception exception) {
            System.out.println((new StringBuilder()).append("naming:").append(
                    exception.getMessage()).toString());
        }
        return conn;
    }

    public String getDefaultConfig() {
        return properties.getProperty("hbase-default");
    }

    public String getSiteConfig() {
        return properties.getProperty("hbase-site");
    }
}