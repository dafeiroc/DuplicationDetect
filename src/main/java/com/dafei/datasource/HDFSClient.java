package com.dafei.datasource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 10-12-20
 * Time: 9:55am
 * To change this template use File | Settings | File Templates.
 */
public class HDFSClient {
    public final String serveraddr = "hdfs://job-tracker";

    public boolean exists(String filepath, String suffix) throws IOException {
        Configuration conf = new Configuration();
        String fullpath = serveraddr + filepath + suffix;
        FileSystem fs = FileSystem.get(URI.create(fullpath), conf);
        return fs.exists(new Path(fullpath));
    }

    public void down(String filepath, String suffix, String dest_dir) throws IOException {
        Configuration conf = new Configuration();
        String fullpath = serveraddr + filepath + suffix;
        FileSystem fs = FileSystem.get(URI.create(fullpath), conf);
        FSDataInputStream dis = fs.open(new Path(fullpath));
        int pos = filepath.lastIndexOf("/");
        String filename = filepath.substring(pos + 1);
        String downloadpath = dest_dir + filename + suffix;
        FileOutputStream fos = new FileOutputStream(downloadpath);
        IOUtils.copyBytes(dis, fos, 4096, false);
        dis.close();
        fos.close();

    }

    public static void main(String args[]) throws IOException {
        HDFSClient hdfsc = new HDFSClient();
        //  System.out.println(hdfsc.exists("/edu_source/former/数据结构/DOC/第一章 绪论", ".doc"));
        System.out.println(hdfsc.exists("/edu_source/former/数据结构/DOC/09查找1", ".doc"));
    }
}
