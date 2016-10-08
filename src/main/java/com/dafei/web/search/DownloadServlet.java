package com.dafei.web.search;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-4-19
 * Time: ÉÏÎç10:25
 * To change this template use File | Settings | File Templates.
 */
public class DownloadServlet extends HttpServlet {
    public final static String base_uri = "hdfs://job-tracker:54310/user/xjtudlc/dafei/all/";
    public final static String contentType = "application/octet-stream";

    public boolean exist(String fileName) throws IOException {
        Configuration conf = new Configuration();
        String fullpath = base_uri + fileName;
        FileSystem fs = FileSystem.get(URI.create(fullpath), conf);
        return fs.exists(new Path(fullpath));
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        //RFC2183 :
        //Content-Disposition: attachment; filename*="utf8''%E4%B8%AD%E6%96%87%20%E6%96%87%E4%BB%B6%E5%90%8D.txt"
        String encoded_fileName = request.getParameter("file");
        String fileName = new String(java.net.URLDecoder.decode(encoded_fileName, "gbk").getBytes("iso8859-1"));
        System.out.println("file name is: " + fileName);
        String user_agent = request.getHeader("User-Agent");
        response.setHeader("Content-Disposition", "inline;filename*=\"gbk\'\'" + encoded_fileName + "\"");


        if (exist(fileName)) {
            response.setContentType(contentType);

            ServletOutputStream sos = response.getOutputStream();
            Configuration conf = new Configuration();
            String fullpath = base_uri + fileName;
            FileSystem fs = FileSystem.get(URI.create(fullpath), conf);
            FSDataInputStream dis = fs.open(new Path(fullpath));
            IOUtils.copyBytes(dis, sos, 4096, true);
            dis.close();
            sos.flush();
            sos.close();

        } else {
            response.setContentType("text/plain");
            PrintWriter pw = response.getWriter();
            pw.println("404");
            pw.close();
        }
    }
}
