package com.dafei.web.search;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by IntelliJ IDEA.
 * User: Administrator
 * Date: 11-4-7
 * Time: ÏÂÎç7:53
 * To change this template use File | Settings | File Templates.
 */
public class ReadStatus extends HttpServlet {
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        response.setContentType("text/html");
        response.addHeader("pragma", "no-cache");
        response.addHeader("cache-control", "no-cache");
        response.addHeader("expires", "0");
        PrintWriter out = response.getWriter();

        HttpSession session = request.getSession();

        out.print(session.getAttribute("status"));
        System.out.println(session.getAttribute("status"));

        out.flush();
        out.close();
    }

}
