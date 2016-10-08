package com.dafei.retargeting.servlet;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-4-18
 * Time: ÏÂÎç5:53
 * To change this template use File | Settings | File Templates.
 */
public class Retarget extends HttpServlet {
    String result[] = new String[31];

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {

        String select_id = request.getParameter("selected_id");
        int dataset_id = Integer.parseInt(request.getParameter("dataset_id"));
        System.out.println(select_id);
        result[dataset_id] = select_id;
        dataset_id++;
        if (dataset_id >= 31) {
            response.setHeader("Content-Disposition", "attachment;filename=result.xls");
            response.setContentType("application/vnd.ms-excel");
            PrintWriter pw = response.getWriter();
            pw.println("times\timage");
            for (int i = 1; i < result.length; i++)
                pw.println(i + "\t" + result[i]);
            pw.close();
        } else {
            request.setAttribute("dataset_id", String.valueOf(dataset_id));
            RequestDispatcher dis = request.getRequestDispatcher("/retarget.jsp");
            dis.forward(request, response);
        }
    }
}
