package com.dafei.web.search;

import com.dafei.bean.Term;
import com.dafei.datasource.TermDao;
import com.dafei.tools.Formula;
import com.dafei.tools.HashFactory;
import com.dafei.tools.IHashFunction;
import com.dafei.tools.Utils;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.*;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-4-5
 * Time: ÏÂÎç4:28
 * To change this template use File | Settings | File Templates.
 */
public class SampleServlet extends HttpServlet {
    String tmpDir = "e:/metadata_raw/upload";
    String dstDir = "e:/metadata_raw/upload/file";
    String vector_file = "e:/metadata_raw/simhash.txt";
    Set<Term> termSet;
    int maxlen;
    Map<String, String> termHash;
    int th = 10;
    int threshold[] = {10, 17, 14};
    Map<String, String> result;
    Map<String, String> result1;

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {

    }


    public void run(String test_file, String vectorFile, Set<Term> set, int maxlen, Map<String, String> termHash)
            throws ServletException {
        result = new HashMap<String, String>();
        result1 = new HashMap<String, String>();
        String line = null;
        BufferedReader br = null;
        Map<String, Integer> test_file_map = Utils.countTermFrequent(test_file, set, maxlen);
        Map<Integer, int[]> test_int_map = Utils.createVector(set, termHash, test_file_map);
        Map<Integer, String> test_01vector = Utils.formatVector(test_int_map);
        try {
            br = new BufferedReader(new FileReader(vectorFile));
            while ((line = br.readLine()) != null) {
                String temp[] = line.split(",");
                String fileName = temp[0];
                String hash48 = temp[1];
                String hash64 = temp[2];
                String hash80 = temp[3];

                int dis48 = Utils.hammingDistance(test_01vector.get(48), hash48);
                int dis64 = Utils.hammingDistance(test_01vector.get(64), hash64);
                int dis80 = Utils.hammingDistance(test_01vector.get(80), hash80);

                // System.out.println("file: " + fileName + " , hamming distance is: <" + dis48+","+dis64+","+dis80+">");
                if (dis64 < th) {
                    // System.out.println("--------------------catch file: " + fileName);
                    result.put(fileName, String.valueOf(dis64));
                }
                if ((dis48 == 0) && (dis64 == 0) && (dis80 == 0)) {
                    result1.put(fileName, "<" + dis48 + "," + dis64 + "," + dis80 + ">");
                } else if (!((dis48 == 0) && (dis64 == 0) && (dis80 == 0)) && Formula.ellipsoid(dis48, dis64, dis80, threshold)) {
                    result1.put(fileName, "<" + dis48 + "," + dis64 + "," + dis80 + ">");
                }

            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public void initial() {
        TermDao tdao = new TermDao();
        try {
            //tdao.parseTermSet();
            tdao.parseTermFromHbase();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        termSet = TermDao.all_term_set;
        maxlen = TermDao.MAXLEN;
        IHashFunction hashFunction = HashFactory.createHashFunctions(HashFactory.HashType.SIMPLEGBK);
        termHash = hashFunction.termToHash(termSet);
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        System.out.println("servlet " + request.getContentType());
        // PrintWriter out = response.getWriter();

        DiskFileItemFactory fileItemFactory = new DiskFileItemFactory();
        fileItemFactory.setSizeThreshold(1 * 1024 * 1024);
        fileItemFactory.setRepository(new File(tmpDir));
        ServletFileUpload upHandler = new ServletFileUpload(fileItemFactory);
        HttpSession session = request.getSession();
        upHandler.setProgressListener(new UploadProgress(session));
        // now lets parse the request!!
        try {
            List items = upHandler.parseRequest(request);
            Iterator itr = items.iterator();
            while (itr.hasNext()) {
                File file = null;
                FileItem item = (FileItem) itr.next();
                System.out.println(item.getFieldName() + " " + item.getName());
                if (item.isFormField()) {
                    // out.println("File Name " + item.getFieldName() + ", value = " + item.getString());
                } else {
                    InputStream ins = item.getInputStream();
                    //  out.println("Field Item : " + item.getFieldName() + ", File Name : " + item.getName() + ", Content Type :" + item.getContentType() + ", File Size : " + item.getSize());
                    file = new File(dstDir, item.getName());
                    FileOutputStream fos = new FileOutputStream(file);
                    byte[] buffer = new byte[4096];
                    int count;
                    while ((count = ins.read(buffer)) != -1) {
                        fos.write(buffer, 0, count);
                    }
                    fos.flush();

                }
                //out.close();
                initial();
                if (file.exists()) {
                    run(dstDir + "/" + item.getName(), vector_file, termSet, maxlen, termHash);
                    request.setAttribute("result", result);
                    request.setAttribute("result1", result1);
                    RequestDispatcher dis = request.getRequestDispatcher("/index.jsp");
                    dis.forward(request, response);
                }
            }

        } catch (FileUploadException eo) {
            log("Error encountered while uploading!");
        } catch (Exception eo) {
            log("Error exception occured!", eo);
        } finally {
            //out.close();
        }
    }
}
