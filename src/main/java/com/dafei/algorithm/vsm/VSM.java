package com.dafei.algorithm.vsm;

import com.dafei.bean.Term;
import com.dafei.datasource.TermDao;
import com.dafei.tools.CommonTool;
import com.dafei.tools.Formula;
import com.dafei.tools.Utils;

import java.io.*;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-30
 * Time: 5:02pm
 * To change this template use File | Settings | File Templates.
 */
public class VSM {
    private static final String under_test_dir = "E:\\metadata_raw\\test"; //under test directory
    private static final String document_set = "E:\\metadata_raw\\all_10G";    // document set directory
    private static final String docset_vector = "E:\\metadata_raw\\vsm_10G.txt";  // file to store text frequency
    private static float threshold = 0.1f;

    /**
     * pre process
     *
     * @param doc_set
     * @param vectorFile
     * @param set
     * @param maxlen
     */
    public void saveTextFreq2File(String doc_set, String vectorFile, Set<Term> set, int maxlen) {
        File freqFile = new File(vectorFile);
        if (freqFile.exists()) freqFile.delete();
        List<String> file_path_list = CommonTool.getFileList(doc_set);
        for (String file_path : file_path_list) {
            String fileName = CommonTool.extractFileName(file_path);
            Map<String, Integer> doc_map = Utils.countTermFrequent(file_path, set, maxlen);
            Utils.saveFreq(fileName, doc_map, freqFile);
        }
    }

    public void run(String test_file, String vectorFile, Set<Term> set, int maxlen, int run_times) {
        String line = null;
        BufferedReader br = null;
        Map<String, Integer> test_file_map = Utils.countTermFrequent(test_file, set, maxlen);
        try {
            br = new BufferedReader(new FileReader(vectorFile));
            while ((line = br.readLine()) != null) {
                Map<String, Integer> doc_map = new HashMap<String, Integer>();
                StringTokenizer st = new StringTokenizer(line, "\001");
                String fileName = st.nextToken();
                String temp[] = st.nextToken().split("\t");
                for (String s : temp) {
                    String item[] = s.split(",");
                    doc_map.put(item[0], Integer.valueOf(item[1]));
                }
                double dis = Formula.minus_pearson(test_file_map, doc_map, set.size());
                System.out.println("file: " + fileName + " , distance is: " + dis);
                if (dis < threshold) {
                    System.out.println("--------------------catch file: " + fileName);
                }
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static void main(String args[]) throws Exception {
        TermDao tdao = new TermDao();
        tdao.parseTermFromHbase();
        Set<Term> termSet = TermDao.all_term_set;
        int maxlen = TermDao.MAXLEN;
        String test_file = CommonTool.getFileList(under_test_dir).get(0);

        VSM vsm = new VSM();
        vsm.saveTextFreq2File(document_set, docset_vector, termSet, maxlen);

//        int run_times = (args.length > 0) ? Integer.parseInt(args[0]) : 1;
//        File f = new File("e:\\metadata_raw\\vsm_single.txt");
//        if (f.exists()) f.delete();
//        BufferedWriter bw = new BufferedWriter(new FileWriter(f, true));
//        for (int i = 0; i < run_times; i++) {
//            long start = System.currentTimeMillis();
//            vsm.run(test_file, docset_vector, termSet, maxlen, i);
//            long end = System.currentTimeMillis();
//            bw.write((i + 1) + "\t" + String.valueOf((end - start) / 1000.0f));
//            bw.newLine();
//        }
//        bw.close();


    }
}
