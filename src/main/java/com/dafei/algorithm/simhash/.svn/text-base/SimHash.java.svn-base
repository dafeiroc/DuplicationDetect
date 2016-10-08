package com.dafei.algorithm.simhash;

import com.dafei.bean.Term;
import com.dafei.datasource.TermDao;
import com.dafei.tools.CommonTool;
import com.dafei.tools.HashFactory;
import com.dafei.tools.IHashFunction;
import com.dafei.tools.Utils;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * every file should less than 2GB
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-10
 * Time: 10:52pm
 * To change this template use File | Settings | File Templates.
 */
public class SimHash {
    private static final String under_test_dir = "E:\\metadata_raw\\test"; //under test directory
    private static final String document_set = "E:\\metadata_raw\\all";    // document set directory
    private static final String docset_vector = "E:\\metadata_raw\\simhash.txt";  // file to store normalization vector
    private IHashFunction hashFunction = HashFactory.createHashFunctions(HashFactory.HashType.SIMPLEGBK);
    private Map<String, String> hm_termToHash;
    private int threshold[] = {10, 17, 14};
    private int th = 10;

    /**
     * pre process
     *
     * @param doc_set
     * @param vectorFile
     * @param set
     * @param maxlen
     * @param termHash
     */
    public void saveHash2File(String doc_set, String vectorFile, Set<Term> set, int maxlen, Map<String, String> termHash) {
        File hashFile = new File(vectorFile);
        if (hashFile.exists()) hashFile.delete();
        List<String> file_path_list = CommonTool.getFileList(doc_set);
        for (String file_path : file_path_list) {
            String fileName = CommonTool.extractFileName(file_path);
            Map<String, Integer> doc_map = Utils.countTermFrequent(file_path, set, maxlen);
            Map<Integer, int[]> doc_int_map = Utils.createVector(set, termHash, doc_map);
            Map<Integer, String> doc_01vector = Utils.formatVector(doc_int_map);
            Utils.storeVectorToFile(doc_01vector, fileName, hashFile);
        }
    }

    public void run(String test_file, String vectorFile, Set<Term> set, int maxlen, Map<String, String> termHash, int run_times) {

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
                    System.out.println("file: " + fileName + " , hamming distance is: <" + dis48 + "," + dis64 + "," + dis80 + ">");
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
        //tdao.parseTermSet();
        Set<Term> termSet = TermDao.all_term_set;
        int maxlen = TermDao.MAXLEN;
        String test_file = CommonTool.getFileList(under_test_dir).get(0);

        SimHash simhash = new SimHash();
        Map<String, String> termHash = simhash.hashFunction.termToHash(termSet);
        //simhash.saveHash2File(document_set,docset_vector,termSet,maxlen,termHash);

        int run_times = (args.length > 0) ? Integer.parseInt(args[0]) : 1;
        File f = new File("e:\\metadata_raw\\simhash_single.txt");
        if (f.exists()) f.delete();
        BufferedWriter bw = new BufferedWriter(new FileWriter(f, true));
        for (int i = 0; i < run_times; i++) {
            long start = System.currentTimeMillis();
            simhash.run(test_file, docset_vector, termSet, maxlen, termHash, i);
            long end = System.currentTimeMillis();
            bw.write((i + 1) + "\t" + String.valueOf((end - start) / 1000.0f));
            bw.newLine();
        }
        bw.close();


    }

}
