package com.dafei.tools;

import com.dafei.bean.Term;
import sun.io.ByteToCharGBK;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-30
 * Time: 9:10am
 * To change this template use File | Settings | File Templates.
 */
public class Utils {
    /**
     * @param termSet       term set
     * @param hm_termToHash <term£¬hash>
     * @param hm_termCount  <term£¬text frequency>
     */
    public static Map<Integer, int[]> createVector(Set<Term> termSet,
                                                   Map<String, String> hm_termToHash,
                                                   Map<String, Integer> hm_termCount) {
        int termCount;//term freq
        String termHash;// term hash value
        Map<Integer, int[]> m_bitComponent = new HashMap<Integer, int[]>();// n bit hash vector
        if (!m_bitComponent.containsKey(64)) m_bitComponent.put(64, new int[64]);
        if (!m_bitComponent.containsKey(48)) m_bitComponent.put(48, new int[48]);
        if (!m_bitComponent.containsKey(80)) m_bitComponent.put(80, new int[80]);
        for (Term term : termSet) {
            termCount = (null == hm_termCount.get(term.getT_name())) ? 0 : hm_termCount.get(term.getT_name());
            termHash = hm_termToHash.get(term.getT_name());
            if (termCount != 0) {
                switch (termHash.length()) {  // n-bit fingerprint , every clever in mapping points from high-dimensional space to low-dimensional space
                    case 64: {// 64-bit hash vector
                        m_bitComponent.put(64, createComponent(m_bitComponent.get(64), termCount, termHash));
                        break;
                    }
                    case 48: {
                        m_bitComponent.put(48, createComponent(m_bitComponent.get(48), termCount, termHash));
                        break;
                    }
                    case 80: {
                        m_bitComponent.put(80, createComponent(m_bitComponent.get(80), termCount, termHash));
                        break;
                    }
                    default:
                }//end switch
            }//end if
        }//end for


        return m_bitComponent;
    }

    /**
     * @param component document hash value ,64 integers
     * @param termCount term freq
     * @param termHash  term hash
     * @return termHash.length()-bit hash vector
     */
    private static int[] createComponent(int[] component, int termCount, String termHash) {
        int[] comp = new int[termHash.length()];
        for (int i = 0; i < termHash.length(); i++) {
            if (termHash.charAt(i) == '0') {
                comp[i] = (component == null ? -termCount : component[i] - termCount);
            } else if (termHash.charAt(i) == '1') {
                comp[i] = (component == null ? termCount : component[i] + termCount);
            }
        }
        return comp;
    }

    /**
     * @param m_bitComponent weighted vector
     * @return normalized vector
     */
    public static Map<Integer, String> formatVector(Map<Integer, int[]> m_bitComponent) {
        Map<Integer, String> m_vector01 = new HashMap<Integer, String>();
        Set<Integer> set = m_bitComponent.keySet();
        Iterator<Integer> it = set.iterator();
        while (it.hasNext()) {
            StringBuilder sb = new StringBuilder();
            int[] component = m_bitComponent.get(it.next());
            if (component != null) {
                for (int i = 0; i < component.length; i++) {
                    if (component[i] > 0) {
                        sb.append(String.valueOf(1));
                    } else if (component[i] <= 0) {
                        sb.append(String.valueOf(0));
                    }
                }
                m_vector01.put(component.length, sb.toString());
                sb = null;
            }
        }
        return m_vector01;
    }


    /**
     * @param vector1 hashvalue1
     * @param vector2 hashvalue2
     * @return hamming distance between vector1 and vector2 , if return -1, not comparable
     */
    public static int hammingDistance(String vector1, String vector2) {
        int distance = 0;
        if (vector1.length() == vector2.length()) {
            for (int i = 0; i < vector1.length(); i++) {
                if (vector1.charAt(i) != vector2.charAt(i)) {
                    distance++;
                }
            }
        } else
            distance = -1;
        return distance;
    }

    /**
     * save normalized vector to file
     *
     * @param vector01            normalized vector
     * @param fileName            which file u want to save
     * @param doc_set_vector_file
     */
    public static void storeVectorToFile(Map<Integer, String> vector01, String fileName, File doc_set_vector_file) {

        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(doc_set_vector_file, true));
            bw.write(fileName + "," + vector01.get(48) + "," + vector01.get(64) + "," + vector01.get(80));
            bw.newLine();
            // bw.flush();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void saveFreq(String fileName, Map<String, Integer> map, File vectorFileName) {
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(vectorFileName, true));
            bw.write(fileName + "\001");
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                bw.write(entry.getKey() + "," + entry.getValue().toString() + "\t");
            }
            bw.newLine();
            bw.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {

        }
    }

    /**
     * @param hm_itermCount  <term,text frequency>
     * @param termSet        term set no duplication
     * @param vectorFileName
     */
    public static void saveFreqToFile(Map<String, Integer> hm_itermCount, Set<Term> termSet, String vectorFileName) {
        File frequentFile = new File(vectorFileName);
        Iterator<Term> termIterator = termSet.iterator();
        int fre;
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(frequentFile, true));
            while (termIterator.hasNext()) {
                String tname = termIterator.next().getT_name();
                fre = (null == hm_itermCount.get(tname)) ? 0 : hm_itermCount.get(tname);
                bw.write(String.valueOf(fre));
                bw.write("\t");

            }
            bw.newLine();
            //bw.flush();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
    }

    public static String longestSentence(String filepath) {
        File f = new File(filepath);
        String longest = null;
        try {
            BufferedReader br = new BufferedReader(new FileReader(f));
            String line = null;

            while ((line = br.readLine()) != null) {
                longest = line;
                String regxs[] = line.split("[!£¡?£¿.¡£]+\\S+[!£¡?£¿.¡£]+");
                int len = (regxs[0] == null) ? 0 : regxs[0].length();
                for (String s : regxs)
                    if (s != null && s.length() > len) longest = s;

            }
            br.close();


        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return longest;
    }

    /**
     * @param content bytes
     * @param termSet term set
     * @param maxlen  max term length of all term in termSet
     * @return <term , text frequency>
     * @throws IOException
     */
    public static Map<String, Integer> countTermFrequent(byte content[], Set<Term> termSet, int maxlen) throws IOException {
        Map<String, Integer> map = new HashMap<String, Integer>();
        sun.io.ByteToCharGBK convert = new ByteToCharGBK();
        char ch[] = convert.convertAll(content);
        int i = 0;
        // int chunklen = 1; // this line 0.5h wtf.....
        while (i < ch.length) {
            StringBuilder sb = new StringBuilder();
            for (int j = i; j < (maxlen + i) && j < ch.length; j++) {
                String str = sb.append(ch[j]).toString();
                Term term = new Term(str);
                if (termSet.contains(term)) {
                    //  chunklen = str.length() < chunklen ? str.length() : chunklen;
                    if (map.get(str) == null)
                        map.put(str, 1);
                    else
                        map.put(str, map.get(str) + 1);
                    //  i = j - str.length() + chunklen;   // this line 1h, i need sleep......
                }
            }
            i++;
        }
        return map;
    }


    /**
     * @param filepath
     * @param termSet
     * @param maxlen   max term length of all term in termSet
     * @return <term, text frequency>
     */
    public static Map<String, Integer> countTermFrequent(String filepath, Set<Term> termSet, int maxlen) {
        Map<String, Integer> map = new HashMap<String, Integer>();
        File f = new File(filepath);
        if (!f.exists() || !f.canRead()) {
            System.out
                    .println("Document directory '"
                            + f.getAbsolutePath()
                            + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }
        try {
            BufferedReader br = new BufferedReader(new FileReader(f));
            String line = null;
            while ((line = br.readLine()) != null)
                count1line(map, line, termSet, maxlen);
            br.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return map;
    }

    private static void count1line(Map<String, Integer> map, String line, Set<Term> termSet, int maxlen) {
        char ch[] = line.toCharArray();
        int i = 0;
        // int chunklen = 1; // this line 0.5h wtf.....
        while (i < ch.length) {
            StringBuilder sb = new StringBuilder();
            for (int j = i; j < (maxlen + i) && j < ch.length; j++) {
                String str = sb.append(ch[j]).toString();
                Term term = new Term(str);
                if (termSet.contains(term)) {
                    //  chunklen = str.length() < chunklen ? str.length() : chunklen;
                    if (map.get(str) == null)
                        map.put(str, 1);
                    else
                        map.put(str, map.get(str) + 1);
                    //  i = j - str.length() + chunklen;   // this line 1h, i need sleep......
                }
            }
            i++;
        }
    }

}