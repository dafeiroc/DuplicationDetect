package com.dafei.datasource;

import com.dafei.bean.Term;
import com.dafei.tools.CommonTool;
import com.dafei.tools.HashFactory;
import com.dafei.tools.IHashFunction;
import com.dafei.tools.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-11
 * Time: 2:06pm
 * To change this template use File | Settings | File Templates.
 */

/**
 * generate hadoop sequence file for vsm algorithm and simhash algorithm
 */
public class DocumentProcess {

    public static void main(String args[]) throws Exception {
        TermDao tdao = new TermDao();
        tdao.parseTermFromHbase();
        //tdao.parseTermSet();
        Set<Term> termSet = TermDao.all_term_set;
        int maxlen = TermDao.MAXLEN;
        IHashFunction hashFunction = HashFactory.createHashFunctions(HashFactory.HashType.SIMPLEGBK);
        Map<String, String> termHash = hashFunction.termToHash(termSet);

        DocumentProcess dp = new DocumentProcess();
        Configuration conf = init();
        // dp.copyAll("e:\\metadata_raw\\all", "e:\\metadata_raw\\all_10G", 10000);
        //dp.copyAll2HDFS("e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_100M", conf, 10);
//        dp.copyVSMSequence2HDFS(10, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_100M_vsm_sequence/test.seq", conf, termSet, maxlen);
//        dp.copyHashSequence2HDFS(10, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_100M_simhash_sequence/test.seq", conf, termSet, maxlen, termHash);
//        dp.copyVSMSequence2HDFS(20, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_200M_vsm_sequence/test.seq", conf, termSet, maxlen);
//        dp.copyHashSequence2HDFS(20, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_200M_simhash_sequence/test.seq", conf, termSet, maxlen, termHash);
//        dp.copyVSMSequence2HDFS(50, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_500M_vsm_sequence/test.seq", conf, termSet, maxlen);
//        dp.copyHashSequence2HDFS(50, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_500M_simhash_sequence/test.seq", conf, termSet, maxlen, termHash);
//
//        dp.copyVSMSequence2HDFS(100, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_1G_vsm_sequence/test.seq", conf, termSet, maxlen);
//        dp.copyHashSequence2HDFS(100, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_1G_simhash_sequence/test.seq", conf, termSet, maxlen, termHash);
//        dp.copyVSMSequence2HDFS(200, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_2G_vsm_sequence/test.seq", conf, termSet, maxlen);
//        dp.copyHashSequence2HDFS(200, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_2G_simhash_sequence/test.seq", conf, termSet, maxlen, termHash);
//        dp.copyVSMSequence2HDFS(500, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_5G_vsm_sequence/test.seq", conf, termSet, maxlen);
//        dp.copyHashSequence2HDFS(500, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_5G_simhash_sequence/test.seq", conf, termSet, maxlen, termHash);
//
//        dp.copyVSMSequence2HDFS(1000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_10G_vsm_sequence/test.seq", conf, termSet, maxlen);
//        dp.copyHashSequence2HDFS(1000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_10G_simhash_sequence/test.seq", conf, termSet, maxlen, termHash);
//        dp.copyVSMSequence2HDFS(2000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_20G_vsm_sequence/test.seq", conf, termSet, maxlen);
//        dp.copyHashSequence2HDFS(2000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_20G_simhash_sequence/test.seq", conf, termSet, maxlen, termHash);
//        dp.copyVSMSequence2HDFS(5000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_50G_vsm_sequence/test.seq", conf, termSet, maxlen);
//        dp.copyHashSequence2HDFS(5000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_50G_simhash_sequence/test.seq", conf, termSet, maxlen, termHash);

//        dp.copyVSMSequence2HDFS(10000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_100G_vsm_sequence/test.seq", conf, termSet, maxlen);
//        dp.copyHashSequence2HDFS(10000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_100G_simhash_sequence/test.seq", conf, termSet, maxlen, termHash);
//        dp.copyVSMSequence2HDFS(20000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_200G_vsm_sequence/test.seq", conf, termSet, maxlen);
//        dp.copyHashSequence2HDFS(20000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_200G_simhash_sequence/test.seq", conf, termSet, maxlen, termHash);
//        dp.copyVSMSequence2HDFS(50000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_500G_vsm_sequence/test.seq", conf, termSet, maxlen);
//        dp.copyHashSequence2HDFS(50000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_500G_simhash_sequence/test.seq", conf, termSet, maxlen, termHash);
//
//        dp.copyVSMSequence2HDFS(100000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_1T_vsm_sequence/test.seq", conf, termSet, maxlen);
//        dp.copyHashSequence2HDFS(100000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_1T_simhash_sequence/test.seq", conf, termSet, maxlen, termHash);
        dp.copyVSMSequence2HDFS(200000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_2T_vsm_sequence/test.seq", conf, termSet, maxlen);
        dp.copyHashSequence2HDFS(200000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_2T_simhash_sequence/test.seq", conf, termSet, maxlen, termHash);
        dp.copyVSMSequence2HDFS(500000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_5T_vsm_sequence/test.seq", conf, termSet, maxlen);
        dp.copyHashSequence2HDFS(500000, "e:\\metadata_raw\\all", "hdfs://job-tracker:54310/user/xjtudlc/dafei/all_5T_simhash_sequence/test.seq", conf, termSet, maxlen, termHash);

    }

    private static Configuration init() {
        Configuration conf = new Configuration();
        conf.set("hadoop.job.ugi", "xjtudlc,xjtudlc");
        return conf;
    }

    /**
     * @param dir
     * @param termSet
     * @param maxlen
     * @param tf_file
     * @throws Exception
     */
    public void sendToFile(String dir, Set<Term> termSet, int maxlen, String tf_file) throws Exception {
        for (String filepath : CommonTool.getFileList(dir)) {
            System.out.println("processing filename:" + CommonTool.extractFileName(filepath) + "...");
            Map<String, Integer> hm_itermCount = Utils.countTermFrequent(filepath, termSet, maxlen);
            System.out.println("processing filename:" + CommonTool.extractFileName(filepath) + " finished.");
            Utils.saveFreqToFile(hm_itermCount, termSet, tf_file);
        }
    }

    public boolean copyVSMSequence2HDFS(int count, String from_dir, String sequenceFile, Configuration conf, Set<Term> termSet, int maxlen) {
        List<String> fileList = CommonTool.getFileList(from_dir);
        FileSystem fs = null;
        SequenceFile.Writer writer = null;
        try {
            fs = FileSystem.get(URI.create("hdfs://job-tracker:54310"), conf);
            Path sequencePath = new Path(sequenceFile);
            if (fs.exists(sequencePath)) fs.delete(sequencePath, true);
            Text key = new Text();
            Text value = new Text();
            writer = SequenceFile.createWriter(fs, conf, sequencePath, key.getClass(), value.getClass());
            for (String filePath : fileList) {
                String fileName = CommonTool.extractFileName(filePath);
                Map<String, Integer> map = Utils.countTermFrequent(filePath, termSet, maxlen);
                for (int i = 0; i < count; i++) {
                    key.set(fileName + "_" + i);
                    StringBuilder sb = new StringBuilder();
                    for (Map.Entry<String, Integer> entry : map.entrySet()) {
                        sb.append(entry.getKey() + "," + entry.getValue().toString() + "\t");
                    }
                    value.set(sb.toString());
                    System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                    writer.append(key, value);
                }
            }
            return true;
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return false;
        } finally {
            IOUtils.closeStream(writer);
        }
    }

    public boolean copyHashSequence2HDFS(int count, String from_dir, String sequenceFile, Configuration conf, Set<Term> termSet, int maxlen, Map<String, String> termHash) {
        List<String> fileList = CommonTool.getFileList(from_dir);
        FileSystem fs = null;
        SequenceFile.Writer writer = null;
        try {
            fs = FileSystem.get(URI.create("hdfs://job-tracker:54310"), conf);
            Path sequencePath = new Path(sequenceFile);
            if (fs.exists(sequencePath)) fs.delete(sequencePath, true);
            Text key = new Text();
            Text value = new Text();
            writer = SequenceFile.createWriter(fs, conf, sequencePath, key.getClass(), value.getClass());
            for (String filePath : fileList) {
                String fileName = CommonTool.extractFileName(filePath);
                Map<String, Integer> map = Utils.countTermFrequent(filePath, termSet, maxlen);
                Map<Integer, int[]> map_int = Utils.createVector(termSet, termHash, map);
                Map<Integer, String> vector = Utils.formatVector(map_int);
                for (int i = 0; i < count; i++) {
                    key.set(fileName + "_" + i);
                    value.set(vector.get(48) + "," + vector.get(64) + "," + vector.get(80));
                    System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                    writer.append(key, value);
                }
            }
            return true;

        } catch (IOException ioe) {
            ioe.printStackTrace();
            return false;
        } finally {
            IOUtils.closeStream(writer);
        }

    }

    public boolean copyFile2HDFS(String from, String to, Configuration conf) {
        File fin = new File(from);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create("hdfs://job-tracker:54310"), conf);
            FileInputStream fis = new FileInputStream(fin);
            FSDataOutputStream fos = fs.create(new Path(to));
            IOUtils.copyBytes(fis, fos, 4096, true);
            return true;
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            return false;
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return false;
        }
    }

    public boolean copyFile(String from, String to) {
        File fin = new File(from);
        File fout = new File(to);
        try {
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(fin));
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(fout));
            int c;
            while ((c = bis.read()) != -1) {
                bos.write(c);
            }
            bis.close();
            bos.close();
            return true;
        } catch (FileNotFoundException fnfe) {
            return false;
        } catch (IOException ioe) {
            return false;
        }
    }

    public void copyAll2HDFS(String from_dir, String to_dir, Configuration conf, int count) {
        List<String> fileList = CommonTool.getFileList(from_dir);
        for (String filePath : fileList) {
            for (int i = 0; i < count; i++) {
                String fileName = CommonTool.extractFileName(filePath);
                String to = to_dir + "/" + i + "_" + fileName;
                copyFile2HDFS(filePath, to, conf);
            }
        }
    }

    public void copyAll(String from_dir, String to_dir, int count) {
        List<String> fileList = CommonTool.getFileList(from_dir);
        for (String filePath : fileList) {
            for (int i = 0; i < count; i++) {
                String fileName = CommonTool.extractFileName(filePath);
                String to = to_dir + "\\" + i + "_" + fileName;
                copyFile(filePath, to);
            }

        }
    }
}
