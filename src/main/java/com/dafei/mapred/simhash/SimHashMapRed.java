package com.dafei.mapred.simhash;

import com.dafei.bean.Term;
import com.dafei.datasource.TermDao;
import com.dafei.tools.HashFactory;
import com.dafei.tools.IHashFunction;
import com.dafei.tools.Utils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-27
 * Time: 10:02am
 * To change this template use File | Settings | File Templates.
 */
public class SimHashMapRed extends Configured implements Tool {
    private static final Logger log = Logger.getLogger(SimHashMapRed.class);
    private TermDao tdao;
    private Map<String, String> hm_termToHash;
    private IHashFunction hashFunction;

    protected Map<Integer, String> getTestFileHashValue(JobConf conf, Set<Term> set, int max, Map<String, String> termHash) throws IOException {
        //use distributed cache
//        File test = new File("test.txt");
//          FileInputStream fis = new FileInputStream(test);
//        byte content[] = new byte[(int) test.length()];
        String underTestFile = "/user/xjtudlc/dafei/test.txt";
        Path file = new Path(underTestFile);
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fis = fs.open(file);
        byte content[] = new byte[(int) fs.getFileStatus(file).getLen()];
        IOUtils.readFully(fis, content, 0, content.length);
        Map<String, Integer> test_map = Utils.countTermFrequent(content, set, max);
        Map<Integer, int[]> test_int_map = Utils.createVector(set, termHash, test_map);
        Map<Integer, String> test_01vector = Utils.formatVector(test_int_map);
        //log.info("test file hash value is "+test_01vector.get(64));
        return test_01vector;
    }

    protected void predo(JobConf conf) throws IOException {
        //  log.info("enter predo method...");
        this.tdao = new TermDao();
        try {
            // tdao.parseTermSet();
            tdao.parseTermFromHbase();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        Set<Term> termSet = TermDao.all_term_set;
        int len = TermDao.MAXLEN;
        this.hashFunction = HashFactory.createHashFunctions(HashFactory.HashType.SIMPLEGBK);
        this.hm_termToHash = this.hashFunction.termToHash(termSet);
        Map<Integer, String> testHashValue = getTestFileHashValue(conf, termSet, len, hm_termToHash);
        String termListFile = conf.get("termlist.path", "/user/xjtudlc/dafei/termlist.txt");
        FileSystem fs = FileSystem.get(conf);
        Path termListPath = new Path(termListFile);
        if (fs.exists(termListPath)) fs.delete(termListPath, true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(termListPath)));
        try {
            bw.write(testHashValue.get(48) + "," + testHashValue.get(64) + "," + testHashValue.get(80));
            bw.newLine();
            bw.write(String.valueOf(len));
            bw.newLine();
            for (Term t : termSet) {
                bw.write(String.valueOf(t.getT_id()) + '\t' + t.getT_name());
                bw.newLine();
            }
        } finally {
            bw.close();
            DistributedCache.addCacheFile(termListPath.toUri(), conf);
        }
        //log.info("from db2, the termlist size is: "+tl.size()+" ,and the max length is: "+len);
    }


    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("SimHashMapRed <input-dir> <output-dir> [10,17,14] [mappers] [reducers] [running times]");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }
        int ret = 0;
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        String threshold = (args.length > 2) ? args[2] : "10,17,14";
        int mappers = (args.length > 3) ? Integer.parseInt(args[3]) : 10;
        int reducers = (args.length > 4) ? Integer.parseInt(args[4]) : 1;
        int run_times = (args.length > 5) ? Integer.parseInt(args[5]) : 1;
        try {
            File f = new File("/home/xjtudlc/dafei/simhash_mapred.txt");
            if (f.exists()) f.delete();
            BufferedWriter bw = new BufferedWriter(new FileWriter(f, true));
            for (int i = 0; i < run_times; i++) {
                long start = System.currentTimeMillis();
                JobConf simhashjob = new JobConf(getConf(), SimHashMapRed.class);
                FileSystem fs = FileSystem.get(simhashjob);
                if (fs.exists(outputPath)) {
                    fs.delete(outputPath, true);
                }
                simhashjob.setJobName("simhash algorithm");
                FileInputFormat.setInputPaths(simhashjob, inputPath);
                FileOutputFormat.setOutputPath(simhashjob, outputPath);
                simhashjob.set("simhash.threshold", threshold);
                simhashjob.setNumMapTasks(mappers);
                simhashjob.setNumReduceTasks(reducers);
                simhashjob.setMapperClass(SimHashMapper.class);
                simhashjob.setReducerClass(IdentityReducer.class);
                simhashjob.setInputFormat(SequenceFileInputFormat.class);
                simhashjob.setOutputFormat(TextOutputFormat.class);
                simhashjob.setOutputKeyClass(Text.class);
                simhashjob.setOutputValueClass(Text.class);


                predo(simhashjob);
                RunningJob running = JobClient.runJob(simhashjob);

                running.waitForCompletion();

                if (running.isSuccessful()) {
                    System.out.println("simhash " + (i + 1) + " times completed successfully!");
                } else {
                    System.out.println("simhash " + (i + 1) + " times - Failed!");
                    ret = -1;
                }
                long end = System.currentTimeMillis();
                bw.write((i + 1) + "\t" + String.valueOf((end - start) / 1000.0f));
                bw.newLine();


            }
            bw.close();

        } catch (Exception e) {
            System.out.println("Caught exception while running simhash algorithm\n");
            e.printStackTrace();
            ret = -1;
        }
        return ret;
    }

    public static void main(String args[]) throws Exception {
        SimHashMapRed simhash = new SimHashMapRed();
        int retStatus = ToolRunner.run(simhash, args);
        System.exit(retStatus);
    }
}
