package com.dafei.mapred.vsm;

import com.dafei.bean.Term;
import com.dafei.datasource.TermDao;
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
 * Date: 11-3-30
 * Time: 9:40pm
 * To change this template use File | Settings | File Templates.
 */
public class VSMMapRed extends Configured implements Tool {
    private static final Logger log = Logger.getLogger(VSMMapRed.class);
    private TermDao tdao;

    protected Map<String, Integer> getTestFreqMap(JobConf conf, Set<Term> set, int max) throws IOException {
        //        File test = new File("test.txt");
//        FileInputStream fis = new FileInputStream(test);
//        byte content[] = new byte[(int) test.length()];
        String underTestFile = "/user/xjtudlc/dafei/test.txt";
        Path file = new Path(underTestFile);
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fis = fs.open(file);
        byte content[] = new byte[(int) fs.getFileStatus(file).getLen()];
        IOUtils.readFully(fis, content, 0, content.length);
        Map<String, Integer> test_map = Utils.countTermFrequent(content, set, max);
        return test_map;
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

        Map<String, Integer> test_map = getTestFreqMap(conf, termSet, len);
        String termListFile = conf.get("termlist.path", "/user/xjtudlc/dafei/termlist.txt");
        FileSystem fs = FileSystem.get(conf);
        Path termListPath = new Path(termListFile);
        if (fs.exists(termListPath)) fs.delete(termListPath, true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(termListPath)));
        try {
            for (Map.Entry<String, Integer> entry : test_map.entrySet()) {
                bw.write(entry.getKey() + "," + entry.getValue() + "\t");
            }
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
            System.out.println("VSMMapRed <input-dir> <output-dir> [threshold] [mappers] [reducers] [category] [running times]");
            System.out.println("category: 1. Cosine 2.Pearson 3.Spearman 4. Eulidean");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }


        int ret = 0;
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        float threshold = (args.length > 2) ? Float.parseFloat(args[2]) : 0.1f;
        int mappers = (args.length > 3) ? Integer.parseInt(args[3]) : 10;
        int reducers = (args.length > 4) ? Integer.parseInt(args[4]) : 1;
        int category = (args.length > 5) ? Integer.parseInt(args[5]) : 2;
        int run_times = (args.length > 6) ? Integer.parseInt(args[6]) : 1;

        try {
            File f = new File("/home/xjtudlc/dafei/vsm_mapred.txt");
            if (f.exists()) f.delete();
            BufferedWriter bw = new BufferedWriter(new FileWriter(f, true));
            for (int i = 0; i < run_times; i++) {
                long start = System.currentTimeMillis();
                JobConf job = new JobConf(getConf(), VSMMapRed.class);
                FileSystem fs = FileSystem.get(job);
                if (fs.exists(outputPath)) {
                    fs.delete(outputPath, true);
                }
                job.setJobName("vsm based algorithm");
                FileInputFormat.setInputPaths(job, inputPath);
                FileOutputFormat.setOutputPath(job, outputPath);
                job.setFloat("vsm.threshold", threshold);
                job.setInt("vsm.category", category);
                job.setNumMapTasks(mappers);
                job.setNumReduceTasks(reducers);
                job.setMapperClass(VSMMapper.class);
                job.setReducerClass(IdentityReducer.class);
                job.setInputFormat(SequenceFileInputFormat.class);
                job.setOutputFormat(TextOutputFormat.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);


                predo(job);
                RunningJob running = JobClient.runJob(job);
                running.waitForCompletion();
                if (running.isSuccessful()) {
                    System.out.println("vsm " + (i + 1) + " times completed successfully!");
                } else {
                    System.out.println("vsm " + (i + 1) + " times - Failed!");
                    ret = -1;
                }

                long end = System.currentTimeMillis();
                bw.write((i + 1) + "\t" + (end - start) / 1000.0f);
                bw.newLine();
            }
            bw.close();
        } catch (Exception e) {
            System.out.println("Caught exception while running VSMMapRed algorithm\n");
            e.printStackTrace();
            ret = -1;
        }


        return ret;
    }

    public static void main(String args[]) throws Exception {
        VSMMapRed vsm = new VSMMapRed();
        int retStatus = ToolRunner.run(vsm, args);
        System.exit(retStatus);
    }
}
