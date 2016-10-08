package com.dafei.mapred.merge;

import com.dafei.bean.Term;
import com.dafei.datasource.TermDao;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-4-9
 * Time: ÏÂÎç3:39
 * To change this template use File | Settings | File Templates.
 */
public class TFSequence extends Configured implements Tool {
    private static final Logger log = Logger.getLogger(TFSequence.class);
    private TermDao tdao;

    protected void predo(JobConf conf) throws IOException {
        //  log.info("enter predo method...");
        this.tdao = new TermDao();
        try {
            //tdao.parseTermSet();
            tdao.parseTermFromHbase();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        Set<Term> termSet = TermDao.all_term_set;
        int len = TermDao.MAXLEN;

        String termListFile = conf.get("termlist.path", "/user/xjtudlc/dafei/termlist.txt");
        FileSystem fs = FileSystem.get(conf);
        Path termListPath = new Path(termListFile);
        if (fs.exists(termListPath)) fs.delete(termListPath, true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(termListPath)));
        try {
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

    public int run(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("TFSequence <input-dir> <output-dir>  [mappers] [reducers]");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        int mappers = (args.length > 2) ? Integer.parseInt(args[2]) : 10;
        int reducers = (args.length > 3) ? Integer.parseInt(args[3]) : 5;

        JobConf conf = new JobConf(getConf(), TFSequence.class);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(conf, inputPath);
        FileOutputFormat.setOutputPath(conf, outputPath);
        conf.setJobName("merge small file to tf sequence file");
        conf.setInputFormat(WholeFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(TFSequenceMapper.class);
        conf.setReducerClass(IdentityReducer.class);
        conf.setNumMapTasks(mappers);
        conf.setNumReduceTasks(reducers);
        predo(conf);
        JobClient.runJob(conf);
        System.out.println("merging finished!");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TFSequence(), args);
        System.exit(exitCode);
    }

}
