package com.dafei.mapred.merge;

import com.dafei.bean.Term;
import com.dafei.tools.Utils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-4-9
 * Time: ÏÂÎç7:10
 * To change this template use File | Settings | File Templates.
 */
public class TFSequenceMapper extends MapReduceBase
        implements Mapper<NullWritable, BytesWritable, Text, Text> {
    private static final Logger log = Logger.getLogger(TFSequenceMapper.class);
    private Set<Term> termSet;
    private int maxlen;
    private JobConf conf;


    public void configure(JobConf conf) {
        this.conf = conf;
        //  log.info("enter configure method...");
        try {
            String termListFile = "termlist.txt";
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (Path cachePath : cacheFiles) {
                    //log.info("PATH is :" + cachePath);
                    if (cachePath.getName().equals(termListFile)) {
                        init(cachePath);
                        break;
                    }
                }
            }
        } catch (IOException ioe) {
            System.err.println("IOException reading from distributed cache");
            System.err.println(ioe.toString());
        }
        // log.info("the term list size is:"+term_list.size()+" and the max length of all term set is: "+maxlen);

    }

    private void init(Path cachePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(cachePath.toString()));
        String line = null;
        this.maxlen = Integer.parseInt(br.readLine());
        this.termSet = new HashSet<Term>();
        try {
            while ((line = br.readLine()) != null) {
                String s[] = line.split("\t");
                Term t = new Term(Integer.parseInt(s[0]), s[1]);
                this.termSet.add(t);
                // log.info("term id is: "+t.getT_id()+",and term name is: "+t.getT_name());
            }
        } finally {
            br.close();
        }
    }

    public void map(NullWritable key, BytesWritable value,
                    OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        String filename = conf.get("map.input.file");
        byte content[] = value.getBytes();
        Map<String, Integer> doc_map = Utils.countTermFrequent(content, termSet, maxlen);
        // log.info("doc_map generate!! size is: " + doc_map.size());
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Integer> e : doc_map.entrySet()) {
            sb.append(e.getKey() + "," + e.getValue() + "\t");
        }
        output.collect(new Text(filename), new Text(sb.toString()));
    }
}