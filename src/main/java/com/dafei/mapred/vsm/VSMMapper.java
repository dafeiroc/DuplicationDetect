package com.dafei.mapred.vsm;

import com.dafei.bean.Term;
import com.dafei.tools.Formula;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-30
 * Time: 9:40pm
 * To change this template use File | Settings | File Templates.
 */
public class VSMMapper extends MapReduceBase
        implements Mapper<Text, Text, Text, Text> {
    private static final Logger log = Logger.getLogger(VSMMapper.class);
    private JobConf conf;
    private float threshold;
    private int category;
    private Map<String, Integer> test_map;
    private Set<Term> termSet;

    enum Clusters {
        Accepted, Discarded
    }

    public void configure(JobConf conf) {
        this.conf = conf;
        this.threshold = conf.getFloat("vsm.threshold", 0.1f);
        this.category = conf.getInt("vsm.category", 1);
        //  log.info("enter configure method...");
        try {
            String termListFile = "termlist.txt";
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (Path cachePath : cacheFiles) {
                    // log.info("PATH is :" + cachePath);
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
        String line = br.readLine();
        String temp[] = line.split("\t");
        test_map = new HashMap<String, Integer>();
        for (String item : temp) {
            String t[] = item.split(",");
            test_map.put(t[0], Integer.valueOf(t[1]));
        }
        br.readLine();  // max length
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

    /**
     * @param key      filename
     * @param value    <term,text frequence>
     * @param output   <filename, tfidf or tf>
     * @param reporter
     * @throws java.io.IOException
     */
    public void map(Text key, Text value,
                    OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        String line[] = value.toString().split("\t");
        Map<String, Integer> doc_map = new HashMap<String, Integer>();
        for (String sp : line) {
            String s[] = sp.split(",");
            doc_map.put(s[0], Integer.valueOf(s[1]));
        }
        double dis = 0;
        switch (this.category) {
            case 1: {
                dis = Formula.minus_cosine(test_map, doc_map);
                if (dis < this.threshold) {
                    // output.collect(key,new Text("\n["+freq.toString()+"]"));
                    output.collect(key, new Text(String.valueOf(dis)));
                    reporter.getCounter(Clusters.Accepted).increment(1);
                } else {
                    reporter.getCounter(Clusters.Discarded).increment(1);
                }
                break;
            }
            case 2: {
                dis = Formula.minus_pearson(test_map, doc_map, termSet.size());
                if (dis < this.threshold) {
                    // output.collect(key,new Text("\n["+freq.toString()+"]"));
                    output.collect(key, new Text(String.valueOf(dis)));
                    reporter.getCounter(Clusters.Accepted).increment(1);
                } else {
                    reporter.getCounter(Clusters.Discarded).increment(1);
                }
                break;
            }
            case 3: {
                dis = Formula.spearman(test_map, doc_map, termSet.size());
                output.collect(key, new Text(String.valueOf(dis)));
                break;
            }
            case 4: {
                dis = Formula.euclidean(test_map, doc_map);
                output.collect(key, new Text(String.valueOf(dis)));
                break;
            }
        }

        log.info("No." + this.category + " similarity is: " + dis);
    }
}
