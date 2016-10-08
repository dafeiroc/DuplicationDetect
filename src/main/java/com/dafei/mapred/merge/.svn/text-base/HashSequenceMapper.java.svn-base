package com.dafei.mapred.merge;

import com.dafei.bean.Term;
import com.dafei.tools.HashFactory;
import com.dafei.tools.IHashFunction;
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
 * Time: ÏÂÎç7:09
 * To change this template use File | Settings | File Templates.
 */
public class HashSequenceMapper extends MapReduceBase
        implements Mapper<NullWritable, BytesWritable, Text, Text> {
    private static final Logger log = Logger.getLogger(HashSequenceMapper.class);
    private Set<Term> termSet;
    private int maxlen;
    private Map<String, String> hm_termToHash;
    private IHashFunction hashFunction;
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
        this.hashFunction = HashFactory.createHashFunctions(HashFactory.HashType.SIMPLEGBK);
        this.hm_termToHash = this.hashFunction.termToHash(this.termSet);
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
        Map<Integer, int[]> doc_int_map = Utils.createVector(termSet, hm_termToHash, doc_map);
        Map<Integer, String> doc_01vector = Utils.formatVector(doc_int_map);
        output.collect(new Text(filename), new Text(doc_01vector.get(48) + "," + doc_01vector.get(64) + "," + doc_01vector.get(80)));
    }
}
