package com.dafei.mapred.simhash;

import com.dafei.tools.Formula;
import com.dafei.tools.Utils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-28
 * Time: 12:41pm
 * To change this template use File | Settings | File Templates.
 */

/**
 * simhash algorithm mapper
 */
public class SimHashMapper extends MapReduceBase
        implements Mapper<Text, Text, Text, Text> {
    private static final Logger log = Logger.getLogger(SimHashMapper.class);
    private JobConf conf;
    private int threshold[];
    private String testHashValue[];


    enum Clusters {
        Accepted, Discarded
    }

    public void configure(JobConf conf) {
        this.conf = conf;
        this.threshold = new int[3];
        String s[] = conf.get("simhash.threshold", "10,17,14").split(",");
        for (int i = 0; i < s.length; i++)
            threshold[i] = Integer.parseInt(s[i]);
        //log.info("get from conf threshold is: <" + threshold[0]+","+threshold[1]+","+threshold[2]+">");

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

    }

    private void init(Path cachePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(cachePath.toString()));
        testHashValue = br.readLine().split(",");
        br.close();
    }

    /**
     * @param key      filename
     * @param value    file content
     * @param output   <filename, filecontent simhash value>
     * @param reporter
     * @throws java.io.IOException
     */
    public void map(Text key, Text value,
                    OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        String hashvalue[] = value.toString().split(",");
        int dis48 = Utils.hammingDistance(this.testHashValue[0], hashvalue[0]);
        int dis64 = Utils.hammingDistance(this.testHashValue[1], hashvalue[1]);
        int dis80 = Utils.hammingDistance(this.testHashValue[2], hashvalue[2]);
        if ((dis48 == 0) && (dis64 == 0) && (dis80 == 0)) {
            reporter.getCounter(Clusters.Accepted).increment(1);
            output.collect(key, new Text("<" + hashvalue[0] + "," + hashvalue[1] + "," + hashvalue[2] + ">" + "\t" + "<" + dis48 + "," + dis64 + "," + dis80 + ">"));

        } else if (!((dis48 == 0) && (dis64 == 0) && (dis80 == 0)) && Formula.ellipsoid(dis48, dis64, dis80, threshold)) {
            reporter.getCounter(Clusters.Accepted).increment(1);
            output.collect(key, new Text("<" + hashvalue[0] + "," + hashvalue[1] + "," + hashvalue[2] + ">" + "\t" + "<" + dis48 + "," + dis64 + "," + dis80 + ">"));
        } else {
            reporter.getCounter(Clusters.Discarded).increment(1);
        }
    }


}
