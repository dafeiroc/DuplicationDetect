package com.dafei.mapred.vsm;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-30
 * Time: 9:40am
 * To change this template use File | Settings | File Templates.
 */
public class VSMReducer extends MapReduceBase
        implements Reducer<Text, Text, Text, Text> {
    private static final Logger log = Logger.getLogger(VSMReducer.class);


    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {


    }

}
