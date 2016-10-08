package yottabox.resource;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import yottabox.metrics.StargateMetrics;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-19
 * Time: ÏÂÎç10:09
 * To change this template use File | Settings | File Templates.
 */
public class YottaBoxRest implements Constants {

    private static YottaBoxRest instance;

    HBaseConfiguration conf;
    HTablePool pool;
    AtomicBoolean stopping = new AtomicBoolean(false);
    Map<String, Integer> maxAgeMap =
            Collections.synchronizedMap(new HashMap<String, Integer>());
    StargateMetrics metrics = new StargateMetrics();

    /**
     * @return the YottaBoxRest singleton instance
     * @throws java.io.IOException
     */
    public synchronized static YottaBoxRest getInstance() throws IOException {
        if (instance == null) {
            instance = new YottaBoxRest();
        }
        return instance;
    }

    /**
     * Constructor
     *
     * @throws java.io.IOException
     */
    public YottaBoxRest() throws IOException {
        this.conf = new HBaseConfiguration();
        this.pool = new HTablePool(conf, 10);
    }

    HTablePool getTablePool() {
        return pool;
    }

    public HBaseConfiguration getConfiguration() {
        return conf;
    }

    StargateMetrics getMetrics() {
        return metrics;
    }

    /**
     * @param tableName the table name
     * @return the maximum cache age suitable for use with this table, in
     *         seconds
     * @throws java.io.IOException
     */
    public int getMaxAge(String tableName) throws IOException {
        Integer i = maxAgeMap.get(tableName);
        if (i != null) {
            return i.intValue();
        }
        HTable table = pool.getTable(tableName);
        try {
            int maxAge = DEFAULT_MAX_AGE;
            for (HColumnDescriptor family :
                    table.getTableDescriptor().getFamilies()) {
                int ttl = family.getTimeToLive();
                if (ttl < 0) {
                    continue;
                }
                if (ttl < maxAge) {
                    maxAge = ttl;
                }
            }
            maxAgeMap.put(tableName, maxAge);
            return maxAge;
        } finally {
            pool.putTable(table);
        }
    }

    /**
     * Signal that a previously calculated maximum cache age has been
     * invalidated by a schema change.
     *
     * @param tableName the table name
     */
    public void invalidateMaxAge(String tableName) {
        maxAgeMap.remove(tableName);
    }
}
