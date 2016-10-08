package com.dafei.datasource;

import com.dafei.bean.Term;
import com.dafei.config.ConnectionFactory;
import com.dafei.config.DBConn;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;


/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-3
 * Time: 5:21pm
 * To change this template use File | Settings | File Templates.
 */
/*  parse the term set and get statistical info:
term_and_resource_no_duplication :59819
term with duplicate:31502
term no duplicate:31236   ,25453 used
num of file:      ds:58      os:48   c:18   java:35    network:123  organization:75    architecture:120     plane geometry:8
term of category: ds:1673    os:2697 c:865  java:4661  network:6156 organization:5385  architecture:6346    plane geometry:337   all :25453
term max len:     ds:40      os:32   c:31   java:41    network:47   organization:50    architecture:44      plane geometry:29
 */
public class TermDao {
    public static Set<Term> datastructure_term_set;
    public static Set<Term> os_term_set;
    public static Set<Term> c_term_set;
    public static Set<Term> java_term_set;
    public static Set<Term> network_term_set;
    public static Set<Term> organization_term_set;
    public static Set<Term> architecture_term_set;
    public static Set<Term> pg_term_set;
    public static Set<Term> all_term_set;
    public static int[] maxlength;
    public static int MAXLEN;
    public static int[] length;

    public int[] getMaxLenArr() {
        return maxlength;
    }

    /*find intersection of s1 and s2*/
    public static <T> Set<T> intersection(Set<T> s1, Set<T> s2) {
        Set<T> intersection;
        T iterm;
        if (s1 instanceof TreeSet) intersection = new TreeSet<T>();
        else intersection = new HashSet<T>();
        Iterator<T> it = s1.iterator();
        while (it.hasNext()) {
            iterm = it.next();
            if (s2.contains(iterm)) intersection.add(iterm);
        }
        return intersection;
    }

    /**
     * get termSet from hbase
     */
    public void parseTermFromHbase() throws IOException {
        HBaseConfiguration mConfig = new HBaseConfiguration();
        mConfig.addResource(new Path(ConnectionFactory.getInstance().getDefaultConfig()));
        mConfig.addResource(new Path(ConnectionFactory.getInstance().getSiteConfig()));
        all_term_set = new HashSet<Term>();
        MAXLEN = 0;
        HTable t = new HTable(mConfig, "term");
        Scan s = new Scan();
        s.addColumn(Bytes.toBytes("data:tname"));
        ResultScanner scanner = t.getScanner(s);
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            for (KeyValue kv : rr.raw()) {
                all_term_set.add(new Term(Integer.parseInt(Bytes.toString(kv.getRow())), Bytes.toString(kv.getValue())));
                MAXLEN = Bytes.toString(kv.getValue()).length() > MAXLEN ? Bytes.toString(kv.getValue()).length() : MAXLEN;
            }

        }
        System.out.println("dictionary size: " + all_term_set.size() + " ,max term length: " + MAXLEN);
        scanner.close();
        t.close();

    }

    /**
     * get termSet from database
     *
     * @throws Exception
     */

    public void parseTermSet() throws Exception {
        String s1 = "select * from db2admin.metadata where re_id in (select re_id from db2admin.term_and_resource_no_duplication group by re_id)";
        String s2 = "select * from db2admin.terms a inner join db2admin.term_and_resource_no_duplication b on a.t_id =b.t_id  where b.re_id=?";
        datastructure_term_set = new HashSet<Term>();
        os_term_set = new HashSet<Term>();
        c_term_set = new HashSet<Term>();
        java_term_set = new HashSet<Term>();
        network_term_set = new HashSet<Term>();
        organization_term_set = new HashSet<Term>();
        architecture_term_set = new HashSet<Term>();
        pg_term_set = new HashSet<Term>();
        all_term_set = new HashSet<Term>();
        MAXLEN = 0;
        maxlength = new int[8];
        DBConn dbconn = DBConn.getInstance();
        if (dbconn.openDB() == 1) {
            ResultSet rs1 = dbconn.doQuery(s1);
            while (rs1.next()) {
                dbconn.prepareStatement(s2);
                dbconn.setInt(1, rs1.getInt("RE_ID"));
                String cate = rs1.getString("OTHER_COURSEWARE_NAME");
                ResultSet rs2 = dbconn.doQuery();
                while (rs2.next()) {
                    Term t = new Term();
                    t.setT_id(rs2.getInt("T_ID"));
                    t.setT_name(rs2.getString("T_NAME"));
                    int len = rs2.getString("T_NAME").length();
                    all_term_set.add(t);
                    MAXLEN = (len > MAXLEN ? len : MAXLEN);

                    if ("数据结构".equals(cate)) {
                        datastructure_term_set.add(t);
                        if (len > maxlength[0]) maxlength[0] = len;
                    } else if ("操作系统".equals(cate)) {
                        os_term_set.add(t);
                        if (len > maxlength[1]) maxlength[1] = len;
                    } else if ("c语言".equals(cate)) {
                        c_term_set.add(t);
                        if (len > maxlength[2]) maxlength[2] = len;
                    } else if ("java语言".equals(cate)) {
                        java_term_set.add(t);
                        if (len > maxlength[3]) maxlength[3] = len;
                    } else if ("计算机网络".equals(cate)) {
                        network_term_set.add(t);
                        if (len > maxlength[4]) maxlength[4] = len;
                    } else if ("计算机组成原理".equals(cate)) {
                        organization_term_set.add(t);
                        if (len > maxlength[5]) maxlength[5] = len;
                    } else if ("中学几何".equals(cate)) {
                        pg_term_set.add(t);
                        if (len > maxlength[7]) maxlength[7] = len;
                    } else if ("计算机系统结构".equals(cate)
                            || "计算系系统结构".equals(cate)
                            || "计算机组成与系统结构".equals(cate)
                            ) {
                        architecture_term_set.add(t);
                        if (len > maxlength[6]) maxlength[6] = len;
                    }
                }
            }
            System.out.println("dictionary size: " + all_term_set.size() + " ,max term length: " + MAXLEN);
        }
        dbconn.closeDB();


    }

    public Set<Term> getTermSet() throws Exception {
        Set<Term> set = new HashSet<Term>();
        String s = "select * from DB2ADMIN.TERMS";
        int count = 0;
        length = new int[51];
        DBConn dbconn = DBConn.getInstance();
        if (dbconn.openDB() == 1) {
            ResultSet resultset = dbconn.doQuery(s);
            while (resultset.next()) {
                count++;
                Term t = new Term();
                t.setT_id(resultset.getInt("T_ID"));
                t.setT_name(resultset.getString("T_NAME"));
                //System.out.println(t.getT_id()+" "+t.getT_name());
                System.out.println(t.getT_name().length());
                length[t.getT_name().length()]++;
                set.add(t);
            }
            System.out.println("term with duplicate:" + count);
        }
        dbconn.closeDB();
        return set;
    }

    public int getMax_TermLength() throws Exception {
        Set<Term> tl = this.getTermSet();
        Iterator<Term> tit = tl.iterator();
        int maxlen = 0;
        int len = 0;
        while (tit.hasNext()) {
            Term t = tit.next();
            len = t.getT_name().length();
            if (maxlen < len) maxlen = len;

        }
        tl = null;
        return maxlen;
    }

    public static void main(String args[]) throws Exception {
        TermDao tdao = new TermDao();

        System.out.println("term no duplicate:" + tdao.getTermSet().size());
//        tdao.parseTermSet();
//
//        //tdao.parseTermFromHbase();
//        System.out.println("ds:" + datastructure_term_set.size() + " os:" + os_term_set.size() + " c:" + c_term_set.size() +
//                " java:" + java_term_set.size() + " network:" + network_term_set.size() + " organization:" + organization_term_set.size() +
//                " architecture:" + architecture_term_set.size() + " plane geometry:" + pg_term_set.size() + " all:" + all_term_set.size());
//        System.out.println(maxlength[0] + " " + maxlength[1] + " " + maxlength[2] + " " + maxlength[3] + " " + maxlength[4] + " " + maxlength[5] + " " + maxlength[6] + " " + maxlength[7] + " " + MAXLEN);
    }
}
