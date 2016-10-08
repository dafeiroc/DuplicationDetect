package com.dafei.datasource;

import com.dafei.bean.MetaData;
import com.dafei.bean.Term;
import com.dafei.config.DBConn;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-17
 * Time: 7:59am
 * To change this template use File | Settings | File Templates.
 */

/**
 * generate all txt from db to hive
 * see http://wiki.apache.org/hadoop/Hive/GettingStarted get more information
 */
public class Dump2HiveMain {
    public static void main(String args[]) throws Exception {
        Dump2HiveMain d2hive = new Dump2HiveMain();
        d2hive.saveTerm_and_resource_no_duplicate("e:\\metadata_raw\\t_and_r.txt");
        d2hive.saveMetadata("e:\\metadata_raw\\metadata.txt");
        d2hive.saveTerms("e:\\metadata_raw\\term.txt");
    }

    public void saveMetadata(String file) throws Exception {
        DownloadMetadataMain dm = new DownloadMetadataMain();
        List<MetaData> mdl = dm.getME_TERMList();
        java.io.File f = new java.io.File(file);
        if (f.exists()) f.delete();
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        int it_has = 0;
        int it_doesnt_have = 0;
        for (MetaData md : mdl) {


            HDFSClient hdfsc = new HDFSClient();
            String filepath = md.getRe_temp_location();
            filepath = filepath.replaceAll(" ", "_");
            filepath = filepath.replaceAll("]", "_");
            filepath = filepath.replaceAll("\\[", "_");

            // String suffix = "." + md.getFile_format();
            String suffix = ".txt";


            if (hdfsc.exists(filepath, suffix)) {
                it_has++;
                bw.write(md.getRe_id() + "\001" +
                        md.getCourseware_id() + "\001" +
                        md.getOther_courseware_name() + "\001" +
                        md.getRe_identify() + "\001" +
                        md.getIs_delete() + "\001" +
                        md.getIs_auth() + "\001" +
                        md.getIs_distribute() + "\001" +
                        md.getRe_size() + "\001" +
                        md.getRe_location() + "\001" +
                        md.getRe_temp_location() + "\001" +
                        md.getRe_upload_date() + "\001" +
                        md.getRe_upload_userid() + "\001" +
                        md.getSubject_id() + "\001" +
                        md.getMe_object() + "\001" +
                        md.getMe_education() + "\001" +
                        md.getMe_title() + "\001" +
                        md.getMe_author() + "\001" +
                        md.getMe_key() + "\001" +
                        md.getMe_abstract() + "\001" +
                        md.getMe_publisher() + "\001" +
                        md.getMe_pub_date() + "\001" +
                        md.getMe_language() + "\001" +
                        md.getMe_version() + "\001" +
                        md.getMe_copyright() + "\001" +
                        md.getMe_type() + "\001" +
                        md.getFile_type() + "\001" +
                        md.getFile_format() + "\001" +
                        md.getMe_source() + "\001" +
                        md.getMe_draw_out() + "\001" +
                        md.getXjtudlc_id()
                );
                bw.newLine();


                System.out.println(md.getRe_id() + "-------------");
            } else {
                it_doesnt_have++;
                System.out.println(md.getRe_id() + " no file in hdfs-------");
            }

        }
        bw.close();
        System.out.println(it_has + "have document," + it_doesnt_have + "no document");
    }

    public void saveTerms(String file) throws Exception {
        TermDao tdao = new TermDao();
        Set<Term> set = tdao.getTermSet();
        Iterator it = set.iterator();
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(file));
            while (it.hasNext()) {

                Term t = (Term) it.next();
                bw.write(t.getT_id() + "\001" + t.getT_name());
                bw.newLine();

            }

            //bw.flush();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void saveTerm_and_resource_no_duplicate(String file) throws Exception {
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        java.io.File f = new java.io.File(file);
        if (f.exists()) f.delete();

        String s = "select * from db2admin.term_and_resource_no_duplication";
        DBConn dbconn = DBConn.getInstance();
        if (dbconn.openDB() == 1) {
            ResultSet rs = dbconn.doQuery(s);
            while (rs.next()) {
                bw.write(rs.getInt("ID") + "\001" + rs.getInt("T_ID") + "\001" + rs.getInt("RE_ID"));
                bw.newLine();
            }
        }
        dbconn.closeDB();
        bw.close();
    }
}
