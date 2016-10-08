package com.dafei.datasource;

import com.dafei.bean.MetaData;
import com.dafei.config.DBConn;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-3
 * Time: 9:44pm
 * To change this template use File | Settings | File Templates.
 */
/*download all metadata raw files to the dir based on category*/
// totally 612 txt have terms,  485 used  127 not use
// totally 777 txt have ke,   394 used 383 not use
public class DownloadMetadataMain {
    private TermDao termDao;
    private MetaDataDao metaDataDao;
    private Knowledge_ElementDao keDao;
    private final String dest_dir0 = "e:/metadata_raw/";
    private final String dest_dir1 = "e:/metadata_raw/datastructure/";
    private final String dest_dir2 = "e:/metadata_raw/os/";
    private final String dest_dir3 = "e:/metadata_raw/architecture/";
    private final String dest_dir4 = "e:/metadata_raw/network/";
    private final String dest_dir5 = "e:/metadata_raw/organization/";
    private final String dest_dir6 = "e:/metadata_raw/c/";
    private final String dest_dir7 = "e:/metadata_raw/java/";
    private final String dest_dir8 = "e:/metadata_raw/plane_geometry/";
    private final String dest_all = "e:/metadata_raw/all/";

    public static void main(String args[]) throws Exception {
        DownloadMetadataMain downloadMetadataMain = new DownloadMetadataMain();
        downloadMetadataMain.downloadME_TERM();
    }

    private void downloadME_KE() throws Exception {
        List<MetaData> mdl = getME_KEList();
        int it_has = 0;
        int it_doesnt_have = 0;
        String dest_dir = "";
        System.out.println("downloading...........");
        for (MetaData md : mdl) {
            HDFSClient hdfsc = new HDFSClient();
            String filepath = md.getRe_temp_location();
            filepath = filepath.replaceAll(" ", "_");
            filepath = filepath.replaceAll("]", "_");
            filepath = filepath.replaceAll("\\[", "_");

            // String suffix = "." + md.getFile_format();
            String suffix = ".txt";
            String category = md.getOther_courseware_name();
            if ("数据结构".equals(category)) dest_dir = dest_dir1;
            else if ("操作系统".equals(category)) dest_dir = dest_dir2;
            else if ("计算机系统结构".equals(category) || "计算系系统结构".equals(category) || "计算机组成与系统结构".equals(category))
                dest_dir = dest_dir3;
            else if ("计算机网络".equals(category)) dest_dir = dest_dir4;
            else if ("计算机组成原理".equals(category)) dest_dir = dest_dir5;
            else if ("c语言".equals(category)) dest_dir = dest_dir6;
            else if ("java语言".equals(category)) dest_dir = dest_dir7;
            else if ("中学几何".equals(category)) dest_dir = dest_dir8;
            else dest_dir = dest_dir0;

            if (hdfsc.exists(filepath, suffix)) {
                it_has++;
                hdfsc.down(filepath, suffix, dest_dir);
                System.out.println(md.getRe_id() + "-------------");
            } else {
                it_doesnt_have++;
                System.out.println(md.getRe_id() + " no file in hdfs-------");
            }
        }
        System.out.println(it_has + "have document," + it_doesnt_have + "no document");
    }

    private void downloadME_TERM() throws Exception {
        List<MetaData> mdl = getME_TERMList();

        int it_has = 0;
        int it_doesnt_have = 0;
        String dest_dir = "";
        System.out.println("downloading...........");
        for (MetaData md : mdl) {


            HDFSClient hdfsc = new HDFSClient();
            String filepath = md.getRe_temp_location();
            filepath = filepath.replaceAll(" ", "_");
            filepath = filepath.replaceAll("]", "_");
            filepath = filepath.replaceAll("\\[", "_");

            // String suffix = "." + md.getFile_format();
            String suffix = ".txt";
            String category = md.getOther_courseware_name();
            if ("数据结构".equals(category)) dest_dir = dest_dir1;
            else if ("操作系统".equals(category)) dest_dir = dest_dir2;
            else if ("计算机系统结构".equals(category) || "计算系系统结构".equals(category) || "计算机组成与系统结构".equals(category))
                dest_dir = dest_dir3;
            else if ("计算机网络".equals(category)) dest_dir = dest_dir4;
            else if ("计算机组成原理".equals(category)) dest_dir = dest_dir5;
            else if ("c语言".equals(category)) dest_dir = dest_dir6;
            else if ("java语言".equals(category)) dest_dir = dest_dir7;
            else if ("中学几何".equals(category)) dest_dir = dest_dir8;
            else dest_dir = dest_dir0;
            if (hdfsc.exists(filepath, suffix)) {
                it_has++;
                hdfsc.down(filepath, suffix, dest_dir);
                hdfsc.down(filepath, suffix, this.dest_all);
                System.out.println(md.getRe_id() + "-------------");
            } else {
                it_doesnt_have++;
                System.out.println(md.getRe_id() + " no file in hdfs-------");
            }

        }

        System.out.println(it_has + "have document," + it_doesnt_have + "no document");
    }

    private List<MetaData> getME_KEList() throws Exception {
        List<MetaData> mdl = new ArrayList<MetaData>();
        String s = "select * from DB2ADMIN.METADATA where RE_ID in (select RE_ID from DB2ADMIN.KNOWLEDGE_ELEMENT_AND_RESOURCE group by RE_ID)";
        int count = 0;
        DBConn dbconn = DBConn.getInstance();
        if (dbconn.openDB() == 1) {
            ResultSet rs = dbconn.doQuery(s);
            while (rs.next()) {
                count++;
                MetaData md = new MetaData();
                md.setRe_id(rs.getInt("RE_ID"));
                md.setCourseware_id(rs.getInt("COURSEWARE_ID"));
                md.setOther_courseware_name(rs.getString("OTHER_COURSEWARE_NAME"));
                md.setRe_identify(rs.getString("RE_IDENTIFY"));
                md.setIs_delete(rs.getString("IS_DELETE"));
                md.setIs_auth(rs.getString("IS_AUTH"));
                md.setIs_distribute(rs.getString("IS_DISTRIBUTE"));
                md.setRe_size(rs.getInt("RE_SIZE"));
                md.setRe_location(rs.getString("RE_LOCATION"));
                md.setRe_temp_location(rs.getString("RE_TEMP_LOCATION"));
                md.setRe_upload_date(rs.getDate("RE_UPLOAD_DATE"));
                md.setRe_upload_userid(rs.getInt("RE_UPLOAD_USERID"));
                md.setSubject_id(rs.getInt("SUBJECT_ID"));
                md.setMe_object(rs.getString("ME_OBJECT"));
                md.setMe_education(rs.getString("ME_EDUCATION"));
                md.setMe_title(rs.getString("ME_TITLE"));
                md.setMe_author(rs.getString("ME_AUTHOR"));
                md.setMe_key(rs.getString("ME_KEY"));
                md.setMe_abstract(rs.getString("ME_ABSTRACT"));
                md.setMe_publisher(rs.getString("ME_PUBLISHER"));
                md.setMe_pub_date(rs.getDate("ME_PUB_DATE"));
                md.setMe_language(rs.getString("ME_LANGUAGE"));
                md.setMe_version(rs.getString("ME_VERSION"));
                md.setMe_copyright(rs.getString("ME_COPYRIGHT"));
                md.setMe_type(rs.getString("ME_TYPE"));
                md.setFile_type(rs.getString("FILE_TYPE"));
                md.setFile_format(rs.getString("FILE_FORMAT"));
                md.setMe_source(rs.getString("ME_SOURCE"));
                md.setMe_draw_out(rs.getString("ME_DRAW_OUT"));
                md.setXjtudlc_id(rs.getString("XJTUDLC_ID"));
                mdl.add(md);

                System.out.println(md.getRe_id() + " # " +
                        md.getCourseware_id() + " # " +
                        md.getOther_courseware_name() + " # " +
                        md.getRe_identify() + " # " +
                        md.getIs_delete() + " # " +
                        md.getIs_auth() + " # " +
                        md.getIs_distribute() + " # " +
                        md.getRe_size() + " # " +
                        md.getRe_location() + " # " +
                        md.getRe_temp_location() + " # " +
                        md.getRe_upload_date() + " # " +
                        md.getRe_upload_userid() + " # " +
                        md.getSubject_id() + " # " +
                        md.getMe_object() + " # " +
                        md.getMe_education() + " # " +
                        md.getMe_title() + " # " +
                        md.getMe_author() + " # " +
                        md.getMe_key() + " # " +
                        md.getMe_abstract() + " # " +
                        md.getMe_publisher() + " # " +
                        md.getMe_pub_date() + " # " +
                        md.getMe_language() + " # " +
                        md.getMe_version() + " # " +
                        md.getMe_copyright() + " # " +
                        md.getMe_type() + " # " +
                        md.getFile_type() + " # " +
                        md.getFile_format() + " # " +
                        md.getMe_source() + " # " +
                        md.getMe_draw_out() + " # " +
                        md.getXjtudlc_id()
                );

            }
            System.out.println(count);
        }
        dbconn.closeDB();
        return mdl;
    }

    protected List<MetaData> getME_TERMList() throws Exception {
        List<MetaData> mdl = new ArrayList<MetaData>();
        String s = "select * from DB2ADMIN.METADATA where RE_ID in (select RE_ID from DB2ADMIN.TERM_AND_RESOURCE_NO_DUPLICATION group by RE_ID)";
        int count = 0;
        DBConn dbconn = DBConn.getInstance();
        if (dbconn.openDB() == 1) {
            ResultSet rs = dbconn.doQuery(s);
            while (rs.next()) {
                count++;
                MetaData md = new MetaData();
                md.setRe_id(rs.getInt("RE_ID"));
                md.setCourseware_id(rs.getInt("COURSEWARE_ID"));
                md.setOther_courseware_name(rs.getString("OTHER_COURSEWARE_NAME"));
                md.setRe_identify(rs.getString("RE_IDENTIFY"));
                md.setIs_delete(rs.getString("IS_DELETE"));
                md.setIs_auth(rs.getString("IS_AUTH"));
                md.setIs_distribute(rs.getString("IS_DISTRIBUTE"));
                md.setRe_size(rs.getInt("RE_SIZE"));
                md.setRe_location(rs.getString("RE_LOCATION"));
                md.setRe_temp_location(rs.getString("RE_TEMP_LOCATION"));
                md.setRe_upload_date(rs.getDate("RE_UPLOAD_DATE"));
                md.setRe_upload_userid(rs.getInt("RE_UPLOAD_USERID"));
                md.setSubject_id(rs.getInt("SUBJECT_ID"));
                md.setMe_object(rs.getString("ME_OBJECT"));
                md.setMe_education(rs.getString("ME_EDUCATION"));
                md.setMe_title(rs.getString("ME_TITLE"));
                md.setMe_author(rs.getString("ME_AUTHOR"));
                md.setMe_key(rs.getString("ME_KEY"));
                md.setMe_abstract(rs.getString("ME_ABSTRACT"));
                md.setMe_publisher(rs.getString("ME_PUBLISHER"));
                md.setMe_pub_date(rs.getDate("ME_PUB_DATE"));
                md.setMe_language(rs.getString("ME_LANGUAGE"));
                md.setMe_version(rs.getString("ME_VERSION"));
                md.setMe_copyright(rs.getString("ME_COPYRIGHT"));
                md.setMe_type(rs.getString("ME_TYPE"));
                md.setFile_type(rs.getString("FILE_TYPE"));
                md.setFile_format(rs.getString("FILE_FORMAT"));
                md.setMe_source(rs.getString("ME_SOURCE"));
                md.setMe_draw_out(rs.getString("ME_DRAW_OUT"));
                md.setXjtudlc_id(rs.getString("XJTUDLC_ID"));
                mdl.add(md);

                System.out.println(md.getRe_id() + " # " +
                        md.getCourseware_id() + " # " +
                        md.getOther_courseware_name() + " # " +
                        md.getRe_identify() + " # " +
                        md.getIs_delete() + " # " +
                        md.getIs_auth() + " # " +
                        md.getIs_distribute() + " # " +
                        md.getRe_size() + " # " +
                        md.getRe_location() + " # " +
                        md.getRe_temp_location() + " # " +
                        md.getRe_upload_date() + " # " +
                        md.getRe_upload_userid() + " # " +
                        md.getSubject_id() + " # " +
                        md.getMe_object() + " # " +
                        md.getMe_education() + " # " +
                        md.getMe_title() + " # " +
                        md.getMe_author() + " # " +
                        md.getMe_key() + " # " +
                        md.getMe_abstract() + " # " +
                        md.getMe_publisher() + " # " +
                        md.getMe_pub_date() + " # " +
                        md.getMe_language() + " # " +
                        md.getMe_version() + " # " +
                        md.getMe_copyright() + " # " +
                        md.getMe_type() + " # " +
                        md.getFile_type() + " # " +
                        md.getFile_format() + " # " +
                        md.getMe_source() + " # " +
                        md.getMe_draw_out() + " # " +
                        md.getXjtudlc_id()
                );

            }
            System.out.println(count);
        }
        dbconn.closeDB();
        return mdl;
    }
}