package com.dafei.datasource;

import com.dafei.bean.MetaData;
import com.dafei.config.DBConn;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 10-12-20
 * Time: 2:02pm
 * To change this template use File | Settings | File Templates.
 */

//  31475  text file (txt doc docx pdf rdf ppt pptx case insensitive)
public class MetaDataDao {


    public List<MetaData> getTxtMetaDataList() throws Exception {
        List<MetaData> mdl = new ArrayList<MetaData>();
        String s = "select * from DB2ADMIN.METADATA where FILE_FORMAT='doc' or " +
                "FILE_FORMAT='DOC' or " +
                "FILE_FORMAT='txt' or " +
                "FILE_FORMAT='TXT' or " +
                "FILE_FORMAT='pdf' or " +
                "FILE_FORMAT='PDF' or " +
                "FILE_FORMAT='ppt' or " +
                "FILE_FORMAT='PPT' or " +
                "FILE_FORMAT='docx' or " +
                "FILE_FORMAT='DOCX' or " +
                "FILE_FORMAT='pptx' or " +
                "FILE_FORMAT='PPTX' or " +
                "FILE_FORMAT='rdf' or " +
                "FILE_FORMAT='RDF'";
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

    public void dumpToMetaDataRaw(MetaData md) throws Exception {
        String sql = "insert into METADATA_RAW(RE_ID,COURSEWARE_ID,OTHER_COURSEWARE_NAME," +
                "RE_IDENTIFY,IS_DELETE,IS_AUTH,IS_DISTRIBUTE,RE_SIZE,RE_LOCATION,RE_TEMP_LOCATION," +
                "RE_UPLOAD_DATE,RE_UPLOAD_USERID,SUBJECT_ID,ME_OBJECT,ME_EDUCATION,ME_TITLE,ME_AUTHOR,ME_KEY,ME_ABSTRACT,ME_PUBLISHER," +
                "ME_PUB_DATE,ME_LANGUAGE,ME_VERSION,ME_COPYRIGHT,ME_TYPE,FILE_TYPE,FILE_FORMAT," +
                "ME_SOURCE,ME_DRAW_OUT,XJTUDLC_ID) values (default," +
                md.getCourseware_id() + "," +
                "'" + md.getOther_courseware_name() + "'," +
                "'" + md.getRe_identify() + "'," +
                md.getIs_delete() + "," +
                md.getIs_auth() + "," +
                md.getIs_distribute() + "," +
                md.getRe_size() + "," +
                "'" + md.getRe_location() + "'," +
                "'" + md.getRe_temp_location() + "'," +
                "CURRENT_DATE," +
                md.getRe_upload_userid() + "," +
                md.getSubject_id() + "," +
                "'" + md.getMe_object() + "'," +
                "'" + md.getMe_education() + "'," +
                "'" + md.getMe_title() + "'," +
                "'" + md.getMe_author() + "'," +
                "'" + md.getMe_key() + "'," +
                "'" + md.getMe_abstract() + "'," +
                "'" + md.getMe_publisher() + "'," +
                "CURRENT_DATE," +
                "'" + md.getMe_language() + "'," +
                "'" + md.getMe_version() + "'," +
                "'" + md.getMe_copyright() + "'," +
                "'" + md.getMe_type() + "'," +
                "'" + md.getFile_type() + "'," +
                "'" + md.getFile_format() + "'," +
                "'" + md.getMe_source() + "'," +
                md.getMe_draw_out() +
                "'" + md.getXjtudlc_id() + "')";
        DBConn dbconn = DBConn.getInstance();
        if (dbconn.openDB() == 1) {
            dbconn.doUpdate(sql);

        }
        dbconn.closeDB();
    }

    public static void main(String args[]) throws Exception {
        MetaDataDao mddao = new MetaDataDao();
        mddao.getTxtMetaDataList();
    }

}
