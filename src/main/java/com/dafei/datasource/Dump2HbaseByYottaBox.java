package com.dafei.datasource;

import com.dafei.bean.MetaData;
import com.dafei.bean.Term;
import com.dafei.config.DBConn;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.ResultSet;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-21
 * Time: 8:43am
 * To change this template use File | Settings | File Templates.
 */

/**
 * HTable is "md" and "t" in hbase master machine
 * table "md" 612 row, table "t" has 31502 row
 * <p/>
 * dump2hbase using yottabox rest service, PUT and POST are equivalent
 * if ur editor default encoding is GBK, please change to utf-8, because the hbase cluster running on the platform whose encoding is utf-8
 * PUT /<table>/<row>/<column>( : <qualifier> )? ( / <timestamp> )?
 * POST /<table>/<row>/<column>( : <qualifier> )? ( / <timestamp> )?
 * Stores cell data into the specified location. If not successful, returns appropriate HTTP error status code. If successful, returns HTTP 200 status. Set Content-Type header to text/xml for XML encoding. Set Content-Type header to application/json for json encoding. Set Content-Type header to application/octet-stream for binary encoding. When using binary encoding, optionally, set X-Timestamp header to the desired timestamp.
 * PUT and POST operations are equivalent here: Specified addresses without existing data will create new values. Specified addresses with existing data will create new versions, overwriting an existing version if all of { row, column:qualifer, timestamp } match that of the existing value.
 * % curl -H "Content-Type: text/xml" --data '[...]' http://localhost:8000/test/testrow/test:testcolumn
 * <p/>
 * HTTP/1.1 200 OK
 * Content-Length: 0
 */
public class Dump2HbaseByYottaBox {
    private static final String baseuri = "http://job-tracker:9527";
    private static final String xml_begin = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?><CellSet>";
    private static final String xml_end = "</Row></CellSet>";

    public static void main(String args[]) throws IOException {
        Dump2HbaseByYottaBox d2hyb = new Dump2HbaseByYottaBox();
        d2hyb.dumpToHbaseByYB();
    }

    public void dumpToHbaseByYB() {
        try {
            String s1 = "select * from db2admin.terms";
            String s2 = "select * from db2admin.metadata a where re_id in ( select re_id from db2admin.term_and_resource_no_duplication group by re_id)";
            DBConn dbconn = DBConn.getInstance();
            if (dbconn.openDB() == 1) {
                ResultSet rs1 = dbconn.doQuery(s1);
                while (rs1.next()) {
                    Term t = new Term();
                    t.setT_id(rs1.getInt("t_id"));
                    t.setT_name(rs1.getString("t_name"));

                    putTerm(t);
                }
                ResultSet rs2 = dbconn.doQuery(s2);
                while (rs2.next()) {
                    MetaData md = new MetaData();
                    md.setRe_id(rs2.getInt("RE_ID"));
                    md.setCourseware_id(rs2.getInt("COURSEWARE_ID"));
                    md.setOther_courseware_name(rs2.getString("OTHER_COURSEWARE_NAME"));
                    md.setRe_identify(rs2.getString("RE_IDENTIFY"));
                    md.setIs_delete(rs2.getString("IS_DELETE"));
                    md.setIs_auth(rs2.getString("IS_AUTH"));
                    md.setIs_distribute(rs2.getString("IS_DISTRIBUTE"));
                    md.setRe_size(rs2.getInt("RE_SIZE"));
                    md.setRe_location(rs2.getString("RE_LOCATION"));
                    md.setRe_temp_location(rs2.getString("RE_TEMP_LOCATION"));
                    md.setRe_upload_date(rs2.getDate("RE_UPLOAD_DATE"));
                    md.setRe_upload_userid(rs2.getInt("RE_UPLOAD_USERID"));
                    md.setSubject_id(rs2.getInt("SUBJECT_ID"));
                    md.setMe_object(rs2.getString("ME_OBJECT"));
                    md.setMe_education(rs2.getString("ME_EDUCATION"));
                    md.setMe_title(rs2.getString("ME_TITLE"));
                    md.setMe_author(rs2.getString("ME_AUTHOR"));
                    md.setMe_key(rs2.getString("ME_KEY"));
                    md.setMe_abstract(rs2.getString("ME_ABSTRACT"));
                    md.setMe_publisher(rs2.getString("ME_PUBLISHER"));
                    md.setMe_pub_date(rs2.getDate("ME_PUB_DATE"));
                    md.setMe_language(rs2.getString("ME_LANGUAGE"));
                    md.setMe_version(rs2.getString("ME_VERSION"));
                    md.setMe_copyright(rs2.getString("ME_COPYRIGHT"));
                    md.setMe_type(rs2.getString("ME_TYPE"));
                    md.setFile_type(rs2.getString("FILE_TYPE"));
                    md.setFile_format(rs2.getString("FILE_FORMAT"));
                    md.setMe_source(rs2.getString("ME_SOURCE"));
                    md.setMe_draw_out(rs2.getString("ME_DRAW_OUT"));
                    md.setXjtudlc_id(rs2.getString("XJTUDLC_ID"));

                    putMetadata(md);
                }
            }

            dbconn.closeDB();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public String HTTP_PUT(String uri, String xml) throws IOException {
        HttpClient client = new HttpClient();
        PutMethod put = new PutMethod(uri);
        RequestEntity re = new StringRequestEntity(xml);
        put.setRequestHeader("Content-Type", "text/xml;charset=utf-8");
        put.setRequestEntity(re);
        client.executeMethod(put);
        put.releaseConnection();
        return new String(put.getStatusLine() + "   " + put.getResponseBodyAsString());
    }


    public void putTerm(Term t) {
        String uri = baseuri + "/t/" + String.valueOf(t.getT_id()) + "/bull_shit";
        String xml = xml_begin +
                "<Row key=\"" + Base64.encodeBytes(Bytes.toBytes(String.valueOf(t.getT_id()))) + "\">" +
                "<Cell  timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:tname")) + "\">" + Base64.encodeBytes(Bytes.toBytes(t.getT_name())) + "</Cell>";

        try {
            String s = "select a.re_id as mdid, b.re_temp_location as uri, b.other_courseware_name as category " +
                    "from db2admin.term_and_resource_no_duplication a inner join db2admin.metadata b on a.re_id=b.re_id where t_id=?";
            DBConn dbconn = DBConn.getInstance();
            if (dbconn.openDB() == 1) {
                dbconn.prepareStatement(s);
                dbconn.setInt(1, t.getT_id());
                ResultSet rs = dbconn.doQuery();
                int i = 0;
                while (rs.next()) {
                    i++;
                    xml += "<Cell timestamp=\"" + (System.currentTimeMillis() + i) + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("md:" + String.valueOf(rs.getInt("mdid")))) + "\">" + Base64.encodeBytes(Bytes.toBytes(rs.getString("uri"))) + "</Cell>";
                }
                xml += xml_end;
            }

            System.out.println("add term " + t.getT_id() + ":" + t.getT_name() + " " + HTTP_PUT(uri, xml));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void putMetadata(MetaData md) {
        String uri = baseuri + "/md/" + String.valueOf(md.getRe_id()) + "/another_bull_shit";
        String xml = xml_begin +
                "<Row key=\"" + Base64.encodeBytes(Bytes.toBytes(String.valueOf(md.getRe_id()))) + "\">" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:uri")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getRe_temp_location())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:cate")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getOther_courseware_name())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:courseware_id")) + "\">" + Base64.encodeBytes(Bytes.toBytes(String.valueOf(md.getCourseware_id()))) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:re_identify")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getRe_identify() == null ? "" : md.getRe_identify())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:is_delete")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getIs_delete() == null ? "" : md.getIs_delete())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:is_auth")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getIs_auth() == null ? "" : md.getIs_auth())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:is_distribute")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getIs_distribute() == null ? "" : md.getIs_distribute())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:re_size")) + "\">" + Base64.encodeBytes(Bytes.toBytes(String.valueOf(md.getRe_size()))) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\"  column=\"" + Base64.encodeBytes(Bytes.toBytes("data:re_location")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getRe_location() == null ? "" : md.getRe_location())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:re_upload_date")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getRe_upload_date().toString())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:re_upload_userid")) + "\">" + Base64.encodeBytes(Bytes.toBytes(String.valueOf(md.getRe_upload_userid()))) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:subject_id")) + "\">" + Base64.encodeBytes(Bytes.toBytes(String.valueOf(md.getSubject_id()))) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:me_object")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getMe_object() == null ? "" : md.getMe_object())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:me_education")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getMe_education() == null ? "" : md.getMe_education())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:me_title")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getMe_title() == null ? "" : md.getMe_title())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:me_author")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getMe_author() == null ? "" : md.getMe_author())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:me_key")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getMe_key() == null ? "" : md.getMe_key())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:me_abstract")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getMe_abstract() == null ? "" : md.getMe_abstract())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:me_publisher")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getMe_publisher() == null ? "" : md.getMe_publisher())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:me_pub_date")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getMe_pub_date() == null ? "" : md.getMe_pub_date().toString())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:me_language")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getMe_language() == null ? "" : md.getMe_language())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:me_version")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getMe_version() == null ? "" : md.getMe_version())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:me_copyright")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getMe_copyright() == null ? "" : md.getMe_copyright())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:me_type")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getMe_type() == null ? "" : md.getMe_type())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:file_type")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getFile_type() == null ? "" : md.getFile_type())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:file_format")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getFile_format() == null ? "" : md.getFile_format())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:me_source")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getMe_source() == null ? "" : md.getMe_source())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:me_draw_out")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getMe_draw_out() == null ? "" : md.getMe_draw_out())) + "</Cell>" +
                "<Cell timestamp=\"" + System.currentTimeMillis() + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("data:xjtudlc_id")) + "\">" + Base64.encodeBytes(Bytes.toBytes(md.getXjtudlc_id() == null ? "" : md.getXjtudlc_id())) + "</Cell>";

        try {
            String s = "select a.t_id as tid, b.t_name as tname " +
                    "from db2admin.term_and_resource_no_duplication a inner join db2admin.terms b on a.t_id=b.t_id where re_id=?";

            DBConn dbconn = DBConn.getInstance();
            if (dbconn.openDB() == 1) {
                dbconn.prepareStatement(s);
                dbconn.setInt(1, md.getRe_id());
                ResultSet rs = dbconn.doQuery();
                int i = 0;
                while (rs.next()) {
                    i++;
                    xml += "<Cell timestamp=\"" + (System.currentTimeMillis() + i) + "\" column=\"" + Base64.encodeBytes(Bytes.toBytes("t:" + String.valueOf(rs.getInt("tid")))) + "\">" + Base64.encodeBytes(Bytes.toBytes(rs.getString("tname"))) + "</Cell>";

                }
                xml += xml_end;
            }

            System.out.println("add metadata " + md.getRe_id() + ":" + md.getRe_temp_location() + " " + HTTP_PUT(uri, xml));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
