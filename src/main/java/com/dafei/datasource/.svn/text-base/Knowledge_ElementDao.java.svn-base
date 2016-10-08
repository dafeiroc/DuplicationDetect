package com.dafei.datasource;

import com.dafei.bean.KnowledgeElement;
import com.dafei.config.DBConn;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-3
 * Time: 9:45am
 * To change this template use File | Settings | File Templates.
 */
// 17340 kownledge element
public class Knowledge_ElementDao {

    public List<KnowledgeElement> getKEList() throws Exception {
        List<KnowledgeElement> l = new ArrayList<KnowledgeElement>();
        String s = "select * from DB2ADMIN.KNOWLEDGE_ELEMENT";
        int count = 0;
        DBConn dbconn = DBConn.getInstance();
        if (dbconn.openDB() == 1) {
            ResultSet rs = dbconn.doQuery(s);
            while (rs.next()) {
                count++;
                KnowledgeElement ke = new KnowledgeElement();
                ke.setE_id(rs.getInt("E_ID"));
                ke.setContext(rs.getString("CONTEXT"));
                ke.setKnowledge_name(rs.getString("KNOWLEDGE_NAME"));
                l.add(ke);

                System.out.println(
                        ke.getE_id() + " " +
                                ke.getKnowledge_name() + " " +
                                ke.getContext() + " "
                );

            }
            System.out.println(count);
        }
        dbconn.closeDB();
        return l;
    }

    public static void main(String args[]) throws Exception {
        Knowledge_ElementDao kedao = new Knowledge_ElementDao();
        kedao.getKEList();
    }
}
