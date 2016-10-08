package com.dafei.web.index;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-4-4
 * Time: ÏÂÎç2:22
 * To change this template use File | Settings | File Templates.
 */
public class IndexSample {
    String[] isbns = new String[]{"1", "2", "3", "4", "5", "6", "7", "8",
            "9", "10", "11"};

    String[] titles = new String[]{
            "Modern Art of Education",
            "Imperial Secrets of Health",
            "Tao Te Ching",
            "G.del, Escher, Bach",
            "Mindstorms",
            "Java Development with Ant",
            "JUnit in Action",
            "Lucene in Action",
            "Tapestry in Action",
            "Extreme Programming Explained",
            "The Pragmatic Programmer"};
    String[] pubmonths = new String[]{"198106", "199401", "198810",
            "197903", "198001", "200208", "200310", "200406", "199910",
            "200403", "199910"};
    String[] categories = new String[]{
            "/education/pedagogy",
            "/health/alternative/chinese",
            "/philosophy/eastern",
            "/technology/computers/ai",
            "/technology/computers/programming/education",
            "/technology/computers/programming",
            "/technology/computers/programming",
            "/technology/computers/programming",
            "/technology/computers/programming",
            "/technology/computers/programming/methodology",
            "/technology/computers/programming"};
    String[] subjects = new String[]{
            "education philosophy psychology practice Waldorf",
            "diet chinese medicine qi gong health herbs",
            "taoism chinese ideas",
            "artificial intelligence number theory mathematics music",
            "children computers powerful ideas LOGO education",
            "apache jakarta ant build tool junit java development",
            "junit unit testing mock objects",
            "lucene search programming",
            "tapestry web user interface components",
            "extreme programming agile test driven development methodology",
            "pragmatic agile methodology developer tools"};

    public void searchIndex(Directory dir) throws Exception {
        IndexSearcher searcher = new IndexSearcher(dir);
        Query query = new TermQuery(new Term("subjects", "taoism chinese ideas"));
        TopDocs docs = searcher.search(query, null, 10);
        System.out.println(docs.totalHits);

        Document first_doc = searcher.doc(docs.scoreDocs[0].doc);
        System.out.println(first_doc.get("subjects"));
    }

    public void buildIndex(Directory dir) throws Exception {

        IndexWriter writer =
                new IndexWriter(dir, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);


        for (int i = 0; i < titles.length; i++) {
            Document doc = new Document();

            doc.add(new Field("isbns", isbns[i], Field.Store.NO, Field.Index.NOT_ANALYZED));

            doc.add(new Field("titles", titles[i], Field.Store.NO, Field.Index.NOT_ANALYZED));

            doc.add(new Field("pubmonths", pubmonths[i], Field.Store.NO, Field.Index.NOT_ANALYZED));
            doc.add(new Field("categories", categories[i], Field.Store.NO, Field.Index.NOT_ANALYZED));

            doc.add(new Field("subjects", subjects[i], Field.Store.NO, Field.Index.NOT_ANALYZED));
            writer.addDocument(doc);
        }
        // writer.optimize();
        writer.close();
    }

    public static void main(String args[]) throws Exception {
        IndexSample sample = new IndexSample();
        File dir = new File("e:\\metadata_raw\\lucene_index");
        if (dir.exists())
            dir.delete();
        dir.createNewFile();
        FSDirectory directory = new SimpleFSDirectory(dir);
        sample.buildIndex(directory);
        sample.searchIndex(directory);

    }
}
