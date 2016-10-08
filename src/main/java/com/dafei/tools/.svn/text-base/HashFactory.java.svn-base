package com.dafei.tools;

import com.dafei.bean.Term;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-25
 * Time: 9:37pm
 * To change this template use File | Settings | File Templates.
 */
public class HashFactory {
    private HashFactory() {
    }

    public enum HashType {
        SIMPLEGBK, POLYNOMIAL, MURMUR
    }

    public static IHashFunction createHashFunctions(HashType type) {
        IHashFunction hashFunction = null;
        switch (type) {
            case SIMPLEGBK:
                hashFunction = new SimpleGBKHash();
                break;
            case POLYNOMIAL:

                break;
            case MURMUR:

                break;
        }
        return hashFunction;
    }

    static class SimpleGBKHash implements IHashFunction {
        public Map<String, String> termToHash(Set<Term> termSet) {
            Map<String, String> term_to_hash = new HashMap<String, String>();
            for (Term t : termSet) {
                term_to_hash.put(t.getT_name(), hash(t.getT_name()));
            }
            return term_to_hash;
        }

        public String hash(String term) {
            byte[] b = null;
            StringBuilder sb = new StringBuilder();
            try {
                b = term.getBytes("gbk");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            for (int i = 0; i < b.length; i++)
                sb.append(Integer.toBinaryString(b[i] & 0xff));
            return sb.toString();
        }

    }
}
