package com.dafei.bean;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-3
 * Time: 5:19pm
 * To change this template use File | Settings | File Templates.
 */
public class Term {
    private int t_id;
    private String t_name;

    public Term() {
    }

    public Term(String name) {
        this.t_name = name;
    }

    public Term(int id, String name) {
        this.t_id = id;
        this.t_name = name;

    }

    public String getT_name() {
        return t_name;
    }

    public void setT_name(String t_name) {
        this.t_name = t_name;
    }

    public int getT_id() {
        return t_id;
    }

    public void setT_id(int t_id) {
        this.t_id = t_id;
    }

    /*overwrite equals and hashCode*/
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o.getClass() == Term.class) {
            Term t = (Term) o;
            return t.t_name.equals(this.t_name);
        }
        return false;
    }

    public int hashCode() {
        return this.t_name.hashCode();
    }

}
