package com.dafei.tools;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-30
 * Time: 9:49am
 * To change this template use File | Settings | File Templates.
 */
public class Formula {
    public static boolean ellipsoid(int x, int y, int z, int th[]) {
        return (x * x) / (th[0] * th[0]) + (y * y) / (th[1] * th[1]) + (z * z) / (th[2] * th[2]) - 1 < 0 ? true : false;
    }

    public static double tf(int freq) {
        return Math.sqrt(freq);
    }

    public static double idf(int docFreq, int numDocs) {
        return Math.log(numDocs / (docFreq + 1)) + 1;
    }

    // It assesses how well the relationship between two variables can be described using a monotonic function
    public static double spearman(double x[], double y[]) {
        return 1 - 6 * sumdiff2(x, y) / (x.length * (x.length * x.length - 1));
    }

    public static double spearman(Map<String, ? extends Number> x, Map<String, ? extends Number> y, int n) {
        return 1 - 6 * sumdiff2(x, y) / (n * (n * n - 1));
    }

    // it assesses how well the relationship between two variables can be described using a linear function
    public static double minus_pearson(double x[], double y[]) {
        double dot_product = 0.0;
        double sumx = 0.0;
        double sumy = 0.0;
        double x2 = 0.0;
        double y2 = 0.0;
        for (int i = 0; i < x.length; i++) {
            if (x[i] == 0 && y[i] == 0)
                continue;
            if (x[i] != 0 && y[i] != 0)
                dot_product += x[i] * y[i];
            if (x[i] != 0) {
                sumx += x[i];
                x2 += x[i] * x[i];
            }
            if (y[i] != 0) {
                sumy += y[i];
                y2 += y[i] * y[i];
            }
        }
        return 1 - Math.abs((x.length * dot_product - sumx * sumy) / (Math.sqrt(x.length * x2 - sumx * sumx) * Math.sqrt(x.length * y2 - sumy * sumy)));
    }

    public static double minus_pearson(Map<String, ? extends Number> x, Map<String, ? extends Number> y, int n) {
        double dot_product = 0.0;
        double sumx = 0.0;
        double sumy = 0.0;
        double x2 = 0.0;
        double y2 = 0.0;
        Set<String> setxy = new HashSet<String>();
        setxy.addAll(x.keySet());
        setxy.addAll(y.keySet());
        Iterator<String> it = setxy.iterator();
        double xi;
        double yi;
        while (it.hasNext()) {
            String key = it.next();
            xi = (null == x.get(key)) ? 0 : x.get(key).doubleValue();
            yi = (null == y.get(key)) ? 0 : y.get(key).doubleValue();
            if (xi == 0 && yi == 0)
                continue;
            if (xi != 0 && yi != 0)
                dot_product += xi * yi;
            if (xi != 0) {
                sumx += xi;
                x2 += xi * xi;
            }
            if (yi != 0) {
                sumy += yi;
                y2 += yi * yi;
            }
        }
        return 1 - Math.abs((n * dot_product - sumx * sumy) / (Math.sqrt(n * x2 - sumx * sumx) * Math.sqrt(n * y2 - sumy * sumy)));
    }

    public static double euclidean(double x[], double y[]) {
        return Math.sqrt(sumdiff2(x, y));
    }

    public static double euclidean(Map<String, ? extends Number> x, Map<String, ? extends Number> y) {
        return Math.sqrt(sumdiff2(x, y));
    }

    public static double minus_cosine(double x[], double y[]) {

        double dot_product = 0.0;
        double x2 = 0.0;
        double y2 = 0.0;
        for (int i = 0; i < x.length; i++) {
            if (x[i] == 0 && y[i] == 0)
                continue;
            if (x[i] != 0 && y[i] != 0)
                dot_product += x[i] * y[i];
            if (x[i] != 0)
                x2 += x[i] * x[i];
            if (y[i] != 0)
                y2 += y[i] * y[i];
        }
        return 1 - dot_product / (Math.sqrt(x2 * y2));
    }

    public static double minus_cosine(Map<String, ? extends Number> x, Map<String, ? extends Number> y) {
        double dot_product = 0.0;
        double x2 = 0.0;
        double y2 = 0.0;
        Set<String> setxy = new HashSet<String>();
        setxy.addAll(x.keySet());
        setxy.addAll(y.keySet());
        Iterator<String> it = setxy.iterator();
        double xi;
        double yi;
        while (it.hasNext()) {
            String key = it.next();
            xi = (null == x.get(key)) ? 0 : x.get(key).doubleValue();
            yi = (null == y.get(key)) ? 0 : y.get(key).doubleValue();
            if (xi == 0 && yi == 0)
                continue;
            if (xi != 0 && yi != 0)
                dot_product += xi * yi;
            if (xi != 0)
                x2 += xi * xi;
            if (yi != 0)
                y2 += yi * yi;
        }
        return 1 - dot_product / (Math.sqrt(x2 * y2));
    }

    private static double sumdiff2(double x[], double y[]) {
        double dis = 0.0;
        for (int i = 0; i < x.length; i++) {
            if (x[i] == 0 && y[i] == 0)
                continue;
            dis += (x[i] - y[i]) * (x[i] - y[i]);
        }
        return dis;
    }

    private static double sumdiff2(Map<String, ? extends Number> x, Map<String, ? extends Number> y) {
        double dis = 0.0;
        Set<String> setxy = new HashSet<String>();
        setxy.addAll(x.keySet());
        setxy.addAll(y.keySet());
        Iterator<String> it = setxy.iterator();
        double xi;
        double yi;
        while (it.hasNext()) {
            String key = it.next();
            xi = (null == x.get(key)) ? 0 : x.get(key).doubleValue();
            yi = (null == y.get(key)) ? 0 : y.get(key).doubleValue();
            if (xi == 0 && yi == 0) continue;
            dis += (xi - yi) * (xi - yi);
        }
        return dis;
    }
}
