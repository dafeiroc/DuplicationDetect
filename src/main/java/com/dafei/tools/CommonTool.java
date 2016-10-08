package com.dafei.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA.
 * User: dafei
 * Date: 11-3-11
 * Time: 2:13pm
 * To change this template use File | Settings | File Templates.
 */
public class CommonTool {
    public static List<String> getFileList(String strPath) {
        List<String> filelist = new ArrayList<String>();
        File dir = new File(strPath);
        File[] files = dir.listFiles();
        if (files == null)
            return null;
        for (int i = 0; i < files.length; i++) {
            if (files[i].isDirectory()) {
                getFileList(files[i].getAbsolutePath());
            } else {
                filelist.add(files[i].getAbsolutePath());
            }
        }
        return filelist;
    }

    public static String extractFileName(String filePath) {
        String fileName = null;
        String patternString = ".+[/|\\\\](.+)$";
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(filePath);
        if (matcher.matches()) fileName = matcher.group(1);
        return fileName;
    }

    public static void main(String args[]) {
        System.out.println(CommonTool.extractFileName("E:\\metadata_raw\\organization\\《计算机组成原理》本科复习指导.txt"));
        System.out.println(CommonTool.extractFileName("E:\\metadata_raw\\organization"));
        for (String s : CommonTool.getFileList("E:\\metadata_raw\\")) {
            System.out.println(s);
        }
    }
}
