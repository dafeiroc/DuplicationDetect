package com.dafei.web.search;

/**
 * Created by IntelliJ IDEA.
 * User: Administrator
 * Date: 11-4-7
 * Time: ����7:51
 * To change this template use File | Settings | File Templates.
 */
public class UploadStatus {
    private long totalSize; // �ܼƴ�С
    private long readSize; // �Ѷ���С
    private int num; // �ڼ����ļ�

    public UploadStatus() {

    }

    public UploadStatus(long totalSize, long readSize, int num) {

        this.totalSize = totalSize;
        this.readSize = readSize;
        this.num = num;
    }

    @Override
    public String toString() {
        return "{totalSize:" + totalSize + ",readSize:" + readSize + ",num:"
                + num + "}";
    }

    public long getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(long totalSize) {
        this.totalSize = totalSize;
    }

    public long getReadSize() {
        return readSize;
    }

    public void setReadSize(long readSize) {
        this.readSize = readSize;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}
