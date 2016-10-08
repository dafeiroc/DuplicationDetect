package com.dafei.web.search;

/**
 * Created by IntelliJ IDEA.
 * User: Administrator
 * Date: 11-4-7
 * Time: 下午7:51
 * To change this template use File | Settings | File Templates.
 */
public class UploadStatus {
    private long totalSize; // 总计大小
    private long readSize; // 已读大小
    private int num; // 第几个文件

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
