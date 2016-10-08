package com.dafei.web.search;

import org.apache.commons.fileupload.ProgressListener;

import javax.servlet.http.HttpSession;

/**
 * Created by IntelliJ IDEA.
 * User: Administrator
 * Date: 11-4-7
 * Time: ÏÂÎç7:49
 * To change this template use File | Settings | File Templates.
 */
public class UploadProgress implements ProgressListener {
    private HttpSession session;
    private UploadStatus status = new UploadStatus();

    public UploadProgress(HttpSession session) {
        this.session = session;
    }

    public void update(long pBytesRead, long pContentLength, int pItems) {
        status.setReadSize(pBytesRead);
        status.setTotalSize(pContentLength);
        status.setNum(pItems);
        session.setAttribute("status", status);
    }
}
