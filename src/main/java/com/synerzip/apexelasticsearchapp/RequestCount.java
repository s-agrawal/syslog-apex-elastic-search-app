package com.synerzip.apexelasticsearchapp;

/**
 * Created by Saurabh Agrawal <saurabh.agrawal@synerzip.com> on 27/6/16.
 */
public class RequestCount {
    private int allowedRequestCount;
    private int deniedRequestCount;

    public RequestCount() {
        this.allowedRequestCount = 0;
        this.deniedRequestCount = 0;
    }

    public RequestCount(int allowedRequestCount, int deniedRequestCount) {
        this.allowedRequestCount = allowedRequestCount;
        this.deniedRequestCount = deniedRequestCount;
    }

    public int getAllowedRequestCount() {
        return this.allowedRequestCount;
    }

    public int getDeniedRequestCount() {
        return this.deniedRequestCount;
    }

    public void setAllowedRequestCount(int allowedRequestCount) {
        this.allowedRequestCount = allowedRequestCount;
    }

    public void setDeniedRequestCount(int deniedRequestCount) {
        this.deniedRequestCount = deniedRequestCount;
    }
}
