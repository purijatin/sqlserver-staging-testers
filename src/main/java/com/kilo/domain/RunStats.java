package com.kilo.domain;

import java.util.Date;
import java.util.OptionalLong;

/**
 * Created by puri on 9/12/16.
 */
public class RunStats {
    private final int runID;
    private final String runType;
    private final boolean success;
    private final Date startTime;
    private final Date endTime;
    private final long timeTaken;
    public OptionalLong networkTime = OptionalLong.empty();
    public OptionalLong dbInnerTime = OptionalLong.empty();
    public OptionalLong tableCreationTime = OptionalLong.empty();
    public OptionalLong tableDropTime = OptionalLong.empty();

    public void setNetworkTime(OptionalLong networkTime) {
        this.networkTime = networkTime;
    }

    public void setDbInnerTime(OptionalLong dbInnerTime) {
        this.dbInnerTime = dbInnerTime;
    }

    public void setTableCreationTime(OptionalLong tableCreationTime) {
        this.tableCreationTime = tableCreationTime;
    }

    public void setTableDropTime(OptionalLong tableDropTime) {
        this.tableDropTime = tableDropTime;
    }

    public RunStats(int runID, String runType, boolean success, Date startTime, Date endTime, long timeTaken) {
        this.runID = runID;
        this.runType = runType;
        this.success = success;
        this.startTime = startTime;
        this.endTime = endTime;
        this.timeTaken = timeTaken;
    }

    public long getTimeTaken() {
        return timeTaken;
    }

    @Override
    public String toString() {
        return "RunStats{" +
                "runID=" + runID +
                ", runType='" + runType + '\'' +
                ", success=" + success +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", timeTaken=" + timeTaken +
                '}';
    }

    public int getRunID() {
        return runID;
    }

    public String getRunType() {
        return runType;
    }

    public boolean isSuccess() {
        return success;
    }

    public Date getStartTime() {
        return startTime;
    }

    public Date getEndTime() {
        return endTime;
    }
}
