
package com.kilo.domain;

import java.util.OptionalLong;

public class StageResult {

    private String dbName;

    private String tableName;

    public OptionalLong networkTime = OptionalLong.empty();
    public OptionalLong dbInnerTime = OptionalLong.empty();
    public OptionalLong tableCreationTime = OptionalLong.empty();
    public OptionalLong tableDropTime = OptionalLong.empty();
    public OptionalLong other = OptionalLong.empty();
    public long totalTime=-1L;

    public void setOther(long other) {
        this.other = OptionalLong.of(other);
    }

    public long getTotalTime() {
        return totalTime;
    }

    public void setTotalTime(long totalTime) {
        this.totalTime = totalTime;
    }

    public OptionalLong getNetworkTime() {
        return networkTime;
    }

    public void setNetworkTime(long networkTime) {
        this.networkTime = OptionalLong.of(networkTime);
    }

    public OptionalLong getDbInnerTime() {
        return dbInnerTime;
    }

    public void setTableCreationTime(long tableCreationTime) {
        this.tableCreationTime = OptionalLong.of(tableCreationTime);
    }

    public void setDbInnerTime(long dbInnerTime) {
        this.dbInnerTime = OptionalLong.of(dbInnerTime);
    }

    public void setTableDropTime(long tableDropTime) {
        this.tableDropTime = OptionalLong.of(tableDropTime);
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "StageResult [dbName=" + dbName + ", tableName=" + tableName
                + "]";
    }
}
