
package com.kilo.domain;

public class StageResult {

    private String dbName;

    private String tableName;

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
