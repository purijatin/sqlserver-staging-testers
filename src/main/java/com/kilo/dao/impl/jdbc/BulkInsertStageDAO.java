
package com.kilo.dao.impl.jdbc;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;

import com.kilo.dao.StageDAO;
import com.kilo.dao.StageUtils;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class BulkInsertStageDAO implements StageDAO {

    private String uncPathPrefix;

    private String dirPath;

    private DataSource dataSource;

    @Override
    public StageResult stage(List<MotleyObject> records, String templateDB,
            String templateTable) {
        String stageTableName = StageUtils.getStageTableName(templateTable);
        try {
            Connection connection = dataSource.getConnection();
            Statement createStatement = connection.createStatement();
            String createTableDDL = "SELECT date, name, id, price, amount, fx_rate, is_valid, knowledge_time INTO "
                    + stageTableName
                    + " FROM "
                    + templateDB
                    + ".."
                    + templateTable;
            createStatement.executeUpdate(createTableDDL);
            createStatement.close();

            StringBuffer content = new StringBuffer();
            for (MotleyObject rec : records) {
                content.append(rec.toBulkInsertString()
                        + BULK_INSERT_ROW_SEPARATOR);
            }

            File file = null;
            try {
                File dir = new File(dirPath);
                file = File.createTempFile(templateDB, stageTableName, dir);
                FileUtils.write(file, content);
            } catch (IOException exception) {
                throw new IllegalArgumentException("Unable to create bcp file",
                        exception);
            }

            // Use T-SQL to bulk insert
            String fileUNCPath = uncPathPrefix
                    + file.getAbsolutePath().replace(File.separatorChar, '\\');
            Statement bulkInsertStmt = connection.createStatement();
            String bulkInsertDML = "BULK INSERT " + stageTableName + " FROM '"
                    + fileUNCPath + "'";
            bulkInsertStmt.executeUpdate(bulkInsertDML);
            bulkInsertStmt.close();

        } catch (SQLException exception) {
            throw new IllegalArgumentException("Unable to stage records",
                    exception);
        }

        StageResult result = new StageResult();
        result.setDbName(templateDB);
        result.setTableName(stageTableName);
        return result;
    }

    @Override
    public void dropStageTable(StageResult stageResult) {
        try {
            Connection connection = dataSource.getConnection();
            Statement createStatement = connection.createStatement();
            String dropTableDML = "DROP TABLE " + stageResult.getDbName()
                    + ".." + stageResult.getTableName();
            createStatement.executeUpdate(dropTableDML);
            createStatement.close();
        } catch (SQLException exception) {
            throw new IllegalArgumentException("Unable to drop table ",
                    exception);
        }
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setUncPathPrefix(String uncPathPrefix) {
        this.uncPathPrefix = uncPathPrefix;
    }

    public void setDirPath(String dirPath) {
        this.dirPath = dirPath;
    }

}
