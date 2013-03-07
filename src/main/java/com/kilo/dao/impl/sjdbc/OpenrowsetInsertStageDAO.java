
package com.kilo.dao.impl.sjdbc;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.transaction.annotation.Transactional;

import com.kilo.dao.StageDAO;
import com.kilo.dao.StageUtils;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class OpenrowsetInsertStageDAO extends JdbcDaoSupport implements
        StageDAO {

    private String uncPathPrefix;

    private String dirPath;

    private String formatFilePath;

    @Override
    @Transactional
    public StageResult stage(List<MotleyObject> records, String templateDB,
            String templateTable) {
        String stageTableName = StageUtils.getStageTableName(templateTable);
        String createTableDDL = "SELECT date, name, id, price, amount, fx_rate, is_valid, knowledge_time INTO "
                + stageTableName + " FROM " + templateDB + ".." + templateTable;
        getJdbcTemplate().update(createTableDDL);

        StringBuilder content = new StringBuilder();
        for (MotleyObject rec : records) {
            content.append(rec.toBulkInsertString() + BULK_INSERT_ROW_SEPARATOR);
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

        // Use T-SQL to insert via OPENROWSET
        String fileUNCPath = uncPathPrefix
                + file.getAbsolutePath().replace(File.separatorChar, '\\');
        String formatFileUNCPath = uncPathPrefix
                + formatFilePath.replace(File.separatorChar, '\\');
        String openrowsetInsertDML = "INSERT INTO "
                + stageTableName
                + " SELECT date, name, id, price, amount, fx_rate, is_valid, knowledge_time FROM OPENROWSET( BULK '"
                + fileUNCPath + "', FORMATFILE='" + formatFileUNCPath
                + "') AS a";
        getJdbcTemplate().update(openrowsetInsertDML);

        // Politely cleanup
        FileUtils.deleteQuietly(file);

        StageResult result = new StageResult();
        result.setDbName(templateDB);
        result.setTableName(stageTableName);
        return result;
    }

    @Override
    public void dropStageTable(StageResult stageResult) {
        getJdbcTemplate().execute(
                "DROP TABLE " + stageResult.getDbName() + ".."
                        + stageResult.getTableName());
    }

    public void setUncPathPrefix(String uncPathPrefix) {
        this.uncPathPrefix = uncPathPrefix;
    }

    public void setDirPath(String dirPath) {
        this.dirPath = dirPath;
    }

    public void setFormatFilePath(String formatFilePath) {
        this.formatFilePath = formatFilePath;
    }
}
