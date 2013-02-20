
package com.kilo.dao.impl.ibatis.bcp;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.springframework.orm.ibatis.support.SqlMapClientDaoSupport;

import com.kilo.dao.StageDAO;
import com.kilo.dao.StageUtils;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class BCPStageDAO extends SqlMapClientDaoSupport implements StageDAO {

    private static final String BCP_BINARY = "/usr/local/bin/freebcp";

    private static final String BULK_INSERT_ROW_SEPARATOR = "\r\n";

    private String username;

    private String password;

    private String server;

    @Override
    public StageResult stage(List<MotleyObject> records, String templateDB,
            String templateTable) {
        // Create the table from the template
        String stageTableName = StageUtils.getStageTableName(templateTable);

        Map<String, Object> stageTableCreationParamMap = new HashMap<>();
        stageTableCreationParamMap.put("templateDB", templateDB);
        stageTableCreationParamMap.put("templateTable", templateTable);
        stageTableCreationParamMap.put("stageTableName", stageTableName);
        getSqlMapClientTemplate().insert("Motley.createStageTable",
                stageTableCreationParamMap);

        StringBuffer content = new StringBuffer();
        for (MotleyObject rec : records) {
            content.append(rec.toBulkInsertString() + BULK_INSERT_ROW_SEPARATOR);
        }

        String fileName = null;
        try {
            File file = File.createTempFile(templateDB, stageTableName);
            fileName = file.getAbsolutePath();
            FileUtils.write(file, content);
        } catch (IOException exception) {
            throw new IllegalArgumentException("Unable to create bcp file",
                    exception);
        }

        String fqStageTableName = templateDB + ".." + stageTableName;
        try {
            String usernameClause = "-U" + username;
            String passwordClause = "-P" + password;
            String serverNameClause = "-S" + server;
            ProcessBuilder builder = new ProcessBuilder(BCP_BINARY,
                    fqStageTableName, "in", fileName, usernameClause,
                    passwordClause, "-c", serverNameClause);
            Process process = builder.start();
            int rc = process.waitFor();
            if (rc != 0) {
                throw new IllegalArgumentException("Unable to start process "
                        + rc + " with ");
            }
        } catch (IOException | InterruptedException exception) {
            throw new IllegalArgumentException("Unable to run bcp command",
                    exception);
        }

        // Politely cleanup
        FileUtils.deleteQuietly(new File(fileName));

        StageResult result = new StageResult();
        result.setDbName(templateDB);
        result.setTableName(stageTableName);
        return result;
    }

    @Override
    public void dropStageTable(StageResult stageResult) {
        Map<String, Object> stageParamMap = new HashMap<>();
        stageParamMap.put("stageDBName", stageResult.getDbName());
        stageParamMap.put("stageTableName", stageResult.getTableName());
        getSqlMapClientTemplate().delete("Motley.insertStageDrop",
                stageParamMap);
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setServer(String server) {
        this.server = server;
    }

}
