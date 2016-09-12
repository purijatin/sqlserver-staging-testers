
package com.kilo.dao.impl.mybatis;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.mybatis.spring.support.SqlSessionDaoSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import com.kilo.dao.StageDAO;
import com.kilo.dao.StageUtils;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class BulkInsertStageDAO extends SqlSessionDaoSupport implements
        StageDAO {

    private String uncPathPrefix;

    private String dirPath;


    @Autowired
    JdbcTemplate jdbcTemplate;


    public static int count = 200;
    static CyclicBarrier barrier ;

    static java.util.concurrent.atomic.AtomicLong time = new java.util.concurrent.atomic.AtomicLong(0);

    @Override
    @Transactional
    public StageResult stage(List<MotleyObject> records, String templateDB,String templateTable) {
        // Create the table from the template
        String stageTableName = StageUtils.getStageTableName(templateTable);

        Map<String, Object> stageTableCreationParamMap = new HashMap<>();
        stageTableCreationParamMap.put("templateDB", templateDB);
        stageTableCreationParamMap.put("templateTable", templateTable);
        stageTableCreationParamMap.put("stageTableName", stageTableName);
        String create = "create table "+stageTableName+" (date DATETIME, name VARCHAR(100), id INT, price INT, amount INT, fx_rate INT, is_valid BIT, knowledge_time DATETIME);";
        long st = System.currentTimeMillis();
        jdbcTemplate.update(create);
        final long creation = System.currentTimeMillis() - st;
        final long bodyL = System.currentTimeMillis();
        StringBuilder content = new StringBuilder(1024*512);
//        records.stream().parallel().map(MotleyObject::toBulkInsertString).collect(Collectors.joining("BULK_INSERT_ROW_SEPARATOR"));
        for (MotleyObject rec : records) {
            content.append(rec.toBulkInsertString()).append(BULK_INSERT_ROW_SEPARATOR);
        }
        final long bodyEnd = System.currentTimeMillis() - bodyL;

        File file;
        st = System.currentTimeMillis();
        try {
            File dir = new File(dirPath);
            file = File.createTempFile(templateDB, stageTableName, dir);
            FileUtils.write(file, content);
        } catch (IOException exception) {
            throw new IllegalArgumentException(exception);
        }
        final long networkTime = System.currentTimeMillis() - st;

        // Use T-SQL to bulk insert
        Map<String, Object> stageParamMap = new HashMap<>();
        final long temp = System.currentTimeMillis();
        stageParamMap.put("stageTableName", stageTableName);
        String fileUNCPath = "\\\\balysandboxdb1a\\pod\\importexport\\"+file.getName();
        stageParamMap.put("fileUNCPath", fileUNCPath);
        getSqlSession().insert(
                "com.kilo.dao.mybatis.mapper.Motley.bulkInsertStage",
                stageParamMap);
        final long dbInner = System.currentTimeMillis() - temp;
        FileUtils.deleteQuietly(file);

        StageResult result = new StageResult();
        result.setDbName(templateDB);
        result.setTableName(stageTableName);
        result.setDbInnerTime(dbInner);
        result.setNetworkTime(networkTime);
        result.setTableCreationTime(creation);
        result.setOther(bodyEnd);
        return result;
    }

    @Override
    public void dropStageTable(StageResult stageResult) {
        Map<String, Object> stageParamMap = new HashMap<>();
        stageParamMap.put("stageDBName", stageResult.getDbName());
        stageParamMap.put("stageTableName", stageResult.getTableName());
        getSqlSession().delete(
                "com.kilo.dao.mybatis.mapper.Motley.insertStageDrop",
                stageParamMap);
    }

    public void setUncPathPrefix(String uncPathPrefix) {
        this.uncPathPrefix = uncPathPrefix;
    }

    public void setDirPath(String dirPath) {
        this.dirPath = dirPath;
    }

}
