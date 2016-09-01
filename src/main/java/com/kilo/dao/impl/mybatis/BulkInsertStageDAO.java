
package com.kilo.dao.impl.mybatis;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.apache.commons.io.FileUtils;
import org.mybatis.spring.support.SqlSessionDaoSupport;
import org.springframework.transaction.annotation.Transactional;

import com.kilo.dao.StageDAO;
import com.kilo.dao.StageUtils;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class BulkInsertStageDAO extends SqlSessionDaoSupport implements
        StageDAO {

    private String uncPathPrefix;

    private String dirPath;

    public static int count = 200;
    static CyclicBarrier barrier ;

    static synchronized void set(){
        if(barrier==null){
            barrier = new CyclicBarrier(count);
            System.out.println("Started cyclie barrier for "+count);
        }
    }

    @Override
    @Transactional
    public StageResult stage(List<MotleyObject> records, String templateDB,
            String templateTable) {
        set();
        // Create the table from the template
        String stageTableName = StageUtils.getStageTableName(templateTable);

        Map<String, Object> stageTableCreationParamMap = new HashMap<>();
        stageTableCreationParamMap.put("templateDB", templateDB);
        stageTableCreationParamMap.put("templateTable", templateTable);
        stageTableCreationParamMap.put("stageTableName", stageTableName);
        getSqlSession().insert(
                "com.kilo.dao.mybatis.mapper.Motley.createStageTable",
                stageTableCreationParamMap);

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

        try {
            barrier.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }

        // Use T-SQL to bulk insert
        Map<String, Object> stageParamMap = new HashMap<>();
        stageParamMap.put("stageTableName", stageTableName);
        String fileUNCPath = uncPathPrefix
                + file.getAbsolutePath().replace(File.separatorChar, '\\');
        stageParamMap.put("fileUNCPath", fileUNCPath);
        getSqlSession().insert(
                "com.kilo.dao.mybatis.mapper.Motley.bulkInsertStage",
                stageParamMap);

        // Politely cleanup
        FileUtils.deleteQuietly(file);

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
