
package com.kilo.dao.impl.mybatis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mybatis.spring.support.SqlSessionDaoSupport;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.Lists;
import com.kilo.dao.StageDAO;
import com.kilo.dao.StageUtils;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class BatchInsertStageDAO extends SqlSessionDaoSupport implements
        StageDAO {

    @Override
    @Transactional
    public StageResult stage(final List<MotleyObject> records,
            String templateDB, String templateTable) {

        // Create the table from the template
        String stageTableName = StageUtils.getStageTableName(templateTable);

        Map<String, Object> stageTableCreationParamMap = new HashMap<>();
        stageTableCreationParamMap.put("templateDB", templateDB);
        stageTableCreationParamMap.put("templateTable", templateTable);
        stageTableCreationParamMap.put("stageTableName", stageTableName);
        long st = System.currentTimeMillis();
        getSqlSession().insert(
                "com.kilo.dao.mybatis.mapper.Motley.createStageTable",
                stageTableCreationParamMap);
        final long creation = System.currentTimeMillis() - st;

        // Insert into the table

        final Map<String, Object> stageParamMap = new HashMap<>();
        stageParamMap.put("stageTableName", stageTableName);

        List<List<MotleyObject>> partitions = Lists.partition(records,
                batchSize);
        st = System.currentTimeMillis();
        for (List<MotleyObject> partition : partitions) {
            for (MotleyObject rec : partition) {
                stageParamMap.put("rec", rec);
                getSqlSession().insert(
                        "com.kilo.dao.mybatis.mapper.Motley.insertStage",
                        stageParamMap);
            }
            getSqlSession().flushStatements();
        }

        StageResult result = new StageResult();
        result.setDbName(templateDB);
        result.setTableName(stageTableName);
        result.setDbInnerTime(System.currentTimeMillis() - st);
        result.setTableCreationTime(creation);
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

}
