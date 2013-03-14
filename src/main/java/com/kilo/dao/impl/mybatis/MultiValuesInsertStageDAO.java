
package com.kilo.dao.impl.mybatis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.orm.ibatis.support.SqlMapClientDaoSupport;
import org.springframework.transaction.annotation.Transactional;

import com.kilo.dao.StageDAO;
import com.kilo.dao.StageUtils;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class MultiValuesInsertStageDAO extends SqlMapClientDaoSupport implements
        StageDAO {

    @Override
    @Transactional
    public StageResult stage(List<MotleyObject> records, String templateDB,
            String templateTable) {

        // Create the table from the template
        String stageTableName = StageUtils.getStageTableName(templateTable);

        Map<String, Object> stageTableCreationParamMap = new HashMap<>();
        stageTableCreationParamMap.put("templateDB", templateDB);
        stageTableCreationParamMap.put("templateTable", templateTable);
        stageTableCreationParamMap.put("stageTableName", stageTableName);
        getSqlMapClientTemplate().insert(
                "com.kilo.dao.mybatis.mapper.Motley.createStageTable",
                stageTableCreationParamMap);

        // Insert into the table
        Map<String, Object> stageParamMap = new HashMap<>();
        stageParamMap.put("stageTableName", stageTableName);
        stageParamMap.put("records", records);
        getSqlMapClientTemplate().insert(
                "com.kilo.dao.mybatis.mapper.Motley.multiValuesInsertStage",
                stageParamMap);

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
        getSqlMapClientTemplate().delete(
                "com.kilo.dao.mybatis.mapper.Motley.insertStageDrop",
                stageParamMap);
    }

}
