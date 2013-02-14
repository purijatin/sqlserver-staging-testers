
package com.kilo.dao.impl.ibatis;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.orm.ibatis.support.SqlMapClientDaoSupport;

import com.kilo.dao.StageDAO;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class MultiInsertStageDAO extends SqlMapClientDaoSupport implements
        StageDAO {

    @Override
    public StageResult stage(List<MotleyObject> records, String templateDB,
            String templateTable) {

        // Create the table from the template
        String stageTableName = templateTable + "_" + new Date().getTime()
                + "_" + Thread.currentThread().getId();

        Map<String, Object> stageTableCreationParamMap = new HashMap<>();
        stageTableCreationParamMap.put("templateDB", templateDB);
        stageTableCreationParamMap.put("templateTable", templateTable);
        stageTableCreationParamMap.put("stageTableName", stageTableName);
        getSqlMapClientTemplate().insert("Motley.createStageTable",
                stageTableCreationParamMap);

        // Insert into the table
        Map<String, Object> stageParamMap = new HashMap<>();
        stageParamMap.put("stageTableName", stageTableName);
        for (MotleyObject rec : records) {
            stageParamMap.put("rec", rec);
            getSqlMapClientTemplate().insert("Motley.multiInsertStage",
                    stageParamMap);
        }

        StageResult result = new StageResult();
        result.setDbName(templateDB);
        result.setTableName(stageTableName);
        return result;
    }

}
