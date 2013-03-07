
package com.kilo.dao.impl.ibatis;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.orm.ibatis.SqlMapClientCallback;
import org.springframework.orm.ibatis.support.SqlMapClientDaoSupport;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.Lists;
import com.ibatis.sqlmap.client.SqlMapExecutor;
import com.kilo.dao.StageDAO;
import com.kilo.dao.StageUtils;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class BatchInsertStageDAO extends SqlMapClientDaoSupport implements
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
        getSqlMapClientTemplate().insert("Motley.createStageTable",
                stageTableCreationParamMap);

        // Insert into the table

        final Map<String, Object> stageParamMap = new HashMap<>();
        stageParamMap.put("stageTableName", stageTableName);

        SqlMapClientCallback<Object> action = new SqlMapClientCallback<Object>() {

            @Override
            public Object doInSqlMapClient(SqlMapExecutor executor)
                    throws SQLException {

                int updates = 0;
                List<List<MotleyObject>> partitions = Lists.partition(records,
                        batchSize);
                for (List<MotleyObject> partition : partitions) {
                    executor.startBatch();
                    for (MotleyObject rec : partition) {
                        stageParamMap.put("rec", rec);
                        executor.insert("Motley.insertStage", stageParamMap);
                    }
                    int update = executor.executeBatch();
                    updates = updates + update;
                }
                return Integer.valueOf(updates);
            }
        };
        getSqlMapClientTemplate().execute(action);

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

}
