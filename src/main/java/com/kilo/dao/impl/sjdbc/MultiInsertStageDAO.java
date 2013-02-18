
package com.kilo.dao.impl.sjdbc;

import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.kilo.dao.StageDAO;
import com.kilo.dao.StageUtils;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class MultiInsertStageDAO extends JdbcDaoSupport implements StageDAO {

    @Override
    public StageResult stage(List<MotleyObject> records, String templateDB,
            String templateTable) {
        String stageTableName = StageUtils.getStageTableName(templateTable);
        String createTableDDL = "SELECT date, name, id, price, amount, fx_rate, is_valid, knowledge_time INTO "
                + stageTableName + " FROM " + templateDB + ".." + templateTable;
        getJdbcTemplate().update(createTableDDL);

        for (MotleyObject rec : records) {
            List<Object> param = new ArrayList<>();
            param.add(rec.getDate());
            param.add(rec.getName());
            param.add(rec.getId());
            param.add(rec.getPrice());
            param.add(rec.getAmount());
            param.add(rec.getFxRate());
            param.add(rec.getIsValid());
            param.add(rec.getKnowledgeTime());
            String insertDML = "INSERT INTO "
                    + stageTableName
                    + "(date, name, id, price, amount, fx_rate, is_valid, knowledge_time) "
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            getJdbcTemplate().update(insertDML, param.toArray());
        }

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
}
