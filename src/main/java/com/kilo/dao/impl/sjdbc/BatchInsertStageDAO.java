
package com.kilo.dao.impl.sjdbc;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.Lists;
import com.kilo.dao.StageDAO;
import com.kilo.dao.StageUtils;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class BatchInsertStageDAO extends JdbcDaoSupport implements StageDAO {

    @Override
    @Transactional
    public StageResult stage(List<MotleyObject> records, String templateDB,
            String templateTable) {
        String stageTableName = StageUtils.getStageTableName(templateTable);
        String createTableDDL = "SELECT date, name, id, price, amount, fx_rate, is_valid, knowledge_time INTO "
                + stageTableName + " FROM " + templateDB + ".." + templateTable;
        getJdbcTemplate().update(createTableDDL);

        String insertDML = "INSERT INTO "
                + stageTableName
                + "(date, name, id, price, amount, fx_rate, is_valid, knowledge_time) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        List<List<MotleyObject>> partitions = Lists.partition(records,
                batchSize);
        for (final List<MotleyObject> partition : partitions) {
            BatchPreparedStatementSetter pss = new BatchPreparedStatementSetter() {

                @Override
                public void setValues(PreparedStatement ps, int i)
                        throws SQLException {
                    MotleyObject rec = partition.get(i);
                    ps.setDate(1, new Date(rec.getDate().getTime()));
                    ps.setString(2, rec.getName());
                    ps.setInt(3, rec.getId());
                    ps.setBigDecimal(4, rec.getPrice());
                    ps.setBigDecimal(5, rec.getAmount());
                    ps.setBigDecimal(6, rec.getFxRate());
                    ps.setBoolean(7, rec.getIsValid());
                    ps.setDate(8, new Date(rec.getKnowledgeTime().getTime()));
                }

                @Override
                public int getBatchSize() {
                    return partition.size();
                }
            };
            getJdbcTemplate().batchUpdate(insertDML, pss);
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
