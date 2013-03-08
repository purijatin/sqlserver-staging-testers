
package com.kilo.dao.impl.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang.time.DateFormatUtils;

import com.google.common.collect.Lists;
import com.kilo.dao.StageDAO;
import com.kilo.dao.StageUtils;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class BatchInsertStageDAO implements StageDAO {

    private DataSource dataSource;

    @Override
    public StageResult stage(List<MotleyObject> records, String templateDB,
            String templateTable) {
        String stageTableName = StageUtils.getStageTableName(templateTable);
        Connection connection = null;
        boolean originalAutoCommitValue = false;
        PreparedStatement ps = null;
        try {
            connection = dataSource.getConnection();
            originalAutoCommitValue = connection.getAutoCommit();
            connection.setAutoCommit(false);
            Statement createStatement = connection.createStatement();
            String createTableDDL = "SELECT date, name, id, price, amount, fx_rate, is_valid, knowledge_time INTO "
                    + stageTableName
                    + " FROM "
                    + templateDB
                    + ".."
                    + templateTable;
            createStatement.executeUpdate(createTableDDL);
            createStatement.close();

            String insertDML = "INSERT INTO "
                    + stageTableName
                    + "(date, name, id, price, amount, fx_rate, is_valid, knowledge_time) "
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

            List<List<MotleyObject>> partitions = Lists.partition(records,
                    batchSize);
            ps = connection.prepareStatement(insertDML);
            for (final List<MotleyObject> partition : partitions) {
                for (MotleyObject rec : partition) {
                    ps.setString(1, DateFormatUtils.formatUTC(rec.getDate(),
                            "yyyyMMdd hh:mm:ss.SSS"));
                    ps.setString(2, rec.getName());
                    ps.setString(3, rec.getId().toString());
                    ps.setString(4, rec.getPrice().toPlainString());
                    ps.setString(5, rec.getAmount().toPlainString());
                    ps.setString(6, rec.getFxRate().toPlainString());
                    ps.setString(7, rec.getIsValid() ? "1" : "0");
                    ps.setString(8, DateFormatUtils.formatUTC(
                            rec.getKnowledgeTime(), "yyyyMMdd hh:mm:ss.SSS"));
                    ps.addBatch();
                }
                ps.executeBatch();
                connection.commit();
            }

        } catch (SQLException exception) {
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException exception1) {
                throw new IllegalArgumentException(
                        "Unable to rollback connection", exception);
            }
            throw new IllegalArgumentException("Unable to stage records",
                    exception);
        } finally {
            try {
                if (ps != null) {
                    ps.clearBatch();
                    ps.close();
                }
                if (connection != null) {
                    connection.setAutoCommit(originalAutoCommitValue);
                }
            } catch (SQLException exception) {
                throw new IllegalArgumentException(
                        "Unable to reset autocommit value", exception);
            }
        }

        StageResult result = new StageResult();
        result.setDbName(templateDB);
        result.setTableName(stageTableName);
        return result;
    }

    @Override
    public void dropStageTable(StageResult stageResult) {
        try {
            Connection connection = dataSource.getConnection();
            Statement createStatement = connection.createStatement();
            String dropTableDML = "DROP TABLE " + stageResult.getDbName()
                    + ".." + stageResult.getTableName();
            createStatement.executeUpdate(dropTableDML);
            createStatement.close();
        } catch (SQLException exception) {
            throw new IllegalArgumentException("Unable to drop table ",
                    exception);
        }
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

}
