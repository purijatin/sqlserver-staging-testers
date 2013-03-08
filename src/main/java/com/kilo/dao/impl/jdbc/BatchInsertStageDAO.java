
package com.kilo.dao.impl.jdbc;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

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
                    ps.setDate(1, new Date(rec.getDate().getTime()));
                    ps.setString(2, rec.getName());
                    ps.setInt(3, rec.getId());
                    ps.setBigDecimal(4, rec.getPrice());
                    ps.setBigDecimal(5, rec.getAmount());
                    ps.setBigDecimal(6, rec.getFxRate());
                    ps.setBoolean(7, rec.getIsValid());
                    ps.setDate(8, new Date(rec.getKnowledgeTime().getTime()));
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
