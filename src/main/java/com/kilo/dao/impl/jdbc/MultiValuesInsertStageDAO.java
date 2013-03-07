
package com.kilo.dao.impl.jdbc;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.springframework.transaction.annotation.Transactional;

import com.kilo.dao.StageDAO;
import com.kilo.dao.StageUtils;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class MultiValuesInsertStageDAO implements StageDAO {

    private DataSource dataSource;

    @Override
    @Transactional
    public StageResult stage(List<MotleyObject> records, String templateDB,
            String templateTable) {
        String stageTableName = StageUtils.getStageTableName(templateTable);
        try {
            Connection connection = dataSource.getConnection();
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
                    + " VALUES ";
            String params = StringUtils.repeat("(?, ?, ?, ?, ?, ?, ?, ?)", ",",
                    records.size());
            PreparedStatement ps = connection.prepareStatement(insertDML
                    + params);
            int i = 1;
            for (MotleyObject rec : records) {
                ps.setDate(i++, new Date(rec.getDate().getTime()));
                ps.setString(i++, rec.getName());
                ps.setInt(i++, rec.getId());
                ps.setBigDecimal(i++, rec.getPrice());
                ps.setBigDecimal(i++, rec.getAmount());
                ps.setBigDecimal(i++, rec.getFxRate());
                ps.setBoolean(i++, rec.getIsValid());
                ps.setDate(i++, new Date(rec.getKnowledgeTime().getTime()));
            }
            ps.execute();
            ps.close();

        } catch (SQLException exception) {
            throw new IllegalArgumentException("Unable to stage records",
                    exception);
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
