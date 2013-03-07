
package com.kilo.dao.impl.sjdbc;

import java.util.List;

import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.transaction.annotation.Transactional;

import com.kilo.dao.StageDAO;
import com.kilo.dao.StageUtils;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class XMLShredderInsertStageDAO extends JdbcDaoSupport implements
        StageDAO {

    @Override
    @Transactional
    public StageResult stage(List<MotleyObject> records, String templateDB,
            String templateTable) {
        String stageTableName = StageUtils.getStageTableName(templateTable);
        String createTableDDL = "SELECT date, name, id, price, amount, fx_rate, is_valid, knowledge_time INTO "
                + stageTableName + " FROM " + templateDB + ".." + templateTable;
        getJdbcTemplate().update(createTableDDL);

        StringBuilder content = new StringBuilder();
        for (MotleyObject rec : records) {
            content.append(rec.toXMLInsertString());
        }

        // Use T-SQL to bulk XML insert
        String xmlInsertDML = "DECLARE @xml XML SET @xml = ? "
                + " INSERT INTO "
                + stageTableName
                + " SELECT row.col.value('(d/text())[1]', 'datetime') AS date,"
                + "row.col.value('(n/text())[1]', 'varchar') AS name, "
                + "row.col.value('(i/text())[1]', 'integer') AS id, "
                + "row.col.value('(p/text())[1]', 'numeric(18,4)') AS price, "
                + "row.col.value('(a/text())[1]', 'numeric(18,4)') AS amount, "
                + "row.col.value('(f/text())[1]', 'numeric(18,7)') AS fx_rate, "
                + "row.col.value('(v/text())[1]', 'tinyint') AS is_valid, "
                + "row.col.value('(k/text())[1]', 'datetime') AS knowledge_time "
                + "FROM @xml.nodes('//r') row(col) ";
        getJdbcTemplate().update(xmlInsertDML,
                new Object[] { content.toString() });

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
