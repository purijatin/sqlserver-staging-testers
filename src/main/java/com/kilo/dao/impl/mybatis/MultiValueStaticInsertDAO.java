package com.kilo.dao.impl.mybatis;


import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mybatis.spring.support.SqlSessionDaoSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.Lists;
import com.kilo.dao.StageDAO;
import com.kilo.dao.StageUtils;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class MultiValueStaticInsertDAO extends SqlSessionDaoSupport implements StageDAO {

    @Autowired
    private JdbcTemplate jdbcTemplate;


    @Override
    @Transactional
    public StageResult stage(List<MotleyObject> records, String templateDB, String templateTable) {
        String stageTableName = StageUtils.getStageTableName(templateTable);

        try {
            Statement statement = getSqlSession().getConnection().createStatement();
            String create = " SELECT date, name, id, price, amount, fx_rate, is_valid, knowledge_time\n" +
                    "          INTO "+stageTableName+
                    "          FROM "+templateDB+".guest."+templateTable+";";
//            statement.addBatch(create);
            jdbcTemplate.update(create);
            List<List<MotleyObject>> partitions = Lists.partition(records, 1000);

            for (List<MotleyObject> partition : partitions) {
                StringBuilder str = new StringBuilder(1024);
                str.append("insert into ").append(stageTableName).append(" (id) values ");
                for (int i = 0; i < partition.size(); i++) {
                    MotleyObject record = partition.get(i);
                    str.append("(").append(record.getId()).append(")");
                    if (i != partition.size()-1) {
                        str.append(",");
                    }
                }
//                ls.add(str.toString());;
                statement.addBatch(str.toString());
//                jdbc
//                temp.append(str.toString()).append(";");
//                jdbcTemplate.update(str.toString());
            }
//            statement.addBatch(temp.toString());
//            jdbcTemplate.batchUpdate(ls.toArray(new String[0]));
            statement.executeBatch();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        StageResult result = new StageResult();
        result.setDbName(templateDB);
        result.setTableName(stageTableName);
        return result;
    }

    @Override
    @Transactional
    public void dropStageTable(StageResult stageResult) {
        try {
//            Statement statement = getSqlSession().getConnection().createStatement();
//            statement.execute("drop table "+stageResult.getTableName());
            jdbcTemplate.update("drop table "+stageResult.getTableName());
//            System.out.println(stageResult.getTableName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
