
package com.kilo.dao;

import java.text.ParseException;
import java.util.List;

import javax.annotation.Resource;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class StageDAOTest extends BaseStageDAOTest {

    private static final Logger LOG = LoggerFactory
            .getLogger(StageDAOTest.class);

    @Resource(name = "multiInsertIbatisStageDAO")
    private StageDAO multiInsertIbatisStageDAO;

    @Resource(name = "multiInsertSJDBCStageDAO")
    private StageDAO multiInsertSJDBCStageDAO;

    @Test
    public void testMultiInsertIbatisStageDAO() throws ParseException {
        testScaffolding(multiInsertIbatisStageDAO);
    }

    @Test
    public void testMultiSJDBCIbatisStageDAO() throws ParseException {
        testScaffolding(multiInsertSJDBCStageDAO);
    }

    private void testScaffolding(StageDAO stageDAO) throws ParseException {

        StopWatch sw = new StopWatch();

        sw.start(CREATING_TEST_RECORDS);
        List<MotleyObject> testRecords = getTestRecords(1000);
        sw.stop();

        sw.start(STAGING_TEST_RECORDS_IN
                + stageDAO.getClass().getCanonicalName());
        StageResult stageResult = stageDAO.stage(testRecords, templateDB,
                templateTable);
        sw.stop();
        LOG.info("Staged table is {}", stageResult.getTableName());

        sw.start(DROPPING_STAGE_TABLE);
        stageDAO.dropStageTable(stageResult);
        sw.stop();

        sw.start(CREATING_TEST_RECORDS);
        testRecords = getTestRecords(100_000);
        sw.stop();

        sw.start(STAGING_TEST_RECORDS_IN
                + stageDAO.getClass().getCanonicalName());
        stageResult = stageDAO.stage(testRecords, templateDB, templateTable);
        sw.stop();
        LOG.info("Staged table is {}", stageResult.getTableName());

        sw.start(DROPPING_STAGE_TABLE);
        stageDAO.dropStageTable(stageResult);
        sw.stop();

        LOG.info(sw.prettyPrint());

    }
}
