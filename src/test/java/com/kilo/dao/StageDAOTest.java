
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

    private final static int smallSize = 1000;

    private final static int largeSize = 100_000;

    private static final int anotherReallySmallSize = 120;

    private static final int reallySmallSize = 100;

    @Resource(name = "multiInsertIbatisStageDAO")
    private StageDAO multiInsertIbatisStageDAO;

    @Resource(name = "multiInsertSJDBCStageDAO")
    private StageDAO multiInsertSJDBCStageDAO;

    @Resource(name = "multiInsertJDBCStageDAO")
    private StageDAO multiInsertJDBCStageDAO;

    @Resource(name = "multiValuesInsertIbatisStageDAO")
    private StageDAO multiValuesInsertIbatisStageDAO;

    @Resource(name = "multiValuesInsertSJDBCStageDAO")
    private StageDAO multiValuesInsertSJDBCStageDAO;

    @Resource(name = "multiValuesInsertJDBCStageDAO")
    private StageDAO multiValuesInsertJDBCStageDAO;

    @Resource(name = "batchInsertIbatisStageDAO")
    private StageDAO batchInsertIbatisStageDAO;

    @Resource(name = "batchInsertSJDBCStageDAO")
    private StageDAO batchInsertSJDBCStageDAO;

    @Resource(name = "batchInsertJDBCStageDAO")
    private StageDAO batchInsertJDBCStageDAO;

    @Test
    public void testMultiInsertIbatisStageDAO() throws ParseException {
        testScaffolding(multiInsertIbatisStageDAO, smallSize, largeSize);
    }

    @Test
    public void testMultiSJDBCStageDAO() throws ParseException {
        testScaffolding(multiInsertSJDBCStageDAO, smallSize, largeSize);
    }

    @Test
    public void testMultiJDBCStageDAO() throws ParseException {
        testScaffolding(multiInsertJDBCStageDAO, smallSize, largeSize);
    }

    @Test
    public void testMultiValuesIbatisStageDAO() throws ParseException {
        testScaffolding(multiValuesInsertIbatisStageDAO, reallySmallSize,
                anotherReallySmallSize);
    }

    @Test
    public void testMultiValuesSJDBCStageDAO() throws ParseException {
        testScaffolding(multiValuesInsertSJDBCStageDAO, reallySmallSize,
                anotherReallySmallSize);
    }

    @Test
    public void testMultiValuesJDBCStageDAO() throws ParseException {
        testScaffolding(multiValuesInsertJDBCStageDAO, reallySmallSize,
                anotherReallySmallSize);
    }

    @Test
    public void testBatchIbatisStageDAO() throws ParseException {
        testScaffolding(batchInsertIbatisStageDAO, smallSize, largeSize);
    }

    @Test
    public void testBatchSJDBCStageDAO() throws ParseException {
        testScaffolding(batchInsertSJDBCStageDAO, smallSize, largeSize);
    }

    @Test
    public void testBatchJDBCStageDAO() throws ParseException {
        testScaffolding(batchInsertJDBCStageDAO, smallSize, largeSize);
    }

    private void testScaffolding(StageDAO stageDAO, int smallSize, int largeSize)
            throws ParseException {

        StopWatch sw = new StopWatch();

        sw.start(CREATING_TEST_RECORDS);
        List<MotleyObject> testRecords = getTestRecords(smallSize);
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
        testRecords = getTestRecords(largeSize);
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
