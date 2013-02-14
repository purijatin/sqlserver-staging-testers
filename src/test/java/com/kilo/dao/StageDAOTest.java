
package com.kilo.dao;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.annotation.Resource;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class StageDAOTest extends BaseDAOTest {

    private static final String STAGING_TEST_RECORDS_IN = "Staging test records in ";

    private static final String CREATING_TEST_RECORDS = "Creating test records";

    private static final String templateDB = "sandbox";

    private static final String templateTable = "motley";

    private static String[] formatTypes = new String[] { "yyyymmdd" };

    private static final Logger LOG = LoggerFactory
            .getLogger(StageDAOTest.class);

    @Resource(name = "multiInsertStageDAO")
    private StageDAO multiInsertStageDAO;

    @Test
    public void testMultiInsertStageDAO() throws ParseException {

        StopWatch sw = new StopWatch();

        sw.start(CREATING_TEST_RECORDS);
        List<MotleyObject> testRecords = getTestRecords(1000);
        sw.stop();

        sw.start(STAGING_TEST_RECORDS_IN
                + multiInsertStageDAO.getClass().getCanonicalName());
        StageResult stageResult = multiInsertStageDAO.stage(testRecords,
                templateDB, templateTable);
        sw.stop();
        LOG.info("Staged table is %s", stageResult);

        LOG.info(sw.prettyPrint());

        sw.start(CREATING_TEST_RECORDS);
        testRecords = getTestRecords(100_000);
        sw.stop();

        sw.start(STAGING_TEST_RECORDS_IN
                + multiInsertStageDAO.getClass().getCanonicalName());
        stageResult = multiInsertStageDAO.stage(testRecords, templateDB,
                templateTable);
        sw.stop();
        LOG.info("Staged table is %s", stageResult);

        LOG.info(sw.prettyPrint());
    }

    private List<MotleyObject> getTestRecords(int size) throws ParseException {

        Date date = DateUtils.parseDate("20130101", formatTypes);
        Date knowledgeTime = new Date();
        List<MotleyObject> results = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            BigDecimal price = new BigDecimal("" + i + ".00");
            BigDecimal amount = new BigDecimal("" + (i * 2) + ".00");
            BigDecimal fxRate = new BigDecimal("" + (i / 2.0));
            MotleyObject mo = new MotleyObject(date, "name_" + i, i, price,
                    amount, fxRate, Boolean.valueOf(i % 2 == 0), knowledgeTime);
            results.add(mo);
        }
        return results;
    }
}
