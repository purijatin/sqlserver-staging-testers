
package com.kilo.dao;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;

import com.kilo.domain.MotleyObject;

public abstract class BaseStageDAOTest extends BaseDAOTest {

    protected static final String STAGING_TEST_RECORDS_IN = " Staging test records in ";

    protected static final String CREATING_TEST_RECORDS = "Creating test records";

    protected static final String DROPPING_STAGE_TABLE = "Dropping stage table";

    protected static final String templateDB = "sandbox";

    protected static final String templateTable = "motley";

    private static String[] formatTypes = new String[] { "yyyymmdd" };

    private static List<MotleyObject> smallTestRecords;

    private static List<MotleyObject> largeTestRecords;

    protected static List<MotleyObject> reallySmallTestRecords;

    protected static List<MotleyObject> anotherReallySmallTestRecords;

    private final static int smallSize = 1000;

    private final static int largeSize = 50_000;

    private static final int anotherReallySmallSize = 120;

    private static final int reallySmallSize = 100;

    public List<MotleyObject> getTestObjects(int size)  {
        try {
            return getTestRecords(size);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    protected BaseStageDAOTest() {
        try {
            // small
            smallTestRecords = getTestRecords(smallSize);
            // large
            largeTestRecords = getTestRecords(largeSize);
            // really small
            reallySmallTestRecords = getTestRecords(reallySmallSize);
            // another really small
            anotherReallySmallTestRecords = getTestRecords(anotherReallySmallSize);
        } catch (ParseException exception) {
            throw new IllegalStateException("Unable to create records",
                    exception);
        }

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
