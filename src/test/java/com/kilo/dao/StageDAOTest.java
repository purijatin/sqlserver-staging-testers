
package com.kilo.dao;

import java.text.ParseException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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


    //    @Resource(name = "multiInsertMybatisStageDAO")
//    private StageDAO multiInsertMybatisStageDAO;
//
//    @Resource(name = "multiInsertIbatisStageDAO")
//    private StageDAO multiInsertIbatisStageDAO;
//
//    @Resource(name = "multiInsertSJDBCStageDAO")
//    private StageDAO multiInsertSJDBCStageDAO;
//
//    @Resource(name = "multiInsertJDBCStageDAO")
//    private StageDAO multiInsertJDBCStageDAO;
//
    @Resource(name = "multiValuesInsertMybatisStageDAO")
    private StageDAO multiValuesInsertMybatisStageDAO;

    @Resource(name = "mvStaticStageDAO")
    private StageDAO mvStaticStageDAO;
//
//    @Resource(name = "multiValuesInsertIbatisStageDAO")
//    private StageDAO multiValuesInsertIbatisStageDAO;
//
//    @Resource(name = "multiValuesInsertSJDBCStageDAO")
//    private StageDAO multiValuesInsertSJDBCStageDAO;
//
//    @Resource(name = "multiValuesInsertJDBCStageDAO")
//    private StageDAO multiValuesInsertJDBCStageDAO;

    @Resource(name = "batchInsertMybatisStageDAO")
    private StageDAO batchInsertMybatisStageDAO;

    //    @Resource(name = "batchInsertIbatisStageDAO")
//    private StageDAO batchInsertIbatisStageDAO;
//
//    @Resource(name = "batchInsertSJDBCStageDAO")
//    private StageDAO batchInsertSJDBCStageDAO;
//
//    @Resource(name = "batchInsertJDBCStageDAO")
//    private StageDAO batchInsertJDBCStageDAO;
//
//    @Resource(name = "bcpIbatisStageDAO")
//    private StageDAO bcpIbatisStageDAO;
//
//    @Resource(name = "bulkInsertMybatisStageDAO")
//    private StageDAO bulkInsertMybatisStageDAO;
//
//    @Resource(name = "bulkInsertIbatisStageDAO")
//    private StageDAO bulkInsertIbatisStageDAO;
//
//    @Resource(name = "bulkInsertSJDBCStageDAO")
//    private StageDAO bulkInsertSJDBCStageDAO;
//
//    @Resource(name = "bulkInsertJDBCStageDAO")
//    private StageDAO bulkInsertJDBCStageDAO;
//
//    @Resource(name = "openrowsetInsertMybatisStageDAO")
//    private StageDAO openrowsetInsertMybatisStageDAO;
//
//    @Resource(name = "openrowsetInsertIbatisStageDAO")
//    private StageDAO openrowsetInsertIbatisStageDAO;
//
//    @Resource(name = "openrowsetInsertSJDBCStageDAO")
//    private StageDAO openrowsetInsertSJDBCStageDAO;
//
//    @Resource(name = "openrowsetInsertJDBCStageDAO")
//    private StageDAO openrowsetInsertJDBCStageDAO;
//
//    @Resource(name = "xmlShredderInsertMybatisStageDAO")
//    private StageDAO xmlShredderInsertMybatisStageDAO;
//
//    @Resource(name = "xmlShredderInsertIbatisStageDAO")
//    private StageDAO xmlShredderInsertIbatisStageDAO;
//
//    @Resource(name = "xmlShredderInsertSJDBCStageDAO")
//    private StageDAO xmlShredderInsertSJDBCStageDAO;
//
//    @Resource(name = "xmlShredderInsertJDBCStageDAO")
//    private StageDAO xmlShredderInsertJDBCStageDAO;
//
//    @Test
//    public void testMultiInsertMybatisStageDAO() throws ParseException {
//        testScaffolding(multiInsertMybatisStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testMultiInsertIbatisStageDAO() throws ParseException {
//        testScaffolding(multiInsertIbatisStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testMultiInsertSJDBCStageDAO() throws ParseException {
//        testScaffolding(multiInsertSJDBCStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testMultiInsertJDBCStageDAO() throws ParseException {
//        testScaffolding(multiInsertJDBCStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
    public void testMultiValuesMybatisStageDAO() throws ParseException {
        LOG.info("Time taken " + getMethodName() + ": " +
                testScaffolding(multiValuesInsertMybatisStageDAO, getTestObjects(50), getTestObjects(50)));
    }
//
//    @Test
//    public void testMultiValuesIbatisStageDAO() throws ParseException {
//        testScaffolding(multiValuesInsertIbatisStageDAO,
//                reallySmallTestRecords, anotherReallySmallTestRecords);
//    }
//
//    @Test
//    public void testMultiValuesSJDBCStageDAO() throws ParseException {
//        testScaffolding(multiValuesInsertSJDBCStageDAO, reallySmallTestRecords,
//                anotherReallySmallTestRecords);
//    }
//
//    @Test
//    public void testMultiValuesJDBCStageDAO() throws ParseException {
//        testScaffolding(multiValuesInsertJDBCStageDAO, reallySmallTestRecords,
//                anotherReallySmallTestRecords);
//    }

    static final int large = 100_000;
    static final int avgruns = 5;
    static final int threads = 100;

    @Test
    public void testStaticMVStageDAO() throws ParseException {
        LOG.info("Time taken " + getMethodName() + ": " +
                average(avgruns, () -> testScaffolding(mvStaticStageDAO, getTestObjects(1000), getTestObjects(large))));
    }

    @Test
    public void testParallelStaticMVStage() throws Exception {
        LOG.info("Time taken " + getMethodName() + ": " +
                runParallel(threads, () -> testScaffolding(mvStaticStageDAO, getTestObjects(1000), getTestObjects(large))));
    }

    //
    @Test
    public void testBatchMybatisStageDAO() throws ParseException {
        LOG.info("Time taken " + getMethodName() + ": " +
                average(avgruns, () -> testScaffolding(batchInsertMybatisStageDAO, getTestObjects(1000), getTestObjects(large))));
    }

    @Test
    public void testParallelBatchMybatisStageDAO() throws Exception {
        LOG.info("Time taken " + getMethodName() + ": " +
                runParallel(threads, () -> testScaffolding(batchInsertMybatisStageDAO, getTestObjects(1000), getTestObjects(large))));
    }

    private double runParallel(int parallel, Supplier<Long> run) throws Exception {
        ExecutorService exec = Executors.newCachedThreadPool();
        List<CompletableFuture<Long>> com = IntStream.rangeClosed(1, parallel)
                .mapToObj(x -> CompletableFuture.supplyAsync(run, exec)).collect(Collectors.toList());
        exec.shutdown();
        return sequence(com).get().stream().mapToLong(x -> x).average().getAsDouble();
    }

    private double average(int runs, Supplier<Long> run) {
        return IntStream.rangeClosed(1, runs).mapToLong(x -> run.get()).average().getAsDouble();
    }
//
//    @Test
//    public void testBatchIbatisStageDAO() throws ParseException {
//        testScaffolding(batchInsertIbatisStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testBatchSJDBCStageDAO() throws ParseException {
//        testScaffolding(batchInsertSJDBCStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testBatchJDBCStageDAO() throws ParseException {
//        testScaffolding(batchInsertJDBCStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testBcpIbatisStageDAO() throws ParseException {
//        testScaffolding(bcpIbatisStageDAO, smallTestRecords, largeTestRecords);
//    }
//
//    @Test
//    public void testBulkMybatisStageDAO() throws ParseException {
//        testScaffolding(bulkInsertMybatisStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testBulkIbatisStageDAO() throws ParseException {
//        testScaffolding(bulkInsertIbatisStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testBulkSJDBCStageDAO() throws ParseException {
//        testScaffolding(bulkInsertSJDBCStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testBulkJDBCStageDAO() throws ParseException {
//        testScaffolding(bulkInsertJDBCStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testOpenrowsetMybatisStageDAO() throws ParseException {
//        testScaffolding(openrowsetInsertMybatisStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testOpenrowsetIbatisStageDAO() throws ParseException {
//        testScaffolding(openrowsetInsertIbatisStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testOpenrowsetSJDBCStageDAO() throws ParseException {
//        testScaffolding(openrowsetInsertSJDBCStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testOpenrowsetJDBCStageDAO() throws ParseException {
//        testScaffolding(openrowsetInsertJDBCStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testXMLShredderMybatisStageDAO() throws ParseException {
//        testScaffolding(xmlShredderInsertMybatisStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testXMLShredderIbatisStageDAO() throws ParseException {
//        testScaffolding(xmlShredderInsertIbatisStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testXMLShredderSJDBCStageDAO() throws ParseException {
//        testScaffolding(xmlShredderInsertSJDBCStageDAO, smallTestRecords,
//                largeTestRecords);
//    }
//
//    @Test
//    public void testXMLShredderJDBCStageDAO() throws ParseException {
//        testScaffolding(xmlShredderInsertJDBCStageDAO, smallTestRecords,
//                largeTestRecords);
//    }

    private long testScaffolding(StageDAO stageDAO,
                                 List<MotleyObject> smallRecords, List<MotleyObject> largeRecords) {

        final StopWatch sw = new StopWatch("StagingStopWatch " + Thread.currentThread());
        sw.start(smallRecords.size() + STAGING_TEST_RECORDS_IN
                + stageDAO.getClass().getCanonicalName());
        StageResult stageResult = stageDAO.stage(smallRecords, templateDB,
                templateTable);
        sw.stop();
//        LOG.info("Staged table is {}", stageResult.getTableName());

        sw.start(DROPPING_STAGE_TABLE);
        stageDAO.dropStageTable(stageResult);
        sw.stop();


        //verylarge
        //large
        sw.start(largeRecords.size() + STAGING_TEST_RECORDS_IN + stageDAO.getClass().getCanonicalName());
        long st = System.currentTimeMillis();
        stageResult = stageDAO.stage(largeRecords, templateDB, templateTable);
        sw.stop();
        long ans = System.currentTimeMillis() - st;
//        LOG.info("Staged table is {}", stageResult.getTableName());

        sw.start(DROPPING_STAGE_TABLE);
        stageDAO.dropStageTable(stageResult);
        sw.stop();
        LOG.info(sw.prettyPrint());
        return ans;

    }

    public String getMethodName() {
        final StackTraceElement[] ste = Thread.currentThread().getStackTrace();
        return ste[2].getMethodName();
    }

    <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> com) {
        return CompletableFuture.allOf(com.toArray(new CompletableFuture[com.size()]))
                .thenApply(v -> com.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList())
                );
    }


}
