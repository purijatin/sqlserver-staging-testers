
package com.kilo.dao;

import java.text.ParseException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Resource;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

import com.kilo.dao.impl.mybatis.BulkInsertStageDAO;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public class StageDAOTest extends BaseStageDAOTest {

    private static final Logger LOG = LoggerFactory
            .getLogger(StageDAOTest.class);


    @Resource(name = "multiValuesInsertMybatisStageDAO")
    private StageDAO multiValuesInsertMybatisStageDAO;

    @Resource(name = "mvStaticStageDAO")
    private StageDAO mvStaticStageDAO;
//


    @Resource(name = "batchInsertMybatisStageDAO")
    private StageDAO batchInsertMybatisStageDAO;


//    private StageDAO bcpIbatisStageDAO;
//
    @Resource(name = "bulkInsertMybatisStageDAO")
    private StageDAO bulkInsertMybatisStageDAO;
//

    static final int large = 200_000;
    static final int avgruns = 5;
    static final int threads = 100;

    @Test
    public void testStaticMVStageDAO() throws ParseException {
        LOG.info("Time taken " + getMethodName() + ": " +
                average(avgruns, () -> testScaffolding(mvStaticStageDAO,  getTestObjects(large))));
    }

    @Test
    public void testParallelStaticMVStage() throws Exception {
        LOG.info("Time taken " + getMethodName() + ": " +
                runParallel(threads, () -> testScaffolding(mvStaticStageDAO,  getTestObjects(large))));
    }

    //
    @Test
    public void testBatchMybatisStageDAO() throws ParseException {
        LOG.info("Time taken " + getMethodName() + ": " +
                average(avgruns, () -> testScaffolding(batchInsertMybatisStageDAO,  getTestObjects(large))));
    }

    @Test
    public void testParallelBatchMybatisStageDAO() throws Exception {
        LOG.info("Time taken " + getMethodName() + ": " +
                runParallel(threads, () -> testScaffolding(batchInsertMybatisStageDAO, getTestObjects(large))));
    }

    @Test
    public void testBulkMybatisStageDAO() throws Exception {
        LOG.info("Time taken " + getMethodName() + ": " +
                average(avgruns, () -> testScaffolding(bulkInsertMybatisStageDAO, getTestObjects(large))));
    }

    @Test
    public void testParallelBulkMybatisStageDAO() throws Exception {
        BulkInsertStageDAO.count = threads;
        LOG.info("Time taken " + getMethodName() + ": " +
                runParallel(threads, () -> testScaffolding(bulkInsertMybatisStageDAO, getTestObjects(large))));
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



    private long testScaffolding(StageDAO stageDAO,List<MotleyObject> largeRecords) {
        final StopWatch sw = new StopWatch("StagingStopWatch " + Thread.currentThread());
        sw.start(largeRecords.size() + STAGING_TEST_RECORDS_IN + stageDAO.getClass().getCanonicalName());
        long st = System.currentTimeMillis();
        StageResult stageResult = stageDAO.stage(largeRecords, templateDB, templateTable);
        sw.stop();
        long ans = System.currentTimeMillis() - st;

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
