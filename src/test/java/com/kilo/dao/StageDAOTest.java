
package com.kilo.dao;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Resource;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

import com.kilo.dao.impl.CommonDao;
import com.kilo.dao.impl.mybatis.BulkInsertStageDAO;
import com.kilo.domain.MotleyObject;
import com.kilo.domain.RunStats;
import com.kilo.domain.StageResult;

public class StageDAOTest extends BaseStageDAOTest {

    private static final Logger LOG = LoggerFactory
            .getLogger(StageDAOTest.class);



    @Resource(name = "mvStaticStageDAO")
    private StageDAO mvStaticStageDAO;
//


    @Resource(name = "batchInsertMybatisStageDAO")
    private StageDAO batchInsertMybatisStageDAO;

    @Resource(name = "bulkInsertMybatisStageDAO")
    private StageDAO bulkInsertMybatisStageDAO;


    @Resource(name = "runStats")
    CommonDao stats;

    @Test
    public void testFullDay(){
        long startTime = System.currentTimeMillis();
        ScheduledThreadPoolExecutor sched = new ScheduledThreadPoolExecutor(1);
        int timeDelay = 4;
        AtomicLong runId = new AtomicLong(0);
        List<MotleyObject> testObjects = getTestObjects(large);
        sched.scheduleAtFixedRate(() -> {
            try {
                run(batchInsertMybatisStageDAO, testObjects, runId.intValue(), "oldSingle", 1);
                Thread.sleep(100);
                run(batchInsertMybatisStageDAO, testObjects, runId.intValue(), "old100", 100);
                Thread.sleep(100);
                run(bulkInsertMybatisStageDAO, testObjects, runId.intValue(), "bulkSingle", 1);
                Thread.sleep(100);
                run(bulkInsertMybatisStageDAO, testObjects, runId.intValue(), "bulk100", 100);
                runId.incrementAndGet();
            } catch (Exception ex) {
                ex.printStackTrace();
                System.err.println("Failed.");
            }
        }, 0, timeDelay, TimeUnit.SECONDS);

        LockSupport.parkNanos(startTime+(24*60*60*1000L*1000L*1000L));
        sched.shutdown();
    }

    private void run(StageDAO stage, List<MotleyObject> testObjects,final int runId,final String name, int threads){
        Date temp = new Date();
        try {
            runParallel(threads, () -> {
                Date start = new Date();
                StageResult stageResult = testScaffolding(stage, testObjects);
                RunStats ob = new RunStats(runId, name, true, start, new Date(), System.currentTimeMillis() - start.getTime());
                ob.setDbInnerTime(stageResult.dbInnerTime);
                ob.setNetworkTime(stageResult.networkTime);
                ob.setTableCreationTime(stageResult.tableCreationTime);
                ob.setTableDropTime(stageResult.tableDropTime);
                ob.setOther(stageResult.other);
                stats.insert(ob);
                return stageResult.getTotalTime();
            });


        } catch (Exception ex) {
            System.err.println("Failed for :" + name + "(" + runId + ")");
            ex.printStackTrace();
            stats.insert(new RunStats(runId, name, false, temp, new Date(), System.currentTimeMillis() - temp.getTime()));
        }
    }



    static final int large = 200_000;
    static final int avgruns = 5;
    static final int threads = 100;

    @Test
    public void testStaticMVStageDAO() throws ParseException {
        LOG.info("Time taken " + getMethodName() + ": " +
                average(avgruns, () -> testScaffolding(mvStaticStageDAO,  getTestObjects(large)).getTotalTime()));
    }

    @Test
    public void testParallelStaticMVStage() throws Exception {
        LOG.info("Time taken " + getMethodName() + ": " +
                runParallel(threads, () -> testScaffolding(mvStaticStageDAO,  getTestObjects(large)).getTotalTime()));
    }

    //
    @Test
    public void testBatchMybatisStageDAO() throws ParseException {
        LOG.info("Time taken " + getMethodName() + ": " +
                average(avgruns, () -> testScaffolding(batchInsertMybatisStageDAO,  getTestObjects(large)).getTotalTime()));
    }

    @Test
    public void testParallelBatchMybatisStageDAO() throws Exception {
        LOG.info("Time taken " + getMethodName() + ": " +
                runParallel(threads, () -> testScaffolding(batchInsertMybatisStageDAO, getTestObjects(large)).getTotalTime()));
    }

    @Test
    public void testBulkMybatisStageDAO() throws Exception {
        LOG.info("Time taken " + getMethodName() + ": " +
                average(avgruns, () -> testScaffolding(bulkInsertMybatisStageDAO, getTestObjects(large)).getTotalTime()));
    }

    @Test
    public void testParallelBulkMybatisStageDAO() throws Exception {
        BulkInsertStageDAO.count = threads;
        LOG.info("Time taken " + getMethodName() + ": " +
                runParallel(threads, () -> testScaffolding(bulkInsertMybatisStageDAO, getTestObjects(large)).getTotalTime()));
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



    private StageResult testScaffolding(StageDAO stageDAO,List<MotleyObject> largeRecords) {
        final StopWatch sw = new StopWatch("StagingStopWatch " + Thread.currentThread());
        sw.start(largeRecords.size() + STAGING_TEST_RECORDS_IN + stageDAO.getClass().getCanonicalName());
        final long st = System.currentTimeMillis();
        StageResult stageResult = stageDAO.stage(largeRecords, templateDB, templateTable);
        sw.stop();
        final long ans = System.currentTimeMillis() - st;

        sw.start(DROPPING_STAGE_TABLE);
        long temp = System.currentTimeMillis();
        stageDAO.dropStageTable(stageResult);
        stageResult.setTableDropTime(System.currentTimeMillis() - temp);
        sw.stop();
        stageResult.setTotalTime(System.currentTimeMillis() - st);
//        LOG.info(sw.prettyPrint());
        return stageResult;

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
