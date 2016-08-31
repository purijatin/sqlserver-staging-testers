
package com.kilo.dao;

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

import com.kilo.MonitorInterceptor;
import com.kilo.dao.impl.mybatis.BatchInsertStageDAO;

public class StageDAOTest extends BaseStageDAOTest {

    private static final Logger LOG = LoggerFactory.getLogger(StageDAOTest.class);



    @Resource(name = "batchInsertMybatisStageDAO")
    private StageDAO batchInsertMybatisStageDAO;


    @Test
    public void testjdbctype(){

        BatchInsertStageDAO batch = (BatchInsertStageDAO) this.batchInsertMybatisStageDAO;
        LOG.info("X jdbcType" + getMethodName() + ": " +average(3,() -> {
            long st = System.currentTimeMillis();
            MonitorInterceptor.count=0;
            batch.getAll();
            long l = System.currentTimeMillis() - st;
            System.out.println("X "+l+". Count: "+MonitorInterceptor.count);
            return l;
        }));

        LOG.info("jdbcTypePresent " + getMethodName() + ": " +average(3,() -> {
            long st = System.currentTimeMillis();
            MonitorInterceptor.count=0;
            batch.getAll2();
            long l = System.currentTimeMillis() - st;
            System.out.println("present "+l+". Count: "+MonitorInterceptor.count);
            return l;
        }));
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
