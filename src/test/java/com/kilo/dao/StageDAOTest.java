
package com.kilo.dao;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Resource;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;
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


    class A<T> implements TypeHandler<T>{

        @Override
        public void setParameter(PreparedStatement ps, int i, T parameter, JdbcType jdbcType) throws SQLException {
        }

        @Override
        public T getResult(ResultSet rs, String columnName) throws SQLException {
            return null;
        }

        @Override
        public T getResult(ResultSet rs, int columnIndex) throws SQLException {
            return null;
        }

        @Override
        public T getResult(CallableStatement cs, int columnIndex) throws SQLException {
            return null;
        }
    }

    class Ar extends A<ArrayList<String>>{}
    class B extends A<List<String>>{}
    class C extends A<Set<String>>{}
    class D extends A<LinkedList<String>>{}
    class E extends A<HashSet<String>>{}
    class F extends A<TreeSet<String>>{}
    class G extends A<java.lang.reflect.Array>{}
    class H extends A<TimeUnit>{}



    @Test
    public void testGetGenericRecUnits() throws Exception {
        TypeHandlerRegistry map = new TypeHandlerRegistry();
        int runs = 217_800_135;

        map.register(ArrayList.class, new Ar());
        map.register(List.class, new B());
        map.register(Set.class, new C());
        map.register(LinkedList.class, new D());
        map.register(HashSet.class, new E());
        map.register(TreeSet.class, new F());
        map.register(java.lang.reflect.Array.class, new G());
        map.register(TimeUnit.class, new H());

        Field field = TypeHandlerRegistry.class.getDeclaredField("TYPE_HANDLER_MAP");
        field.setAccessible(true);

        Map<Type, Map<JdbcType, TypeHandler<?>>> type = (Map<Type, Map<JdbcType, TypeHandler<?>>>) field.get(map);
        Set<Type> types = type.keySet();
        for (Type type1 : types) {
            long st = System.currentTimeMillis();
            boolean b = true;//so that jit doesnt do anything special
            for (int i = 0; i < runs; i++) {
                b &= map.hasTypeHandler((Class<?>) type1);
            }
            System.out.println(b);
            System.out.println(type1 + " - Total: " + (System.currentTimeMillis() - st));
        }
    }


}
