
package com.kilo.dao;

import java.util.concurrent.atomic.AtomicInteger;

public class StageUtils {
    static AtomicInteger count = new AtomicInteger();

    public static String getStageTableName(String templateTable) {
        String stageTableName = templateTable + "_" + count.incrementAndGet() +"_"+System.nanoTime()
                + "_" + Thread.currentThread().getId();
        return stageTableName;
    }
}
