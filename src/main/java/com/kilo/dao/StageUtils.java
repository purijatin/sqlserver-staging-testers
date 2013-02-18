
package com.kilo.dao;

import java.util.Date;

public class StageUtils {

    public static String getStageTableName(String templateTable) {
        String stageTableName = templateTable + "_" + new Date().getTime()
                + "_" + Thread.currentThread().getId();
        return stageTableName;
    }
}
